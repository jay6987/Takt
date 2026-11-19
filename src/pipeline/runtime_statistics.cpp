#include "takt/pipeline/pipeline_runtime.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include "takt/pipeline/node.h"
#include "takt/pipeline/pipe_base.h"
#include "takt/pipeline/pipe_core.h"

namespace takt
{
void PipelineRuntime::enable_statistics()
{
    if (state_ != State::Initialized)
        throw std::logic_error("statistics require an initialized runtime before start");

    std::vector<PipelineRuntime*> runtimes;
    std::vector<std::shared_ptr<PipeBase>> pipes;
    collect_tree(runtimes, pipes);
    for (const auto* runtime : runtimes)
        if (runtime->statistics_mode_ != StatisticsMode::Disabled)
            throw std::logic_error("statistics already enabled in runtime tree");

    std::vector<std::pair<Node*, std::unique_ptr<NodeStatistics>>> nodes;
    for (const auto* runtime : runtimes)
    {
        for (const auto& entry : runtime->nodes_)
        {
            if (auto* node = dynamic_cast<Node*>(entry.get()))
            {
                auto statistics = std::make_unique<NodeStatistics>();
                statistics->workers.resize(node->worker_count());
                nodes.emplace_back(node, std::move(statistics));
            }
        }
    }
    std::unordered_set<PipeBase*> seen;
    std::vector<std::pair<PipeCore*, std::unique_ptr<PipeStatistics>>> cores;
    for (const auto& pipe : pipes)
    {
        if (!seen.insert(pipe.get()).second)
            continue;
        if (auto* core = pipe->statistics_core())
        {
            auto statistics = std::make_unique<PipeStatistics>();
            statistics->slot_capacity = core->slot_capacity();
            statistics->peak_unreusable_slots =
                core->write_reserved_frame_end_ - core->reusable_frame_start_locked();
            cores.emplace_back(core, std::move(statistics));
        }
    }
    for (auto& [node, statistics] : nodes)
        node->statistics_ = std::move(statistics);
    for (auto& [core, statistics] : cores)
        core->statistics_ = std::move(statistics);
    for (auto* runtime : runtimes)
        runtime->statistics_mode_ = StatisticsMode::Child;
    statistics_mode_ = StatisticsMode::Root;
}

RuntimeStatistics PipelineRuntime::statistics() const
{
    RuntimeStatistics result;
    if (statistics_mode_ == StatisticsMode::Disabled)
        return result;
    if (statistics_mode_ == StatisticsMode::Child)
        throw std::logic_error("read statistics from the runtime that enabled them");
    if (!joined_)
        throw std::logic_error("statistics require a completed runtime join");
    std::vector<const PipeBase*> seen_pipes;
    append_statistics("", result, seen_pipes);
    return result;
}

void PipelineRuntime::append_statistics(
    const std::string& prefix,
    RuntimeStatistics& result,
    std::vector<const PipeBase*>& seen_pipes) const
{
    for (const auto& entry : nodes_)
    {
        const auto path = prefix + entry->name();
        const auto* node = dynamic_cast<const Node*>(entry.get());
        if (node && node->statistics_ && node->statistics_started_)
        {
            result.nodes.push_back(*node->statistics_);
            result.nodes.back().path = path;
        }
        for (const auto& child : entry->child_runtimes())
            child->append_statistics(path + "/", result, seen_pipes);
    }
    // Visit child pipes first so shared boundary pipes get their inner path.
    for (const auto& pipe : pipes_)
    {
        auto* core = pipe->statistics_core();
        if (!core || !core->statistics_ ||
            std::find(seen_pipes.begin(), seen_pipes.end(), pipe.get()) !=
                seen_pipes.end())
            continue;
        seen_pipes.push_back(pipe.get());
        result.pipes.push_back(*core->statistics_);
        result.pipes.back().path = prefix + pipe->name();
    }
}
} // namespace takt
