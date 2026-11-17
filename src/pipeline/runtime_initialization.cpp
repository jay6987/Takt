// Validates and initializes complete nested runtime trees.
#include "takt/pipeline/pipeline_runtime.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_set>

#include "takt/pipeline/node.h"
#include "takt/pipeline/pipe_base.h"

namespace takt
{
void PipelineRuntime::collect_tree(
    std::vector<PipelineRuntime*>& runtimes,
    std::vector<std::shared_ptr<PipeBase>>& pipes)
{
    if (std::find(runtimes.begin(), runtimes.end(), this) != runtimes.end())
        throw std::logic_error(
            "runtime tree contains a cycle or shared subgraph");
    if (state_ == State::Started || state_ == State::Failed ||
        state_ == State::Stopped)
        throw std::logic_error("runtime is not available for initialization");
    runtimes.push_back(this);
    pipes.insert(pipes.end(), pipes_.begin(), pipes_.end());
    std::unordered_set<NodeBase*> nodes;
    for (const auto& node : nodes_)
    {
        if (!nodes.insert(node.get()).second)
            throw std::logic_error("runtime contains duplicate node ownership");
        for (const auto& child : node->child_runtimes())
        {
            if (!child)
                throw std::logic_error("subgraph runtime cannot be null");
            child->collect_tree(runtimes, pipes);
        }
    }
}

void PipelineRuntime::validate_nodes(
    const std::vector<PipelineRuntime*>& runtimes)
{
    std::unordered_set<NodeBase*> nodes;
    for (const auto* runtime : runtimes)
        for (const auto& node : runtime->nodes_)
            if (!nodes.insert(node.get()).second)
                throw std::logic_error("node is shared by multiple runtimes");
}

void PipelineRuntime::initialize_pipes(
    const std::vector<std::shared_ptr<PipeBase>>& pipes)
{
    std::unordered_set<PipeBase*> seen;
    std::vector<std::shared_ptr<PipeBase>> unique;
    for (const auto& pipe : pipes)
    {
        if (!seen.insert(pipe.get()).second)
            continue;
        if (pipe->closed() || pipe->faulted())
            throw std::logic_error(
                "pipe='" + pipe->name() +
                "': terminal pipe cannot initialize a runtime");
        if (!pipe->initialized())
            pipe->validate_configuration();
        unique.push_back(pipe);
    }
    for (const auto& pipe : unique)
        if (!pipe->initialized())
            pipe->initialize();
}

void PipelineRuntime::initialize()
{
    if (state_ != State::Created)
        throw std::logic_error("runtime initialization is one-shot");
    std::vector<PipelineRuntime*> runtimes;
    std::vector<std::shared_ptr<PipeBase>> pipes;
    try
    {
        collect_tree(runtimes, pipes);
        validate_nodes(runtimes);
        initialize_pipes(pipes);
        for (auto* runtime : runtimes)
            runtime->state_ = State::Initialized;
    }
    catch (...)
    {
        initialization_failure_ = std::current_exception();
        state_ = State::Failed;
        for (auto* runtime : runtimes)
        {
            runtime->initialization_failure_ = initialization_failure_;
            runtime->state_ = State::Failed;
        }
        throw;
    }
}
} // namespace takt
