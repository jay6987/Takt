// Implements pipeline builder topology operations.
#include "builder_detail.h"

#include "takt/pipeline/pipeline_runtime.h"
#include <algorithm>
#include <optional>
#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace takt
{
using namespace builder_detail;
bool PipelineBuilder::has_node(const std::string& node_name) const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    return find_node_locked(state_->nodes, node_name) != nullptr;
}

bool PipelineBuilder::has_bound_node(const std::string& node_name) const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    return state_->bound_nodes.find(node_name) != state_->bound_nodes.end();
}

const NodeSpec* PipelineBuilder::find_node(const std::string& node_name) const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    return find_node_locked(state_->nodes, node_name);
}

const EdgeSpec* PipelineBuilder::find_edge(const std::string& edge_name) const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    auto it = std::find_if(state_->channels.begin(), state_->channels.end(),
                           [&](const EdgeSpec& channel)
                           { return channel.name == edge_name; });
    return it == state_->channels.end() ? nullptr : &*it;
}

const std::vector<NodeSpec>& PipelineBuilder::nodes() const noexcept
{
    return state_->nodes;
}

const std::vector<EdgeSpec>& PipelineBuilder::edges() const noexcept
{
    return state_->channels;
}

std::shared_ptr<NodeBase>
PipelineBuilder::find_bound_node(const std::string& node_name) const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    auto it = state_->bound_nodes.find(node_name);
    return it == state_->bound_nodes.end() ? nullptr : it->second;
}

void PipelineBuilder::validate() const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    validate_locked(*state_);
}

PipelineGraph PipelineBuilder::graph() const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    return {state_->name,
            state_->nodes,
            state_->channels,
            state_->subgraphs,
            {},
            {},
            {},
            {}};
}

std::string PipelineBuilder::to_dot(DotOptions options) const
{
    return takt::to_dot(graph(), options);
}
} // namespace takt
