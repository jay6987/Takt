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
PipelineBuilder& PipelineBuilder::connect(std::string from_node,
                                          std::string from_port,
                                          std::string to_node,
                                          std::string to_port,
                                          PipeDescriptor descriptor,
                                          std::string channel_name)
{
    if (from_node.empty() || from_port.empty() || to_node.empty() ||
        to_port.empty())
    {
        throw std::invalid_argument("connection endpoints cannot be empty");
    }

    std::lock_guard<std::mutex> lk(state_->mutex);
    auto* source_node = find_node_locked(state_->nodes, from_node);
    auto* target_node = find_node_locked(state_->nodes, to_node);
    if (!source_node || !target_node)
    {
        throw std::logic_error(
            "connection endpoints must reference declared nodes");
    }

    if (!find_port_locked(source_node->output_ports, from_port))
    {
        throw std::logic_error("source output port is missing");
    }
    if (!find_port_locked(target_node->input_ports, to_port))
    {
        throw std::logic_error("target input port is missing");
    }

    EdgeSpec channel;
    channel.name = std::move(channel_name);
    channel.from_node = std::move(from_node);
    channel.from_port = std::move(from_port);
    channel.to_node = std::move(to_node);
    channel.to_port = std::move(to_port);
    channel.descriptor = std::move(descriptor);
    state_->channels.push_back(std::move(channel));
    return *this;
}

PipelineBuilder& PipelineBuilder::bind_node(std::string node_name,
                                            std::shared_ptr<NodeBase> node)
{
    if (node_name.empty())
    {
        throw std::invalid_argument("node name cannot be empty");
    }
    if (!node)
    {
        throw std::invalid_argument("node cannot be null");
    }

    std::lock_guard<std::mutex> lk(state_->mutex);
    if (!find_node_locked(state_->nodes, node_name))
    {
        throw std::logic_error("cannot bind undeclared node");
    }
    state_->subgraphs.erase(node_name);
    state_->bound_nodes[std::move(node_name)] = std::move(node);
    return *this;
}

PipelineBuilder&
PipelineBuilder::bind_subgraph(std::string node_name,
                               const PipelineBuilder& subgraph_builder)
{
    return bind_subgraph(std::move(node_name), subgraph_builder, {}, {});
}

} // namespace takt
