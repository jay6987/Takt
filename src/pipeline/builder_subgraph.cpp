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
namespace
{
/// Resolved transport state prepared before binding a nested runtime.
struct BoundaryLinks
{
    std::vector<SubgraphNode::BridgeLink> links;
    std::map<std::string, MappingTransportInfo> inputs;
    std::map<std::string, MappingTransportInfo> outputs;
};

// Copies the declared outer node while holding its builder lock.
NodeSpec copy_outer_node(PipelineBuilder::State& state,
                         const std::string& name)
{
    std::lock_guard<std::mutex> lk(state.mutex);
    const auto* found = find_node_locked(state.nodes, name);
    if (!found)
        throw std::logic_error("cannot bind undeclared node");
    return *found;
}

// Validates and resolves nested boundary mappings under the child lock.
BoundaryLinks resolve_links(PipelineBuilder::State& state,
                            const NodeSpec& outer_node,
                            const std::unordered_map<std::string,
                                                     NodePortRef>& inputs,
                            const std::unordered_map<std::string,
                                                     NodePortRef>& outputs)
{
    BoundaryLinks result;
    std::lock_guard<std::mutex> lk(state.mutex);
    validate_subgraph_port_mappings_locked(outer_node, state.nodes, inputs,
                                           outputs);
    result.links = build_subgraph_bridge_links_locked(
        outer_node, state.nodes, inputs, outputs, result.inputs,
        result.outputs);
    return result;
}
} // namespace

PipelineBuilder& PipelineBuilder::bind_subgraph(
    std::string node_name,
    const PipelineBuilder& subgraph_builder,
    std::unordered_map<std::string,
                       NodePortRef> input_port_mappings,
    std::unordered_map<std::string,
                       NodePortRef> output_port_mappings)
{
    if (node_name.empty())
    {
        throw std::invalid_argument("node name cannot be empty");
    }

    if (state_.get() == subgraph_builder.state_.get())
    {
        throw std::logic_error("a builder cannot bind itself as a subgraph");
    }

    auto subgraph_runtime = subgraph_builder.build_runtime();

    const auto outer_node = copy_outer_node(*state_, node_name);
    auto boundaries = resolve_links(*subgraph_builder.state_, outer_node,
                                    input_port_mappings, output_port_mappings);

    auto topology = subgraph_builder.graph();
    topology.input_mappings.insert(input_port_mappings.begin(),
                                   input_port_mappings.end());
    topology.output_mappings.insert(output_port_mappings.begin(),
                                    output_port_mappings.end());
    topology.input_transports = std::move(boundaries.inputs);
    topology.output_transports = std::move(boundaries.outputs);
    std::lock_guard<std::mutex> lk(state_->mutex);
    state_->bound_nodes[node_name] = std::make_shared<SubgraphNode>(
        node_name, std::move(subgraph_runtime), std::move(input_port_mappings),
        std::move(output_port_mappings), std::move(boundaries.links));
    state_->subgraphs[node_name] = std::move(topology);
    return *this;
}

} // namespace takt
