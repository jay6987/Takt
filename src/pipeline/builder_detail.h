// Declares builder state and topology helper operations.
#pragma once

#include "takt/pipeline/pipeline_builder.h"
#include "takt/pipeline/subgraph_node.h"

#include <mutex>

namespace takt
{
/// Shared mutable topology and executable bindings for builder handles.
struct PipelineBuilder::State
{
    // Establishes the topology's diagnostic identity.
    explicit State(std::string builder_name) : name(std::move(builder_name)) {}
    std::string name;
    std::vector<NodeSpec> nodes;
    std::vector<EdgeSpec> channels;
    std::unordered_map<std::string, std::shared_ptr<NodeBase>> bound_nodes;
    std::map<std::string, PipelineGraph> subgraphs;
    mutable std::mutex mutex;
};

namespace builder_detail
{
// Finds or creates a node in locked state.
NodeSpec& ensure_node_locked(std::vector<NodeSpec>& nodes,
                             const std::string& name);
// Finds a node in locked state.
const NodeSpec* find_node_locked(const std::vector<NodeSpec>& nodes,
                                 const std::string& name);
// Finds or creates a port in locked state.
PortSpec& ensure_port_locked(std::vector<PortSpec>& ports,
                             const std::string& port_name,
                             PortDirection direction,
                             const PipeDescriptor& descriptor);
// Finds a port in locked state.
const PortSpec* find_port_locked(const std::vector<PortSpec>& ports,
                                 const std::string& port_name);
// Retrieves and validates a port's runtime adapter.
std::shared_ptr<AnyPipeEndpoint>
require_pipe_endpoint_from_port(const PortSpec& port,
                                const std::string& context);
// Resolves subgraph boundary mappings to executable links.
std::vector<SubgraphNode::BridgeLink> build_subgraph_bridge_links_locked(
    const NodeSpec& outer_node,
    const std::vector<NodeSpec>& inner_nodes,
    const std::unordered_map<std::string,
                             NodePortRef>& input_port_mappings,
    const std::unordered_map<std::string,
                             NodePortRef>& output_port_mappings,
    std::map<std::string,
             MappingTransportInfo>& input_transports,
    std::map<std::string,
             MappingTransportInfo>& output_transports);
// Validates declared connections.
void validate_locked(const PipelineBuilder::State& state);
// Collects input labels for failure diagnostics.
std::vector<std::string>
collect_input_port_names_locked(const NodeSpec& node_spec,
                                const std::vector<EdgeSpec>& channels);
// Collects output labels for failure diagnostics.
std::vector<std::string>
collect_output_port_names_locked(const NodeSpec& node_spec,
                                 const std::vector<EdgeSpec>& channels);
// Validates child boundary mappings.
void validate_subgraph_port_mappings_locked(
    const NodeSpec& outer_node,
    const std::vector<NodeSpec>& inner_nodes,
    const std::unordered_map<std::string,
                             NodePortRef>& input_port_mappings,
    const std::unordered_map<std::string,
                             NodePortRef>& output_port_mappings);
} // namespace builder_detail
} // namespace takt
