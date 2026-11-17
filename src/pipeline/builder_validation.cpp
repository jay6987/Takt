// Implements pipeline builder topology operations.
#include "builder_detail.h"

#include "takt/pipeline/pipeline_runtime.h"
#include <algorithm>
#include <optional>
#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace takt::builder_detail
{
// Rejects channels referencing missing nodes or ports while state is locked.
void validate_locked(const PipelineBuilder::State& state)
{
    for (const auto& channel : state.channels)
    {
        const auto* source_node =
            find_node_locked(state.nodes, channel.from_node);
        const auto* target_node =
            find_node_locked(state.nodes, channel.to_node);
        if (!source_node || !target_node)
        {
            throw std::logic_error("channel references an undeclared node");
        }
        if (!find_port_locked(source_node->output_ports, channel.from_port))
        {
            throw std::logic_error(
                "channel references a missing source output port");
        }
        if (!find_port_locked(target_node->input_ports, channel.to_port))
        {
            throw std::logic_error(
                "channel references a missing target input port");
        }
    }
}

// Rejects missing mappings for a boundary direction.
void require_mappings(const std::vector<PortSpec>& outer_ports,
                      const std::unordered_map<std::string,
                                               NodePortRef>& mappings,
                      bool input)
{
    for (const auto& outer_port : outer_ports)
    {
        if (mappings.find(outer_port.name) == mappings.end())
            throw std::logic_error(
                input
                    ? "missing subgraph input mapping for outer input port"
                    : "missing subgraph output mapping for outer output port");
    }
}

// Validates the mapped boundary ports in one direction.
void validate_mapping_direction(const std::vector<PortSpec>& outer_ports,
                                const std::vector<NodeSpec>& inner_nodes,
                                const std::unordered_map<std::string,
                                                         NodePortRef>& mappings,
                                bool input)
{
    for (const auto& mapping : mappings)
    {
        if (!find_port_locked(outer_ports, mapping.first))
            throw std::logic_error(input ? "subgraph input mapping references "
                                           "unknown outer input port"
                                         : "subgraph output mapping references "
                                           "unknown outer output port");
        const auto* inner_node =
            find_node_locked(inner_nodes, mapping.second.node_name);
        if (!inner_node)
            throw std::logic_error(
                input
                    ? "subgraph input mapping references unknown inner node"
                    : "subgraph output mapping references unknown inner node");
        const auto& ports =
            input ? inner_node->input_ports : inner_node->output_ports;
        if (!find_port_locked(ports, mapping.second.port_name))
            throw std::logic_error(input ? "subgraph input mapping references "
                                           "unknown inner input port"
                                         : "subgraph output mapping references "
                                           "unknown inner output port");
    }
}

// Requires complete mappings to existing child ports of the right direction.
void validate_subgraph_port_mappings_locked(
    const NodeSpec& outer_node,
    const std::vector<NodeSpec>& inner_nodes,
    const std::unordered_map<std::string,
                             NodePortRef>& input_port_mappings,
    const std::unordered_map<std::string,
                             NodePortRef>& output_port_mappings)
{
    require_mappings(outer_node.input_ports, input_port_mappings, true);
    require_mappings(outer_node.output_ports, output_port_mappings, false);
    validate_mapping_direction(outer_node.input_ports, inner_nodes,
                               input_port_mappings, true);
    validate_mapping_direction(outer_node.output_ports, inner_nodes,
                               output_port_mappings, false);
}
} // namespace takt::builder_detail
