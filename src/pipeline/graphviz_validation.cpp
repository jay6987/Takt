// Implements DOT graph rendering helpers.
#include "graphviz_detail.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>

namespace takt::dot_detail
{
// Resolves a declared node index or rejects an unknown graph reference.
size_t node_index(const PipelineGraph& graph,
                  const std::string& name)
{
    const auto node = std::find_if(graph.nodes.begin(), graph.nodes.end(),
                                   [&](const NodeSpec& candidate)
                                   { return candidate.name == name; });
    if (node == graph.nodes.end())
    {
        throw std::invalid_argument("DOT graph references unknown node: " +
                                    name);
    }
    return static_cast<size_t>(node - graph.nodes.begin());
}

// Rejects a graph reference to an undeclared port.
void require_port(const std::vector<PortSpec>& ports,
                  const std::string& name)
{
    if (std::none_of(ports.begin(), ports.end(),
                     [&](const PortSpec& port) { return port.name == name; }))
    {
        throw std::invalid_argument("DOT graph references unknown port: " +
                                    name);
    }
}

// Validates child mappings and transport metadata against declared ports.
void validate_mappings(const NodeSpec& outer,
                       const PipelineGraph& inner,
                       bool input)
{
    const auto& mappings = input ? inner.input_mappings : inner.output_mappings;
    const auto& transports =
        input ? inner.input_transports : inner.output_transports;
    const auto& outer_ports = input ? outer.input_ports : outer.output_ports;
    for (const auto& mapping : mappings)
    {
        require_port(outer_ports, mapping.first);
        const auto& target =
            inner.nodes[node_index(inner, mapping.second.node_name)];
        require_port(input ? target.input_ports : target.output_ports,
                     mapping.second.port_name);
    }
    for (const auto& transport : transports)
    {
        if (mappings.count(transport.first) == 0)
            throw std::invalid_argument(
                "DOT transport references unknown mapping: " + transport.first);
        const auto& info = transport.second;
        switch (info.mode)
        {
        case MappingTransport::Unresolved:
        case MappingTransport::Bridge:
            break;
        case MappingTransport::Direct:
            if (info.source_pipe != info.target_pipe)
                throw std::invalid_argument(
                    "DOT direct mapping has different pipe names");
            break;
        default:
            throw std::invalid_argument(
                "DOT mapping has invalid transport mode");
        }
    }
}

// Recursively rejects invalid graph names, edges and nested mappings.
void validate_graph(const PipelineGraph& graph)
{
    std::map<std::string, bool> names;
    for (const auto& node : graph.nodes)
    {
        if (node.name.empty() || !names.emplace(node.name, true).second)
        {
            throw std::invalid_argument(
                "DOT graph node names must be nonempty and unique");
        }
    }
    for (const auto& edge : graph.edges)
    {
        require_port(
            graph.nodes[node_index(graph, edge.from_node)].output_ports,
            edge.from_port);
        require_port(graph.nodes[node_index(graph, edge.to_node)].input_ports,
                     edge.to_port);
    }
    for (const auto& child : graph.subgraphs)
    {
        const auto& outer = graph.nodes[node_index(graph, child.first)];
        validate_mappings(outer, child.second, true);
        validate_mappings(outer, child.second, false);
        validate_graph(child.second);
    }
}

} // namespace takt::dot_detail
