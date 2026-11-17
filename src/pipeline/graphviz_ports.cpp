// Implements DOT graph rendering helpers.
#include "graphviz_detail.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>

namespace takt::dot_detail
{
// Reports whether an edge or parent mapping references the selected port.
bool connected_port(const PipelineGraph& graph,
                    const NodeSpec& node,
                    const std::string& port,
                    bool input)
{
    for (const auto& edge : graph.edges)
        if (input ? edge.to_node == node.name && edge.to_port == port
                  : edge.from_node == node.name && edge.from_port == port)
            return true;
    const auto& mappings = input ? graph.input_mappings : graph.output_mappings;
    for (const auto& mapping : mappings)
        if (mapping.second.node_name == node.name &&
            mapping.second.port_name == port)
            return true;
    return false;
}

// Keeps logical ports visible only when required by the selected graph view.
bool show_boundary(const PipelineGraph& graph,
                   const NodeSpec& node,
                   const PipelineGraph& inner,
                   const std::string& port,
                   bool input,
                   const DotOptions& options)
{
    const auto info = transport_info(inner, port, input);
    return options.view == DotView::Interfaces ||
           info.mode == MappingTransport::Unresolved ||
           (info.mode == MappingTransport::Direct &&
            !connected_port(graph, node, port, input));
}

// Emits visible subgraph boundary ports and fallback empty-node labels.
void write_ports(std::ostream& output,
                 const PipelineGraph& graph,
                 const NodeSpec& node,
                 const PipelineGraph& inner,
                 const std::string& id,
                 const DotOptions& options)
{
    for (const auto& port : node.input_ports)
    {
        if (!show_boundary(graph, node, inner, port.name, true, options))
            continue;
        output << "  " << boundary_id(node, id, port.name, true)
               << " [shape=ellipse,label=" << quoted("in: " + port.name)
               << "];\n";
    }
    for (const auto& port : node.output_ports)
    {
        if (!show_boundary(graph, node, inner, port.name, false, options))
            continue;
        output << "  " << boundary_id(node, id, port.name, false)
               << " [shape=ellipse,label=" << quoted("out: " + port.name)
               << "];\n";
    }
    if (node.input_ports.empty() && node.output_ports.empty())
    {
        output << "  " << id << " [shape=box,label=" << quoted(node.name)
               << "];\n";
    }
}

} // namespace takt::dot_detail
