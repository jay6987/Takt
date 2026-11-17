// Implements DOT graph rendering helpers.
#include "graphviz_detail.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>

namespace takt::dot_detail
{
/// Counts mapping modes used in collapsed graph labels.
struct MappingCounts
{
    size_t direct = 0;
    size_t bridge = 0;
    size_t unresolved = 0;
};

// Counts the transports on one boundary direction.
void count_mappings(const PipelineGraph& inner,
                    const std::map<std::string,
                                   NodePortRef>& mappings,
                    bool input,
                    MappingCounts& counts)
{
    for (const auto& mapping : mappings)
        switch (transport_info(inner, mapping.first, input).mode)
        {
        case MappingTransport::Direct:
            ++counts.direct;
            break;
        case MappingTransport::Bridge:
            ++counts.bridge;
            break;
        case MappingTransport::Unresolved:
            ++counts.unresolved;
            break;
        }
}

// Counts direct, bridge and unresolved mappings for collapsed graph labels.
std::string mapping_summary(const PipelineGraph& inner)
{
    MappingCounts counts;
    count_mappings(inner, inner.input_mappings, true, counts);
    count_mappings(inner, inner.output_mappings, false, counts);
    return "\ndirect=" + std::to_string(counts.direct) +
           " bridge=" + std::to_string(counts.bridge) +
           " unresolved=" + std::to_string(counts.unresolved);
}

// Renders nodes and recursively expands selected subgraphs.
void write_nodes(std::ostream& output,
                 const PipelineGraph& graph,
                 const std::string& prefix,
                 const DotOptions& options)
{
    for (size_t index = 0; index < graph.nodes.size(); ++index)
    {
        const auto& node = graph.nodes[index];
        const auto id = node_id(prefix, index);
        const auto child = graph.subgraphs.find(node.name);
        if (options.expand_subgraphs && child != graph.subgraphs.end())
        {
            const auto inner_prefix = id + "_";
            output << "  subgraph cluster_" << id << " {\n"
                   << "    label="
                   << quoted(node.name + "\n" + child->second.name) << ";\n";
            write_ports(output, graph, node, child->second, id, options);
            write_graph(output, child->second, inner_prefix, options);
            write_mappings(output, graph, node, child->second, prefix,
                           inner_prefix, options);
            output << "  }\n";
        }
        else
        {
            output << "  " << id << " [label="
                   << quoted(node.name + (child == graph.subgraphs.end()
                                              ? ""
                                              : mapping_summary(child->second)))
                   << ",shape="
                   << (child == graph.subgraphs.end() ? "box" : "box3d")
                   << "];\n";
        }
    }
}

void write_graph(std::ostream& output,
                 const PipelineGraph& graph,
                 const std::string& prefix,
                 const DotOptions& options)
{
    write_nodes(output, graph, prefix, options);
    for (const auto& edge : graph.edges)
    {
        const auto label = (edge.name.empty() ? "" : edge.name + "\n") +
                           edge.from_port + " -> " + edge.to_port;
        const auto source = resolve_endpoint(graph, prefix, edge.from_node,
                                             edge.from_port, false, options);
        const auto target = resolve_endpoint(graph, prefix, edge.to_node,
                                             edge.to_port, true, options);
        write_edge(output, source.id, target.id,
                   label + source.annotation + target.annotation);
    }
}
} // namespace takt::dot_detail
