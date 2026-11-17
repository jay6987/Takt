// Renders direct and bridged subgraph boundary mappings.
#include "graphviz_detail.h"

#include <ostream>

namespace takt::dot_detail
{
namespace
{
/// Holds the graph context needed to render each boundary mapping.
struct MappingWriter
{
    std::ostream& output;
    const PipelineGraph& outer;
    const NodeSpec& node;
    const PipelineGraph& inner;
    const std::string& prefix;
    const std::string& inner_prefix;
    const DotOptions& options;

    // Renders one mapping in its configured transport mode.
    void write_one(const std::pair<const std::string,
                                   NodePortRef>& mapping,
                   bool input) const;
    // Renders a dedicated bridge and its connection edges.
    void write_bridge(const std::string& boundary,
                      const Endpoint& endpoint,
                      const MappingTransportInfo& info,
                      const std::string& ports,
                      bool input) const;
};

void MappingWriter::write_bridge(const std::string& boundary,
                                 const Endpoint& endpoint,
                                 const MappingTransportInfo& info,
                                 const std::string& ports,
                                 bool input) const
{
    const auto bridge = boundary + "_bridge";
    output << "  " << bridge << " [shape=component,label="
           << quoted("bridge: " + ports + "\ndedicated worker\n" +
                     info.source_pipe + " -> " + info.target_pipe)
           << "];\n";
    if (options.view == DotView::Interfaces)
        write_edge(output, input ? boundary : bridge, input ? bridge : boundary,
                   input ? info.source_pipe : info.target_pipe);
    write_edge(output, input ? bridge : endpoint.id,
               input ? endpoint.id : bridge,
               (input ? info.target_pipe : info.source_pipe) + "\n" + ports +
                   endpoint.annotation);
}

void MappingWriter::write_one(const std::pair<const std::string,
                                              NodePortRef>& mapping,
                              bool input) const
{
    const auto id = node_id(prefix, node_index(outer, node.name));
    const auto boundary = boundary_id(node, id, mapping.first, input);
    const auto endpoint =
        resolve_endpoint(inner, inner_prefix, mapping.second.node_name,
                         mapping.second.port_name, input, options);
    const auto info = transport_info(inner, mapping.first, input);
    const auto ports = input
                           ? mapping.first + " -> " + mapping.second.port_name
                           : mapping.second.port_name + " -> " + mapping.first;
    if (info.mode == MappingTransport::Bridge)
        write_bridge(boundary, endpoint, info, ports, input);
    else if (show_boundary(outer, node, inner, mapping.first, input, options))
    {
        const auto label = ports +
                           (info.mode == MappingTransport::Direct
                                ? "\ndirect/shared Pipe: " + info.source_pipe
                                : "\nunresolved") +
                           endpoint.annotation;
        write_edge(output, input ? boundary : endpoint.id,
                   input ? endpoint.id : boundary, label,
                   info.mode == MappingTransport::Unresolved);
    }
}
} // namespace

void write_mappings(std::ostream& output,
                    const PipelineGraph& outer,
                    const NodeSpec& node,
                    const PipelineGraph& inner,
                    const std::string& prefix,
                    const std::string& inner_prefix,
                    const DotOptions& options)
{
    const MappingWriter writer{output, outer,        node,   inner,
                               prefix, inner_prefix, options};
    for (const auto& mapping : inner.input_mappings)
        writer.write_one(mapping, true);
    for (const auto& mapping : inner.output_mappings)
        writer.write_one(mapping, false);
}
} // namespace takt::dot_detail
