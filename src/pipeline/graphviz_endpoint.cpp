// Implements DOT graph rendering helpers.
#include "graphviz_detail.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>

namespace takt::dot_detail
{
// Resolves visible endpoints through nested direct maps or bridge workers.
Endpoint resolve_endpoint(const PipelineGraph& graph,
                          const std::string& prefix,
                          const std::string& node,
                          const std::string& port,
                          bool input,
                          const DotOptions& options)
{
    const auto index = node_index(graph, node);
    auto id = node_id(prefix, index);
    const auto child = graph.subgraphs.find(node);
    if (options.expand_subgraphs && child != graph.subgraphs.end())
    {
        const auto boundary = boundary_id(graph.nodes[index], id, port, input);
        if (options.view == DotView::Connections)
        {
            const auto& inner = child->second;
            const auto& mappings =
                input ? inner.input_mappings : inner.output_mappings;
            const auto mapping = mappings.find(port);
            if (mapping != mappings.end())
            {
                const auto info = transport_info(inner, port, input);
                if (info.mode == MappingTransport::Direct)
                {
                    auto endpoint = resolve_endpoint(
                        inner, id + "_", mapping->second.node_name,
                        mapping->second.port_name, input, options);
                    endpoint.annotation =
                        "\ndirect/shared Pipe: " + info.source_pipe +
                        endpoint.annotation;
                    return endpoint;
                }
                if (info.mode == MappingTransport::Bridge)
                    return {boundary + "_bridge",
                            "\nPipe: " +
                                (input ? info.source_pipe : info.target_pipe)};
            }
        }
        return {boundary, {}};
    }
    return {id, {}};
}

} // namespace takt::dot_detail
