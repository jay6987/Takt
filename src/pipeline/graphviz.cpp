// Renders expanded, collapsed and interface DOT views of pipeline bindings.
#include "takt/pipeline/graphviz.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>

namespace takt
{
namespace
{
// Escapes DOT quoted strings and rejects unsupported control characters.
std::string quoted(const std::string& value)
{
    std::string result = "\"";
    for (const char character : value)
    {
        switch (character)
        {
        case '"':
            result += "\\\"";
            break;
        case '\\':
            result += "\\\\";
            break;
        case '\n':
            result += "\\n";
            break;
        case '\r':
            result += "\\r";
            break;
        case '\t':
            result += "\\t";
            break;
        default:
            if (static_cast<unsigned char>(character) < 32)
            {
                throw std::invalid_argument(
                    "DOT labels contain unsupported control characters");
            }
            result += character;
        }
    }
    return result + '"';
}

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

// Forms stable internal DOT identifiers without relying on user labels.
std::string node_id(const std::string& prefix,
                    size_t index)
{
    return prefix + "n" + std::to_string(index);
}

// Returns recorded mapping transport or explicit unresolved metadata.
MappingTransportInfo transport_info(const PipelineGraph& inner,
                                    const std::string& port,
                                    bool input)
{
    const auto& transports =
        input ? inner.input_transports : inner.output_transports;
    const auto found = transports.find(port);
    return found == transports.end() ? MappingTransportInfo{} : found->second;
}

// Forms a stable DOT identifier for a declared subgraph boundary port.
std::string boundary_id(const NodeSpec& node,
                        const std::string& id,
                        const std::string& port,
                        bool input)
{
    const auto& ports = input ? node.input_ports : node.output_ports;
    const auto found =
        std::find_if(ports.begin(), ports.end(), [&](const PortSpec& candidate)
                     { return candidate.name == port; });
    return id + (input ? "_in" : "_out") +
           std::to_string(found - ports.begin());
}

// Emits an escaped labeled edge, optionally styled as a logical mapping.
void write_edge(std::ostream& output,
                const std::string& source,
                const std::string& target,
                const std::string& label,
                bool mapping = false)
{
    output << "  " << source << " -> " << target << " [label=" << quoted(label);
    if (mapping)
    {
        output << ",style=dashed";
    }
    output << "];\n";
}

struct Endpoint
{
    std::string id;
    std::string annotation;
};

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

// Emits real bridge workers or logical/direct boundary mapping edges.
void write_mappings(std::ostream& output,
                    const PipelineGraph& outer,
                    const NodeSpec& node,
                    const PipelineGraph& inner,
                    const std::string& prefix,
                    const std::string& inner_prefix,
                    const DotOptions& options)
{
    const auto write = [&](const auto& mappings, bool input)
    {
        const auto id = node_id(prefix, node_index(outer, node.name));
        for (const auto& mapping : mappings)
        {
            const auto boundary = boundary_id(node, id, mapping.first, input);
            const auto endpoint =
                resolve_endpoint(inner, inner_prefix, mapping.second.node_name,
                                 mapping.second.port_name, input, options);
            const auto info = transport_info(inner, mapping.first, input);
            const auto ports =
                input ? mapping.first + " -> " + mapping.second.port_name
                      : mapping.second.port_name + " -> " + mapping.first;
            if (info.mode == MappingTransport::Bridge)
            {
                const auto bridge = boundary + "_bridge";
                output << "  " << bridge << " [shape=component,label="
                       << quoted("bridge: " + ports + "\ndedicated worker\n" +
                                 info.source_pipe + " -> " + info.target_pipe)
                       << "];\n";
                if (options.view == DotView::Interfaces)
                    write_edge(output, input ? boundary : bridge,
                               input ? bridge : boundary,
                               input ? info.source_pipe : info.target_pipe);
                write_edge(output, input ? bridge : endpoint.id,
                           input ? endpoint.id : bridge,
                           (input ? info.target_pipe : info.source_pipe) +
                               "\n" + ports + endpoint.annotation);
            }
            else if (show_boundary(outer, node, inner, mapping.first, input,
                                   options))
            {
                const auto label =
                    ports +
                    (info.mode == MappingTransport::Direct
                         ? "\ndirect/shared Pipe: " + info.source_pipe
                         : "\nunresolved") +
                    endpoint.annotation;
                write_edge(output, input ? boundary : endpoint.id,
                           input ? endpoint.id : boundary, label,
                           info.mode == MappingTransport::Unresolved);
            }
        }
    };
    write(inner.input_mappings, true);
    write(inner.output_mappings, false);
}

// Counts direct, bridge and unresolved mappings for collapsed graph labels.
std::string mapping_summary(const PipelineGraph& inner)
{
    size_t direct = 0, bridge = 0, unresolved = 0;
    const auto count = [&](const auto& mappings, bool input)
    {
        for (const auto& mapping : mappings)
            switch (transport_info(inner, mapping.first, input).mode)
            {
            case MappingTransport::Direct:
                ++direct;
                break;
            case MappingTransport::Bridge:
                ++bridge;
                break;
            case MappingTransport::Unresolved:
                ++unresolved;
                break;
            }
    };
    count(inner.input_mappings, true);
    count(inner.output_mappings, false);
    return "\ndirect=" + std::to_string(direct) +
           " bridge=" + std::to_string(bridge) +
           " unresolved=" + std::to_string(unresolved);
}

// Recursively emits nodes, nested clusters and resolved connection edges.
void write_graph(std::ostream& output,
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
} // namespace

std::string to_dot(const PipelineGraph& graph,
                   DotOptions options)
{
    if (options.view != DotView::Connections &&
        options.view != DotView::Interfaces)
        throw std::invalid_argument("DOT options have invalid view");
    validate_graph(graph);
    std::ostringstream output;
    output << "digraph pipeline {\n  graph [label=" << quoted(graph.name)
           << ",labelloc=t];\n  rankdir="
           << (options.left_to_right ? "LR" : "TB") << ";\n";
    write_graph(output, graph, "g_", options);
    output << "}\n";
    return output.str();
}
} // namespace takt
