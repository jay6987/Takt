#include "takt/pipeline/graphviz.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>

namespace takt
{
namespace
{
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

size_t node_index(const PipelineGraph& graph, const std::string& name)
{
    const auto node =
        std::find_if(graph.nodes.begin(), graph.nodes.end(),
                     [&](const NodeSpec& candidate) { return candidate.name == name; });
    if (node == graph.nodes.end())
    {
        throw std::invalid_argument("DOT graph references unknown node: " + name);
    }
    return static_cast<size_t>(node - graph.nodes.begin());
}

void require_port(const std::vector<PortSpec>& ports, const std::string& name)
{
    if (std::none_of(ports.begin(), ports.end(),
                     [&](const PortSpec& port) { return port.name == name; }))
    {
        throw std::invalid_argument("DOT graph references unknown port: " + name);
    }
}

void validate_mappings(const NodeSpec& outer, const PipelineGraph& inner, bool input)
{
    const auto& mappings = input ? inner.input_mappings : inner.output_mappings;
    const auto& outer_ports = input ? outer.input_ports : outer.output_ports;
    for (const auto& mapping : mappings)
    {
        require_port(outer_ports, mapping.first);
        const auto& target = inner.nodes[node_index(inner, mapping.second.node_name)];
        require_port(input ? target.input_ports : target.output_ports,
                     mapping.second.port_name);
    }
}

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
        require_port(graph.nodes[node_index(graph, edge.from_node)].output_ports,
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

std::string node_id(const std::string& prefix, size_t index)
{
    return prefix + "n" + std::to_string(index);
}

void write_edge(std::ostream& output, const std::string& source,
                const std::string& target, const std::string& label,
                bool mapping = false)
{
    output << "  " << source << " -> " << target << " [label=" << quoted(label);
    if (mapping)
    {
        output << ",style=dashed";
    }
    output << "];\n";
}

std::string endpoint_id(const PipelineGraph& graph, const std::string& prefix,
                        const std::string& node, const std::string& port, bool input,
                        const DotOptions& options)
{
    const auto index = node_index(graph, node);
    auto id = node_id(prefix, index);
    if (options.expand_subgraphs && graph.subgraphs.count(node) != 0)
    {
        const auto& ports =
            input ? graph.nodes[index].input_ports : graph.nodes[index].output_ports;
        const auto found =
            std::find_if(ports.begin(), ports.end(), [&](const PortSpec& candidate)
                         { return candidate.name == port; });
        id += (input ? "_in" : "_out") + std::to_string(found - ports.begin());
    }
    return id;
}

void write_ports(std::ostream& output, const NodeSpec& node, const std::string& id)
{
    for (size_t i = 0; i < node.input_ports.size(); ++i)
    {
        output << "  " << id << "_in" << i
               << " [shape=ellipse,label=" << quoted("in: " + node.input_ports[i].name)
               << "];\n";
    }
    for (size_t i = 0; i < node.output_ports.size(); ++i)
    {
        output << "  " << id << "_out" << i << " [shape=ellipse,label="
               << quoted("out: " + node.output_ports[i].name) << "];\n";
    }
    if (node.input_ports.empty() && node.output_ports.empty())
    {
        output << "  " << id << " [shape=box,label=" << quoted(node.name) << "];\n";
    }
}

void write_mappings(std::ostream& output, const PipelineGraph& outer,
                    const NodeSpec& node, const PipelineGraph& inner,
                    const std::string& prefix, const std::string& inner_prefix,
                    const DotOptions& options)
{
    for (const auto& mapping : inner.input_mappings)
    {
        write_edge(output,
                   endpoint_id(outer, prefix, node.name, mapping.first, true, options),
                   endpoint_id(inner, inner_prefix, mapping.second.node_name,
                               mapping.second.port_name, true, options),
                   mapping.first + " -> " + mapping.second.port_name, true);
    }
    for (const auto& mapping : inner.output_mappings)
    {
        write_edge(output,
                   endpoint_id(inner, inner_prefix, mapping.second.node_name,
                               mapping.second.port_name, false, options),
                   endpoint_id(outer, prefix, node.name, mapping.first, false, options),
                   mapping.second.port_name + " -> " + mapping.first, true);
    }
}

void write_graph(std::ostream& output, const PipelineGraph& graph,
                 const std::string& prefix, const DotOptions& options)
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
                   << "    label=" << quoted(node.name + "\n" + child->second.name)
                   << ";\n";
            write_ports(output, node, id);
            write_graph(output, child->second, inner_prefix, options);
            write_mappings(output, graph, node, child->second, prefix, inner_prefix,
                           options);
            output << "  }\n";
        }
        else
        {
            output << "  " << id << " [label=" << quoted(node.name)
                   << ",shape=" << (child == graph.subgraphs.end() ? "box" : "box3d")
                   << "];\n";
        }
    }
    for (const auto& edge : graph.edges)
    {
        const auto label = (edge.name.empty() ? "" : edge.name + "\n") +
                           edge.from_port + " -> " + edge.to_port;
        write_edge(
            output,
            endpoint_id(graph, prefix, edge.from_node, edge.from_port, false, options),
            endpoint_id(graph, prefix, edge.to_node, edge.to_port, true, options),
            label);
    }
}
} // namespace

std::string to_dot(const PipelineGraph& graph, DotOptions options)
{
    validate_graph(graph);
    std::ostringstream output;
    output << "digraph pipeline {\n  graph [label=" << quoted(graph.name)
           << ",labelloc=t];\n  rankdir=" << (options.left_to_right ? "LR" : "TB")
           << ";\n";
    write_graph(output, graph, "g_", options);
    output << "}\n";
    return output.str();
}
} // namespace takt
