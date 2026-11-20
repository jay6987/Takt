// Converts generic TOML graph descriptions into Takt's static DOT model.
#include "takt/pipeline/toml_config.h"

#include "takt/pipeline/graphviz.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace takt
{
namespace
{
NodePortRef split_port(const std::string &ref)
{
    const auto dot = ref.find('.');
    if (dot == 0 || dot == std::string::npos || dot + 1 == ref.size() ||
        ref.find('.', dot + 1) != std::string::npos)
        throw std::invalid_argument("invalid port reference: " + ref);
    return {ref.substr(0, dot), ref.substr(dot + 1)};
}

NodeSpec node_spec(const std::string &id, const PipelineGraph &child)
{
    NodeSpec node{id, {}, {}, {}};
    for (const auto &[name, ref] : child.input_mappings)
        node.input_ports.push_back({name, PortDirection::Input, {}});
    for (const auto &[name, ref] : child.output_mappings)
        node.output_ports.push_back({name, PortDirection::Output, {}});
    return node;
}

void add_leaf_port(PipelineGraph &graph,
                   const NodePortRef &ref,
                   PortDirection direction)
{
    const auto node = std::find_if(
        graph.nodes.begin(), graph.nodes.end(),
        [&](const NodeSpec &candidate) { return candidate.name == ref.node_name; });
    if (node == graph.nodes.end())
        throw std::invalid_argument("DOT graph references unknown node '" +
                                    ref.node_name + "'");
    const auto &ports = direction == PortDirection::Input
                            ? node->input_ports
                            : node->output_ports;
    if (std::find_if(ports.begin(), ports.end(),
                     [&](const PortSpec &port) { return port.name == ref.port_name; }) !=
        ports.end())
        return;
    auto &mutable_ports = direction == PortDirection::Input
                              ? node->input_ports
                              : node->output_ports;
    mutable_ports.push_back({ref.port_name, direction, {}});
}

void connect_boundary(PipelineGraph &parent, const NodePortRef &ref,
                      bool input, const std::string &pipe)
{
    const auto it = parent.subgraphs.find(ref.node_name);
    if (it == parent.subgraphs.end())
        return;
    auto &child = it->second;
    auto &transports = input ? child.input_transports : child.output_transports;
    const auto &mappings = input ? child.input_mappings : child.output_mappings;
    const auto mapping = mappings.find(ref.port_name);
    if (mapping == mappings.end())
        throw std::invalid_argument("unmapped subgraph boundary port '" +
                                    ref.node_name + "." + ref.port_name + "'");
    transports[ref.port_name] = {MappingTransport::Direct, pipe, pipe};
    connect_boundary(child, mapping->second, input, pipe);
}

std::string pipe_label(const std::string &name, const PipeConfig &pipe)
{
    switch (pipe.mode)
    {
    case CapacityMode::Minimal:
        return name + " [minimal]";
    case CapacityMode::MaxConcurrency:
        return name + " [max_concurrency]";
    case CapacityMode::Fixed:
        if (!pipe.slots)
            throw std::invalid_argument("fixed pipe capacity is missing slots");
        return name + " [fixed: " + std::to_string(*pipe.slots) + "]";
    }
    throw std::logic_error("unknown pipe capacity mode");
}

PipelineGraph describe(const PipelineConfig &config,
                       const GraphConfig &spec,
                       const std::string &name)
{
    PipelineGraph graph;
    graph.name = name;
    for (const auto &[id, node] : spec.nodes)
    {
        if (node.subgraph.empty())
        {
            graph.nodes.push_back({id, {}, {}, {}});
            continue;
        }
        const auto child_config = config.subgraphs.find(node.subgraph);
        if (child_config == config.subgraphs.end())
            throw std::invalid_argument("unknown subgraph '" + node.subgraph + "'");
        auto child = describe(config, child_config->second, id);
        graph.nodes.push_back(node_spec(id, child));
        graph.subgraphs.emplace(id, std::move(child));
    }
    for (const auto &[port, ref] : spec.inputs)
        graph.input_mappings.emplace(port, split_port(ref));
    for (const auto &[port, ref] : spec.outputs)
        graph.output_mappings.emplace(port, split_port(ref));
    for (const auto &[id, pipe] : spec.pipes)
    {
        const auto from = split_port(pipe.from);
        const auto to = split_port(pipe.to);
        add_leaf_port(graph, from, PortDirection::Output);
        add_leaf_port(graph, to, PortDirection::Input);
        connect_boundary(graph, from, false, id);
        connect_boundary(graph, to, true, id);
        graph.edges.push_back({pipe_label(id, pipe), from.node_name,
                               from.port_name, to.node_name, to.port_name, {}});
    }
    for (const auto &[port, ref] : spec.inputs)
        add_leaf_port(graph, split_port(ref), PortDirection::Input);
    for (const auto &[port, ref] : spec.outputs)
        add_leaf_port(graph, split_port(ref), PortDirection::Output);
    return graph;
}
} // namespace

std::string export_pipeline_toml_dot(const std::filesystem::path &path,
                                     bool expand_subgraphs,
                                     bool show_interfaces)
{
    const auto config = read_pipeline_toml(path);
    const auto name = path.stem().string();
    auto graph = describe(config, config.root, name);
    if (!graph.input_mappings.empty() || !graph.output_mappings.empty())
    {
        PipelineGraph outer;
        outer.name = name;
        outer.nodes.push_back(node_spec(name, graph));
        outer.subgraphs.emplace(name, std::move(graph));
        graph = std::move(outer);
        expand_subgraphs = true;
    }
    return to_dot(graph, {expand_subgraphs, true,
                          show_interfaces ? DotView::Interfaces
                                          : DotView::Connections});
}
} // namespace takt
