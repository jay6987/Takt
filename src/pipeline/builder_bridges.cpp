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
// Requires nonnull endpoint metadata before binding executable transport.
std::shared_ptr<AnyPipeEndpoint>
require_pipe_endpoint_from_port(const PortSpec& port,
                                const std::string& context)
{
    const auto* endpoint =
        find_descriptor_value<std::shared_ptr<AnyPipeEndpoint>>(
            port.descriptor, "pipe_endpoint");
    if (!endpoint || !*endpoint)
    {
        throw std::logic_error("missing pipe_endpoint descriptor for " +
                               context + " port '" + port.name + "'");
    }
    return *endpoint;
}

// Appends mappings for one boundary direction and records their transport.
void append_bridge_links(const NodeSpec& outer_node,
                         const std::vector<NodeSpec>& inner_nodes,
                         const std::unordered_map<std::string,
                                                  NodePortRef>& mappings,
                         std::map<std::string,
                                  MappingTransportInfo>& transports,
                         std::vector<SubgraphNode::BridgeLink>& links,
                         bool input)
{
    for (const auto& mapping : mappings)
    {
        const auto& outer_ports =
            input ? outer_node.input_ports : outer_node.output_ports;
        const auto* outer_port = find_port_locked(outer_ports, mapping.first);
        const auto* inner_node =
            find_node_locked(inner_nodes, mapping.second.node_name);
        const auto* inner_port =
            inner_node ? find_port_locked(input ? inner_node->input_ports
                                                : inner_node->output_ports,
                                          mapping.second.port_name)
                       : nullptr;
        if (!outer_port || !inner_port)
            throw std::logic_error(
                input ? "invalid subgraph input bridge mapping"
                      : "invalid subgraph output bridge mapping");
        auto source = require_pipe_endpoint_from_port(
            input ? *outer_port : *inner_port,
            input ? "outer input" : "inner output");
        auto target = require_pipe_endpoint_from_port(
            input ? *inner_port : *outer_port,
            input ? "inner input" : "outer output");
        const auto outer_label = outer_node.name + "." + outer_port->name;
        const auto inner_label =
            mapping.second.node_name + "." + mapping.second.port_name;
        links.push_back({std::move(source), std::move(target),
                         input ? outer_label : inner_label,
                         input ? inner_label : outer_label});
        transports.emplace(
            mapping.first,
            classify_mapping(*links.back().source, *links.back().target));
    }
}

// Binds mapped endpoints and records their actual direct/bridge transport.
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
             MappingTransportInfo>& output_transports)
{
    std::vector<SubgraphNode::BridgeLink> links;
    links.reserve(input_port_mappings.size() + output_port_mappings.size());
    append_bridge_links(outer_node, inner_nodes, input_port_mappings,
                        input_transports, links, true);
    append_bridge_links(outer_node, inner_nodes, output_port_mappings,
                        output_transports, links, false);
    return links;
}

} // namespace takt::builder_detail
