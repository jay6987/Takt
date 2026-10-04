#include "takt/pipeline/pipeline_builder.h"

#include <algorithm>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "takt/pipeline/node.h"
#include "takt/pipeline/pipeline_runtime.h"
#include "takt/pipeline/subgraph_node.h"

namespace takt
{
struct PipelineBuilder::State
{
    explicit State(std::string builder_name) : name(std::move(builder_name)) {}

    std::string name;
    std::vector<NodeSpec> nodes;
    std::vector<EdgeSpec> channels;
    std::unordered_map<std::string, std::shared_ptr<NodeBase>> bound_nodes;
    mutable std::mutex mutex;
};

namespace
{
class Registry
{
  public:
    static Registry& instance()
    {
        static Registry* registry = new Registry();
        return *registry;
    }

    std::shared_ptr<PipelineBuilder::State> get_or_create_state(const std::string& name)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = states_.find(name);
        if (it != states_.end())
        {
            return it->second;
        }

        auto state = std::make_shared<PipelineBuilder::State>(name);
        states_.emplace(name, state);
        return state;
    }

  private:
    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<PipelineBuilder::State>> states_;
};

NodeSpec& ensure_node_locked(std::vector<NodeSpec>& nodes, const std::string& name)
{
    auto it = std::find_if(nodes.begin(), nodes.end(),
                           [&](const NodeSpec& node) { return node.name == name; });
    if (it == nodes.end())
    {
        NodeSpec spec;
        spec.name = name;
        nodes.push_back(std::move(spec));
        return nodes.back();
    }

    return *it;
}

const NodeSpec* find_node_locked(const std::vector<NodeSpec>& nodes,
                                 const std::string& name)
{
    auto it = std::find_if(nodes.begin(), nodes.end(),
                           [&](const NodeSpec& node) { return node.name == name; });
    return it == nodes.end() ? nullptr : &*it;
}

PortSpec& ensure_port_locked(std::vector<PortSpec>& ports, const std::string& port_name,
                             PortDirection direction, const PipeDescriptor& descriptor)
{
    auto it = std::find_if(ports.begin(), ports.end(), [&](const PortSpec& port)
                           { return port.name == port_name; });
    if (it == ports.end())
    {
        PortSpec spec;
        spec.name = port_name;
        spec.direction = direction;
        spec.descriptor = descriptor;
        ports.push_back(std::move(spec));
        return ports.back();
    }

    if (it->direction != direction)
    {
        throw std::logic_error("port direction mismatch");
    }

    for (const auto& kv : descriptor)
    {
        it->descriptor[kv.first] = kv.second;
    }
    return *it;
}

const PortSpec* find_port_locked(const std::vector<PortSpec>& ports,
                                 const std::string& port_name)
{
    auto it = std::find_if(ports.begin(), ports.end(), [&](const PortSpec& port)
                           { return port.name == port_name; });
    return it == ports.end() ? nullptr : &*it;
}

std::shared_ptr<AnyPipeEndpoint>
require_pipe_endpoint_from_port(const PortSpec& port, const std::string& context)
{
    const auto* endpoint = find_descriptor_value<std::shared_ptr<AnyPipeEndpoint>>(
        port.descriptor, "pipe_endpoint");
    if (!endpoint || !*endpoint)
    {
        throw std::logic_error("missing pipe_endpoint descriptor for " + context +
                               " port '" + port.name + "'");
    }
    return *endpoint;
}

std::vector<SubgraphNode::BridgeLink> build_subgraph_bridge_links_locked(
    const NodeSpec& outer_node, const std::vector<NodeSpec>& inner_nodes,
    const std::unordered_map<std::string, NodePortRef>& input_port_mappings,
    const std::unordered_map<std::string, NodePortRef>& output_port_mappings)
{
    std::vector<SubgraphNode::BridgeLink> links;
    links.reserve(input_port_mappings.size() + output_port_mappings.size());

    for (const auto& mapping : input_port_mappings)
    {
        const auto* outer_input =
            find_port_locked(outer_node.input_ports, mapping.first);
        const auto* inner_node =
            find_node_locked(inner_nodes, mapping.second.node_name);
        const auto* inner_input =
            inner_node
                ? find_port_locked(inner_node->input_ports, mapping.second.port_name)
                : nullptr;
        if (!outer_input || !inner_input)
        {
            throw std::logic_error("invalid subgraph input bridge mapping");
        }

        links.push_back(SubgraphNode::BridgeLink{
            require_pipe_endpoint_from_port(*outer_input, "outer input"),
            require_pipe_endpoint_from_port(*inner_input, "inner input"),
            outer_node.name + "." + outer_input->name,
            mapping.second.node_name + "." + mapping.second.port_name});
    }

    for (const auto& mapping : output_port_mappings)
    {
        const auto* outer_output =
            find_port_locked(outer_node.output_ports, mapping.first);
        const auto* inner_node =
            find_node_locked(inner_nodes, mapping.second.node_name);
        const auto* inner_output =
            inner_node
                ? find_port_locked(inner_node->output_ports, mapping.second.port_name)
                : nullptr;
        if (!outer_output || !inner_output)
        {
            throw std::logic_error("invalid subgraph output bridge mapping");
        }

        links.push_back(SubgraphNode::BridgeLink{
            require_pipe_endpoint_from_port(*inner_output, "inner output"),
            require_pipe_endpoint_from_port(*outer_output, "outer output"),
            mapping.second.node_name + "." + mapping.second.port_name,
            outer_node.name + "." + outer_output->name});
    }

    return links;
}

void validate_locked(const PipelineBuilder::State& state)
{
    for (const auto& channel : state.channels)
    {
        const auto* source_node = find_node_locked(state.nodes, channel.from_node);
        const auto* target_node = find_node_locked(state.nodes, channel.to_node);
        if (!source_node || !target_node)
        {
            throw std::logic_error("channel references an undeclared node");
        }
        if (!find_port_locked(source_node->output_ports, channel.from_port))
        {
            throw std::logic_error("channel references a missing source output port");
        }
        if (!find_port_locked(target_node->input_ports, channel.to_port))
        {
            throw std::logic_error("channel references a missing target input port");
        }
    }
}

std::vector<std::string>
collect_input_port_names_locked(const NodeSpec& node_spec,
                                const std::vector<EdgeSpec>& channels)
{
    std::vector<std::string> ports;
    std::unordered_set<std::string> seen;

    for (const auto& port_spec : node_spec.input_ports)
    {
        if (seen.insert(port_spec.name).second)
        {
            ports.push_back(port_spec.name);
        }
    }

    for (const auto& channel : channels)
    {
        if (channel.to_node != node_spec.name)
        {
            continue;
        }
        if (seen.insert(channel.to_port).second)
        {
            ports.push_back(channel.to_port);
        }
    }
    return ports;
}

std::vector<std::string>
collect_output_port_names_locked(const NodeSpec& node_spec,
                                 const std::vector<EdgeSpec>& channels)
{
    std::vector<std::string> ports;
    std::unordered_set<std::string> seen;

    for (const auto& port_spec : node_spec.output_ports)
    {
        if (seen.insert(port_spec.name).second)
        {
            ports.push_back(port_spec.name);
        }
    }

    for (const auto& channel : channels)
    {
        if (channel.from_node != node_spec.name)
        {
            continue;
        }
        if (seen.insert(channel.from_port).second)
        {
            ports.push_back(channel.from_port);
        }
    }
    return ports;
}

void validate_subgraph_port_mappings_locked(
    const NodeSpec& outer_node, const std::vector<NodeSpec>& inner_nodes,
    const std::unordered_map<std::string, NodePortRef>& input_port_mappings,
    const std::unordered_map<std::string, NodePortRef>& output_port_mappings)
{
    for (const auto& outer_port : outer_node.input_ports)
    {
        if (input_port_mappings.find(outer_port.name) == input_port_mappings.end())
        {
            throw std::logic_error(
                "missing subgraph input mapping for outer input port");
        }
    }

    for (const auto& outer_port : outer_node.output_ports)
    {
        if (output_port_mappings.find(outer_port.name) == output_port_mappings.end())
        {
            throw std::logic_error(
                "missing subgraph output mapping for outer output port");
        }
    }

    for (const auto& mapping : input_port_mappings)
    {
        const auto* outer_input =
            find_port_locked(outer_node.input_ports, mapping.first);
        if (!outer_input)
        {
            throw std::logic_error(
                "subgraph input mapping references unknown outer input port");
        }

        const auto* inner_node =
            find_node_locked(inner_nodes, mapping.second.node_name);
        if (!inner_node)
        {
            throw std::logic_error(
                "subgraph input mapping references unknown inner node");
        }
        if (!find_port_locked(inner_node->input_ports, mapping.second.port_name))
        {
            throw std::logic_error(
                "subgraph input mapping references unknown inner input port");
        }
    }

    for (const auto& mapping : output_port_mappings)
    {
        const auto* outer_output =
            find_port_locked(outer_node.output_ports, mapping.first);
        if (!outer_output)
        {
            throw std::logic_error(
                "subgraph output mapping references unknown outer output port");
        }

        const auto* inner_node =
            find_node_locked(inner_nodes, mapping.second.node_name);
        if (!inner_node)
        {
            throw std::logic_error(
                "subgraph output mapping references unknown inner node");
        }
        if (!find_port_locked(inner_node->output_ports, mapping.second.port_name))
        {
            throw std::logic_error(
                "subgraph output mapping references unknown inner output port");
        }
    }
}
} // namespace

PipelineBuilder PipelineBuilder::declare(std::string name)
{
    return PipelineBuilder(Registry::instance().get_or_create_state(name));
}

PipelineBuilder::PipelineBuilder(std::shared_ptr<State> state)
    : state_(std::move(state))
{
    if (!state_)
    {
        throw std::invalid_argument("pipeline builder state cannot be null");
    }
}

PipelineBuilder::PipelineBuilder(const PipelineBuilder&) = default;
PipelineBuilder& PipelineBuilder::operator=(const PipelineBuilder&) = default;
PipelineBuilder::PipelineBuilder(PipelineBuilder&&) noexcept = default;
PipelineBuilder& PipelineBuilder::operator=(PipelineBuilder&&) noexcept = default;
PipelineBuilder::~PipelineBuilder() = default;

const std::string& PipelineBuilder::name() const noexcept
{
    return state_->name;
}

PipelineBuilder& PipelineBuilder::add_node(std::string node_name,
                                           PipeDescriptor descriptor)
{
    if (node_name.empty())
    {
        throw std::invalid_argument("node name cannot be empty");
    }

    std::lock_guard<std::mutex> lk(state_->mutex);
    auto& node = ensure_node_locked(state_->nodes, std::move(node_name));
    for (const auto& kv : descriptor)
    {
        node.descriptor[kv.first] = kv.second;
    }
    return *this;
}

PipelineBuilder& PipelineBuilder::add_input_port(std::string node_name,
                                                 std::string port_name,
                                                 PipeDescriptor descriptor)
{
    if (node_name.empty())
    {
        throw std::invalid_argument("node name cannot be empty");
    }
    if (port_name.empty())
    {
        throw std::invalid_argument("port name cannot be empty");
    }

    std::lock_guard<std::mutex> lk(state_->mutex);
    auto& node = ensure_node_locked(state_->nodes, node_name);
    ensure_port_locked(node.input_ports, port_name, PortDirection::Input, descriptor);
    return *this;
}

PipelineBuilder& PipelineBuilder::add_output_port(std::string node_name,
                                                  std::string port_name,
                                                  PipeDescriptor descriptor)
{
    if (node_name.empty())
    {
        throw std::invalid_argument("node name cannot be empty");
    }
    if (port_name.empty())
    {
        throw std::invalid_argument("port name cannot be empty");
    }

    std::lock_guard<std::mutex> lk(state_->mutex);
    auto& node = ensure_node_locked(state_->nodes, node_name);
    ensure_port_locked(node.output_ports, port_name, PortDirection::Output, descriptor);
    return *this;
}

PipelineBuilder& PipelineBuilder::connect(std::string from_node, std::string from_port,
                                          std::string to_node, std::string to_port,
                                          PipeDescriptor descriptor,
                                          std::string channel_name)
{
    if (from_node.empty() || from_port.empty() || to_node.empty() || to_port.empty())
    {
        throw std::invalid_argument("connection endpoints cannot be empty");
    }

    std::lock_guard<std::mutex> lk(state_->mutex);
    auto* source_node = find_node_locked(state_->nodes, from_node);
    auto* target_node = find_node_locked(state_->nodes, to_node);
    if (!source_node || !target_node)
    {
        throw std::logic_error("connection endpoints must reference declared nodes");
    }

    if (!find_port_locked(source_node->output_ports, from_port))
    {
        throw std::logic_error("source output port is missing");
    }
    if (!find_port_locked(target_node->input_ports, to_port))
    {
        throw std::logic_error("target input port is missing");
    }

    EdgeSpec channel;
    channel.name = std::move(channel_name);
    channel.from_node = std::move(from_node);
    channel.from_port = std::move(from_port);
    channel.to_node = std::move(to_node);
    channel.to_port = std::move(to_port);
    channel.descriptor = std::move(descriptor);
    state_->channels.push_back(std::move(channel));
    return *this;
}

PipelineBuilder& PipelineBuilder::bind_node(std::string node_name,
                                            std::shared_ptr<NodeBase> node)
{
    if (node_name.empty())
    {
        throw std::invalid_argument("node name cannot be empty");
    }
    if (!node)
    {
        throw std::invalid_argument("node cannot be null");
    }

    std::lock_guard<std::mutex> lk(state_->mutex);
    if (!find_node_locked(state_->nodes, node_name))
    {
        throw std::logic_error("cannot bind undeclared node");
    }
    state_->bound_nodes[std::move(node_name)] = std::move(node);
    return *this;
}

PipelineBuilder& PipelineBuilder::bind_subgraph(std::string node_name,
                                                const PipelineBuilder& subgraph_builder)
{
    return bind_subgraph(std::move(node_name), subgraph_builder, {}, {});
}

PipelineBuilder& PipelineBuilder::bind_subgraph(
    std::string node_name, const PipelineBuilder& subgraph_builder,
    std::unordered_map<std::string, NodePortRef> input_port_mappings,
    std::unordered_map<std::string, NodePortRef> output_port_mappings)
{
    if (node_name.empty())
    {
        throw std::invalid_argument("node name cannot be empty");
    }

    if (state_.get() == subgraph_builder.state_.get())
    {
        throw std::logic_error("a builder cannot bind itself as a subgraph");
    }

    auto subgraph_runtime = subgraph_builder.build_runtime();

    NodeSpec outer_node;
    {
        std::lock_guard<std::mutex> outer_lk(state_->mutex);
        const auto* found = find_node_locked(state_->nodes, node_name);
        if (!found)
        {
            throw std::logic_error("cannot bind undeclared node");
        }
        outer_node = *found;
    }

    std::vector<SubgraphNode::BridgeLink> bridge_links;
    {
        std::lock_guard<std::mutex> inner_lk(subgraph_builder.state_->mutex);
        validate_subgraph_port_mappings_locked(
            outer_node, subgraph_builder.state_->nodes, input_port_mappings,
            output_port_mappings);

        bridge_links = build_subgraph_bridge_links_locked(
            outer_node, subgraph_builder.state_->nodes, input_port_mappings,
            output_port_mappings);
    }

    std::lock_guard<std::mutex> lk(state_->mutex);
    state_->bound_nodes[node_name] = std::make_shared<SubgraphNode>(
        node_name, std::move(subgraph_runtime), std::move(input_port_mappings),
        std::move(output_port_mappings), std::move(bridge_links));
    return *this;
}

bool PipelineBuilder::has_node(const std::string& node_name) const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    return find_node_locked(state_->nodes, node_name) != nullptr;
}

bool PipelineBuilder::has_bound_node(const std::string& node_name) const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    return state_->bound_nodes.find(node_name) != state_->bound_nodes.end();
}

const NodeSpec* PipelineBuilder::find_node(const std::string& node_name) const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    return find_node_locked(state_->nodes, node_name);
}

const EdgeSpec* PipelineBuilder::find_edge(const std::string& edge_name) const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    auto it = std::find_if(state_->channels.begin(), state_->channels.end(),
                           [&](const EdgeSpec& channel)
                           { return channel.name == edge_name; });
    return it == state_->channels.end() ? nullptr : &*it;
}

const std::vector<NodeSpec>& PipelineBuilder::nodes() const noexcept
{
    return state_->nodes;
}

const std::vector<EdgeSpec>& PipelineBuilder::edges() const noexcept
{
    return state_->channels;
}

std::shared_ptr<NodeBase>
PipelineBuilder::find_bound_node(const std::string& node_name) const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    auto it = state_->bound_nodes.find(node_name);
    return it == state_->bound_nodes.end() ? nullptr : it->second;
}

std::shared_ptr<PipelineRuntime> PipelineBuilder::build_runtime() const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    validate_locked(*state_);

    std::vector<std::shared_ptr<NodeBase>> ordered_nodes;
    ordered_nodes.reserve(state_->nodes.size());
    for (const auto& node_spec : state_->nodes)
    {
        auto it = state_->bound_nodes.find(node_spec.name);
        if (it == state_->bound_nodes.end() || !it->second)
        {
            throw std::logic_error(
                "every declared node must be bound before build_runtime");
        }

        auto& node = it->second;
        node->clear_input_port_names();
        node->clear_output_port_names();

        const auto input_ports =
            collect_input_port_names_locked(node_spec, state_->channels);
        const auto output_ports =
            collect_output_port_names_locked(node_spec, state_->channels);
        for (const auto& port : input_ports)
        {
            node->add_input_port_name(port);
        }
        for (const auto& port : output_ports)
        {
            node->add_output_port_name(port);
        }

        const std::string* stage_name =
            find_descriptor_value<std::string>(node_spec.descriptor, "stage_name");
        if (stage_name)
        {
            node->set_stage_name(*stage_name);
        }

        node->set_pipeline_name(state_->name);

        ordered_nodes.push_back(node);
    }

    return std::make_shared<PipelineRuntime>(std::move(ordered_nodes));
}

void PipelineBuilder::validate() const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    validate_locked(*state_);
}
} // namespace takt