// Implements pipeline builder topology operations.
#include "builder_detail.h"

#include "takt/pipeline/pipeline_runtime.h"
#include <algorithm>
#include <optional>
#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace takt
{
using namespace builder_detail;
namespace
{
// Collects endpoint pipes while preserving declared port ordering.
void collect_pipes(const std::vector<PortSpec>& ports,
                   std::vector<std::shared_ptr<PipeBase>>& pipes)
{
    for (const auto& port : ports)
    {
        if (port.descriptor.find("pipe_endpoint") == port.descriptor.end())
            continue;
        const auto endpoint = require_pipe_endpoint_from_port(port, "runtime");
        auto pipe = endpoint->lifecycle_pipe();
        if (!pipe)
            throw std::logic_error("runtime endpoint pipe cannot be null");
        pipes.push_back(std::move(pipe));
    }
}

// Applies declared port and stage diagnostics to a bound node.
void configure_node(NodeBase& node,
                    const NodeSpec& spec,
                    const PipelineBuilder::State& state)
{
    node.clear_input_port_names();
    node.clear_output_port_names();
    const auto inputs = collect_input_port_names_locked(spec, state.channels);
    const auto outputs = collect_output_port_names_locked(spec, state.channels);
    for (const auto& port : inputs)
        node.add_input_port_name(port);
    for (const auto& port : outputs)
        node.add_output_port_name(port);
    const auto* stage_name =
        find_descriptor_value<std::string>(spec.descriptor, "stage_name");
    if (stage_name)
        node.set_stage_name(*stage_name);
    node.set_pipeline_name(state.name);
}
} // namespace

std::shared_ptr<PipelineRuntime> PipelineBuilder::build_runtime() const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    validate_locked(*state_);

    std::vector<std::shared_ptr<NodeBase>> ordered_nodes;
    std::vector<std::shared_ptr<PipeBase>> pipes;
    ordered_nodes.reserve(state_->nodes.size());
    for (const auto& node_spec : state_->nodes)
    {
        collect_pipes(node_spec.input_ports, pipes);
        collect_pipes(node_spec.output_ports, pipes);
        auto it = state_->bound_nodes.find(node_spec.name);
        if (it == state_->bound_nodes.end() || !it->second)
        {
            throw std::logic_error(
                "every declared node must be bound before build_runtime");
        }

        configure_node(*it->second, node_spec, *state_);
        ordered_nodes.push_back(it->second);
    }

    return std::make_shared<PipelineRuntime>(std::move(ordered_nodes),
                                             std::move(pipes));
}

} // namespace takt
