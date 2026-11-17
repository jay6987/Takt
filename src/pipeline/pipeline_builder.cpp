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
PipelineBuilder PipelineBuilder::create(std::string name)
{
    return PipelineBuilder(std::make_shared<State>(std::move(name)));
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
PipelineBuilder&
PipelineBuilder::operator=(PipelineBuilder&&) noexcept = default;
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
    ensure_port_locked(node.input_ports, port_name, PortDirection::Input,
                       descriptor);
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
    ensure_port_locked(node.output_ports, port_name, PortDirection::Output,
                       descriptor);
    return *this;
}

} // namespace takt
