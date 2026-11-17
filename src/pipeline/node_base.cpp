// Maintains node identity and the diagnostic labels bound before execution.
#include "takt/pipeline/node_base.h"

#include <utility>

namespace takt
{
NodeBase::NodeBase(std::string node_name) : name_(std::move(node_name)) {}

NodeBase::~NodeBase() = default;

const std::string& NodeBase::name() const noexcept
{
    return name_;
}

void NodeBase::set_stage_name(std::string stage_name)
{
    stage_name_ = std::move(stage_name);
}

void NodeBase::set_pipeline_name(std::string pipeline_name)
{
    pipeline_name_ = std::move(pipeline_name);
}

void NodeBase::add_input_port_name(std::string input_port_name)
{
    if (input_port_name.empty())
    {
        return;
    }
    input_port_names_.push_back(std::move(input_port_name));
}

void NodeBase::add_output_port_name(std::string output_port_name)
{
    if (output_port_name.empty())
    {
        return;
    }
    output_port_names_.push_back(std::move(output_port_name));
}

void NodeBase::clear_input_port_names() noexcept
{
    input_port_names_.clear();
}

void NodeBase::clear_output_port_names() noexcept
{
    output_port_names_.clear();
}

const std::string& NodeBase::stage_name() const noexcept
{
    return stage_name_;
}

const std::string& NodeBase::pipeline_name() const noexcept
{
    return pipeline_name_;
}

const std::vector<std::string>& NodeBase::input_port_names() const noexcept
{
    return input_port_names_;
}

const std::vector<std::string>& NodeBase::output_port_names() const noexcept
{
    return output_port_names_;
}
} // namespace takt
