#pragma once

#include <string>
#include <utility>
#include <vector>

#include "takt/pipeline/descriptor.h"

namespace takt
{
enum class PortDirection
{
    Input,
    Output,
};

struct PortSpec
{
    std::string name;
    PortDirection direction = PortDirection::Input;
    PipeDescriptor descriptor;
};

struct NodeSpec
{
    std::string name;
    PipeDescriptor descriptor;
    std::vector<PortSpec> input_ports;
    std::vector<PortSpec> output_ports;
};

struct EdgeSpec
{
    std::string name;
    std::string from_node;
    std::string from_port;
    std::string to_node;
    std::string to_port;
    PipeDescriptor descriptor;
};

struct NodePortRef
{
    std::string node_name;
    std::string port_name;
};
} // namespace takt