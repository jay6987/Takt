// Describes ports and the transport selected for subgraph boundary mappings.
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

/// Named input or output port and its descriptive metadata.
struct PortSpec
{
    std::string name;
    PortDirection direction = PortDirection::Input;
    PipeDescriptor descriptor;
};

/// Declared graph node with its metadata and boundary ports.
struct NodeSpec
{
    std::string name;
    PipeDescriptor descriptor;
    std::vector<PortSpec> input_ports;
    std::vector<PortSpec> output_ports;
};

/// Directed graph connection between two named ports.
struct EdgeSpec
{
    std::string name;
    std::string from_node;
    std::string from_port;
    std::string to_node;
    std::string to_port;
    PipeDescriptor descriptor;
};

/// Identifies a port on a named graph node.
struct NodePortRef
{
    std::string node_name;
    std::string port_name;
};

enum class MappingTransport
{
    Unresolved,
    Direct,
    Bridge,
};

/// Describes how a subgraph boundary mapping transfers frames.
struct MappingTransportInfo
{
    MappingTransport mode = MappingTransport::Unresolved;
    std::string source_pipe;
    std::string target_pipe;
};
} // namespace takt