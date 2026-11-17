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
// Collects unique declared and connected input labels for exception context.
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

// Collects unique declared and connected output labels for exception context.
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

} // namespace takt::builder_detail
