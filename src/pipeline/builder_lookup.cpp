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
// Finds a named node or appends an empty declaration while state is locked.
NodeSpec& ensure_node_locked(std::vector<NodeSpec>& nodes,
                             const std::string& name)
{
    auto it = std::find_if(nodes.begin(), nodes.end(), [&](const NodeSpec& node)
                           { return node.name == name; });
    if (it == nodes.end())
    {
        NodeSpec spec;
        spec.name = name;
        nodes.push_back(std::move(spec));
        return nodes.back();
    }

    return *it;
}

// Looks up node metadata without acquiring the already-held state mutex.
const NodeSpec* find_node_locked(const std::vector<NodeSpec>& nodes,
                                 const std::string& name)
{
    auto it = std::find_if(nodes.begin(), nodes.end(), [&](const NodeSpec& node)
                           { return node.name == name; });
    return it == nodes.end() ? nullptr : &*it;
}

// Creates or augments port metadata while preserving its declared direction.
PortSpec& ensure_port_locked(std::vector<PortSpec>& ports,
                             const std::string& port_name,
                             PortDirection direction,
                             const PipeDescriptor& descriptor)
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

// Looks up a port in state protected by the caller's lock.
const PortSpec* find_port_locked(const std::vector<PortSpec>& ports,
                                 const std::string& port_name)
{
    auto it = std::find_if(ports.begin(), ports.end(), [&](const PortSpec& port)
                           { return port.name == port_name; });
    return it == ports.end() ? nullptr : &*it;
}

} // namespace takt::builder_detail
