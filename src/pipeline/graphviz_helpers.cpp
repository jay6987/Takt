// Implements DOT graph rendering helpers.
#include "graphviz_detail.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>

namespace takt::dot_detail
{
// Escapes DOT quoted strings and rejects unsupported control characters.
std::string quoted(const std::string& value)
{
    std::string result = "\"";
    for (const char character : value)
    {
        switch (character)
        {
        case '"':
            result += "\\\"";
            break;
        case '\\':
            result += "\\\\";
            break;
        case '\n':
            result += "\\n";
            break;
        case '\r':
            result += "\\r";
            break;
        case '\t':
            result += "\\t";
            break;
        default:
            if (static_cast<unsigned char>(character) < 32)
            {
                throw std::invalid_argument(
                    "DOT labels contain unsupported control characters");
            }
            result += character;
        }
    }
    return result + '"';
}

// Forms stable internal DOT identifiers without relying on user labels.
std::string node_id(const std::string& prefix,
                    size_t index)
{
    return prefix + "n" + std::to_string(index);
}

// Returns recorded mapping transport or explicit unresolved metadata.
MappingTransportInfo transport_info(const PipelineGraph& inner,
                                    const std::string& port,
                                    bool input)
{
    const auto& transports =
        input ? inner.input_transports : inner.output_transports;
    const auto found = transports.find(port);
    return found == transports.end() ? MappingTransportInfo{} : found->second;
}

// Forms a stable DOT identifier for a declared subgraph boundary port.
std::string boundary_id(const NodeSpec& node,
                        const std::string& id,
                        const std::string& port,
                        bool input)
{
    const auto& ports = input ? node.input_ports : node.output_ports;
    const auto found =
        std::find_if(ports.begin(), ports.end(), [&](const PortSpec& candidate)
                     { return candidate.name == port; });
    return id + (input ? "_in" : "_out") +
           std::to_string(found - ports.begin());
}

// Emits an escaped labeled edge, optionally styled as a logical mapping.
void write_edge(std::ostream& output,
                const std::string& source,
                const std::string& target,
                const std::string& label,
                bool mapping)
{
    output << "  " << source << " -> " << target << " [label=" << quoted(label);
    if (mapping)
    {
        output << ",style=dashed";
    }
    output << "];\n";
}

} // namespace takt::dot_detail
