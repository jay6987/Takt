// Attaches optional payload metadata to a pipe for diagnostics and DOT output.
#pragma once

#include <any>
#include <string>
#include <unordered_map>

namespace takt
{
using PipeDescriptor = std::unordered_map<std::string, std::any>;

// Returns a metadata value only when the key exists with the requested type.
template <typename T>
const T* find_descriptor_value(const PipeDescriptor& descriptor,
                               const std::string& key)
{
    auto it = descriptor.find(key);
    if (it == descriptor.end())
    {
        return nullptr;
    }

    return std::any_cast<T>(&(it->second));
}
} // namespace takt