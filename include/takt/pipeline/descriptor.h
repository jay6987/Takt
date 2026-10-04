#pragma once

#include <any>
#include <string>
#include <unordered_map>

namespace takt
{
using PipeDescriptor = std::unordered_map<std::string, std::any>;

template <typename T>
const T* find_descriptor_value(const PipeDescriptor& descriptor, const std::string& key)
{
    auto it = descriptor.find(key);
    if (it == descriptor.end())
    {
        return nullptr;
    }

    return std::any_cast<T>(&(it->second));
}
} // namespace takt