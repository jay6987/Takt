#pragma once

#include <cstddef>
#include <string>

namespace takt
{
enum class BufferPolicy
{
    Minimal,
    MaxConcurrency,
};

struct ProducerConfig
{
    std::string name;
    size_t concurrency = 1;
    size_t write_size = 1;
};

struct ConsumerConfig
{
    std::string name;
    size_t concurrency = 1;
    size_t read_size = 1;
    size_t overlap_size = 0;
};
} // namespace takt
