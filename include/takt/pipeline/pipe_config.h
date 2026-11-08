#pragma once

#include <cstddef>
#include <string>
#include <utility>

namespace takt
{
enum class BufferPolicy
{
    Minimal,
    MaxConcurrency,
};

struct Concurrency
{
    explicit constexpr Concurrency(size_t value) noexcept : value(value) {}
    size_t value;
};

struct FramesPerWrite
{
    explicit constexpr FramesPerWrite(size_t value) noexcept : value(value) {}
    size_t value;
};

struct FramesPerRead
{
    explicit constexpr FramesPerRead(size_t value) noexcept : value(value) {}
    size_t value;
};

struct Overlap
{
    explicit constexpr Overlap(size_t value) noexcept : value(value) {}
    size_t value;
};

constexpr Concurrency concurrency(size_t value) noexcept
{
    return Concurrency(value);
}

constexpr FramesPerWrite frames_per_write(size_t value) noexcept
{
    return FramesPerWrite(value);
}

constexpr FramesPerRead frames_per_read(size_t value) noexcept
{
    return FramesPerRead(value);
}

constexpr Overlap overlap(size_t value) noexcept
{
    return Overlap(value);
}

struct ProducerConfig
{
    ProducerConfig() = default;
    ProducerConfig(std::string name, Concurrency concurrency = takt::concurrency(1),
                   FramesPerWrite frames = frames_per_write(1))
        : name(std::move(name)), concurrency(concurrency.value),
          write_size(frames.value)
    {
    }

    std::string name;
    size_t concurrency = 1;
    size_t write_size = 1;
};

struct ConsumerConfig
{
    ConsumerConfig() = default;
    ConsumerConfig(std::string name, Concurrency concurrency = takt::concurrency(1),
                   FramesPerRead frames = frames_per_read(1),
                   Overlap overlap = takt::overlap(0))
        : name(std::move(name)), concurrency(concurrency.value),
          read_size(frames.value), overlap_size(overlap.value)
    {
    }

    std::string name;
    size_t concurrency = 1;
    size_t read_size = 1;
    size_t overlap_size = 0;
};
} // namespace takt
