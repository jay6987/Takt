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

struct OverlapFrames
{
    explicit constexpr OverlapFrames(size_t value) noexcept : value(value) {}
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

constexpr OverlapFrames overlap_frames(size_t value) noexcept
{
    return OverlapFrames(value);
}

struct ProducerConfig
{
    ProducerConfig() = default;
    ProducerConfig(std::string name, Concurrency concurrency = takt::concurrency(1),
                   FramesPerWrite frames = takt::frames_per_write(1))
        : name(std::move(name)), concurrency(concurrency.value),
          frames_per_write(frames.value)
    {
    }

    std::string name;
    size_t concurrency = 1;
    size_t frames_per_write = 1;
};

struct ConsumerConfig
{
    ConsumerConfig() = default;
    ConsumerConfig(std::string name, Concurrency concurrency = takt::concurrency(1),
                   FramesPerRead frames = takt::frames_per_read(1),
                   OverlapFrames overlap = takt::overlap_frames(0))
        : name(std::move(name)), concurrency(concurrency.value),
          frames_per_read(frames.value), overlap_frames(overlap.value)
    {
    }

    std::string name;
    size_t concurrency = 1;
    size_t frames_per_read = 1;
    size_t overlap_frames = 0;
};
} // namespace takt
