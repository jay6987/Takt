// Provides explicit, strongly typed producer and consumer configuration.
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
    // Wraps an explicit endpoint concurrency count without implicit conversion.
    explicit constexpr Concurrency(size_t value) noexcept : value(value) {}
    size_t value;
};

struct FramesPerWrite
{
    // Wraps an explicit producer batch size.
    explicit constexpr FramesPerWrite(size_t value) noexcept : value(value) {}
    size_t value;
};

struct FramesPerRead
{
    // Wraps an explicit consumer window size.
    explicit constexpr FramesPerRead(size_t value) noexcept : value(value) {}
    size_t value;
};

struct OverlapFrames
{
    // Wraps the count retained between consumer windows.
    explicit constexpr OverlapFrames(size_t value) noexcept : value(value) {}
    size_t value;
};

// Creates a strongly typed endpoint concurrency argument.
constexpr Concurrency concurrency(size_t value) noexcept
{
    return Concurrency(value);
}

// Creates a strongly typed producer batch-size argument.
constexpr FramesPerWrite frames_per_write(size_t value) noexcept
{
    return FramesPerWrite(value);
}

// Creates a strongly typed consumer window-size argument.
constexpr FramesPerRead frames_per_read(size_t value) noexcept
{
    return FramesPerRead(value);
}

// Creates a strongly typed retained-overlap argument.
constexpr OverlapFrames overlap_frames(size_t value) noexcept
{
    return OverlapFrames(value);
}

struct ProducerConfig
{
    // Uses one producer and one frame per write by default.
    ProducerConfig() = default;
    // Names a producer with explicit, noninterchangeable batching settings.
    ProducerConfig(std::string name,
                   Concurrency concurrency = takt::concurrency(1),
                   FramesPerWrite frames = takt::frames_per_write(1))
        : name(std::move(name)),
          concurrency(concurrency.value),
          frames_per_write(frames.value)
    {
    }

    std::string name;
    size_t concurrency = 1;
    size_t frames_per_write = 1;
};

struct ConsumerConfig
{
    // Uses one consumer and one frame per read without overlap by default.
    ConsumerConfig() = default;
    // Names a consumer with explicit window, overlap and concurrency settings.
    ConsumerConfig(std::string name,
                   Concurrency concurrency = takt::concurrency(1),
                   FramesPerRead frames = takt::frames_per_read(1),
                   OverlapFrames overlap = takt::overlap_frames(0))
        : name(std::move(name)),
          concurrency(concurrency.value),
          frames_per_read(frames.value),
          overlap_frames(overlap.value)
    {
    }

    std::string name;
    size_t concurrency = 1;
    size_t frames_per_read = 1;
    size_t overlap_frames = 0;
};
} // namespace takt
