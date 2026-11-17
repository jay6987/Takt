// Tests explicit pipe configuration, initialization and cancellation rules.
#pragma once
#include "takt/takt.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <stdexcept>
#include <type_traits>

namespace
{
/// Represents the frame probe test fixture.
struct FrameProbe
{
    static inline int live = 0;
    static inline int copies_until_failure = -1;
    int value = 17;

    // Counts construction so allocation timing can be observed.
    FrameProbe()
    {
        ++live;
    }
    // Moves the probe value while tracking another live instance.
    FrameProbe(FrameProbe&& other) noexcept : value(other.value)
    {
        ++live;
    }
    // Counts copies and injects the configured allocation-time failure.
    FrameProbe(const FrameProbe& other) : value(other.value)
    {
        if (copies_until_failure == 0)
            throw std::runtime_error("frame copy failed");
        if (copies_until_failure > 0)
            --copies_until_failure;
        ++live;
    }
    // Copies the value without changing live-instance accounting.
    FrameProbe& operator=(const FrameProbe&) = default;
    // Decrements the live-instance count on destruction.
    ~FrameProbe()
    {
        --live;
    }
};
} // namespace
