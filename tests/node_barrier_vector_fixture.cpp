// Implements vector barrier fixture setup and frame-token lifetime tracking.
#include "node_barrier_tests_test_support.h"

#include <gtest/gtest.h>

#include <utility>

namespace takt_test
{
// Binds shared vector input and output to two parallel region workers.
VectorBarrierProbe::VectorBarrierProbe(
    std::shared_ptr<takt::Pipe<std::vector<float>>> input_pipe,
    std::shared_ptr<takt::Pipe<std::vector<float>>> output_pipe)
    : Node("vector-barrier",
           2),
      input(std::move(input_pipe)),
      output(std::move(output_pipe))
{
}

// Stops and joins before gate state and borrowed pipes are released.
VectorBarrierProbe::~VectorBarrierProbe()
{
    request_stop();
    join();
}

// Tracks one vector frame and the region group that owns its tokens.
VectorBarrierProbe::Context::Context(VectorBarrierProbe& node,
                                     size_t group_index)
    : owner(node),
      group(group_index)
{
}

// Releases frame tokens only after all region payloads are destroyed.
VectorBarrierProbe::Context::~Context()
{
    EXPECT_EQ(owner.live_regions.load(), 0U);
    write = takt::Pipe<std::vector<float>>::WriteToken{};
    read = takt::Pipe<std::vector<float>>::ReadToken{};
    ++owner.released;
}

// Admits one tracked vector interval with its completion barrier flag.
VectorBarrierProbe::Region::Region(VectorBarrierProbe& node,
                                   Context& frame,
                                   size_t begin,
                                   size_t end,
                                   bool is_barrier)
    : owner(node),
      context(&frame),
      first(begin),
      last(end)
{
    barrier = is_barrier;
    ++owner.live_regions;
}

// Records region destruction before frame context cleanup.
VectorBarrierProbe::Region::~Region()
{
    --owner.live_regions;
}

// Closes a fixture pipe without propagating cleanup errors from cancellation.
void VectorBarrierProbe::close_pipe(
    const std::shared_ptr<takt::Pipe<std::vector<float>>>& pipe) noexcept
{
    if (!pipe)
    {
        return;
    }
    try
    {
        pipe->close();
    }
    catch (...)
    {
    }
}
} // namespace takt_test
