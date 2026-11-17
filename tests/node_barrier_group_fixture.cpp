// Implements grouped barrier fixture setup and payload lifetime tracking.
#include "node_barrier_tests_test_support.h"

#include <gtest/gtest.h>

namespace takt_test
{
// Configures bounded group processing and its observable output pipe.
GroupProbe::GroupProbe(size_t workers,
                       size_t task_count)
    : Node("groups",
           workers),
      count(task_count)
{
    pipe->set_producer(
        {"groups", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    pipe->set_consumer({"sink", takt::concurrency(1), takt::frames_per_read(1),
                        takt::overlap_frames(0)});
    pipe->initialize();
}

// Stops and joins execution before fixture state is destroyed.
GroupProbe::~GroupProbe()
{
    request_stop();
    join();
}

// Associates one retained group with its lifetime counters.
GroupProbe::Context::Context(GroupProbe& node) : owner(node) {}

// Confirms task destruction precedes group storage release.
GroupProbe::Context::~Context()
{
    EXPECT_EQ(owner.live_items.load(), 0U);
    ++owner.destroyed_frames;
}

// Admits a tracked payload with an optional completion barrier.
GroupProbe::Item::Item(GroupProbe& node,
                       Context& group,
                       size_t task_index,
                       bool is_barrier)
    : owner(node),
      context(&group),
      local_index(task_index)
{
    barrier = is_barrier;
    ++owner.live_items;
}

// Records payload cleanup and wakes observers waiting on destruction.
GroupProbe::Item::~Item()
{
    --owner.live_items;
    if (local_index == 1)
    {
        ++owner.barrier_items_destroyed;
    }
    owner.task_changed.notify_all();
}

// Cancels node admission and releases blocked output operations.
void GroupProbe::request_stop() noexcept
{
    Node::request_stop();
    if (!pipe || pipe->closed() || pipe->faulted())
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
