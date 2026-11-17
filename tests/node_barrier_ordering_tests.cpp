// Verifies barriers wait for prior payload destruction before admitting work.
#include "node_barrier_tests_test_support.h"

namespace
{
// Holds the first task and observes attempts to generate a later group.
void install_first_task_gate(GroupProbe& node,
                             bool& first_task_entered,
                             bool& release_first_task,
                             std::atomic<bool>& next_group_generated)
{
    node.work = [&](size_t, auto& task)
    {
        const auto local_index = task.sequence_id % 2;
        if (node.ended.load() == 0 && local_index == 0)
        {
            std::unique_lock<std::mutex> lock(node.task_mutex);
            first_task_entered = true;
            node.task_changed.notify_all();
            if (!node.task_changed.wait_for(lock, std::chrono::seconds(5),
                                            [&] { return release_first_task; }))
                throw std::runtime_error("first task release timeout");
        }
    };
    node.generating = [&](uint64_t index)
    {
        (void)index;
        if (node.ended.load() != 0)
            next_group_generated = true;
    };
}

// Confirms a live earlier payload blocks the barrier and next group.
void verify_first_group_is_blocked(
    GroupProbe& node,
    const bool& first_task_entered,
    bool& release_first_task,
    const std::atomic<bool>& next_group_generated)
{
    std::unique_lock<std::mutex> lock(node.task_mutex);
    const bool ready = node.task_changed.wait_for(
        lock, std::chrono::seconds(5),
        [&]
        {
            return first_task_entered &&
                   node.barrier_items_destroyed.load() == 1U &&
                   node.processed.load() == 1U;
        });
    EXPECT_TRUE(ready);
    EXPECT_EQ(node.ended.load(), 0U);
    EXPECT_EQ(node.acquired.load(), 1U);
    EXPECT_EQ(node.generated.load(), 2U);
    EXPECT_EQ(node.live_items.load(), 1U);
    EXPECT_FALSE(next_group_generated.load());
    release_first_task = true;
}

// Confirms releasing the gated task lets both groups drain completely.
void verify_barrier_completes_after_release(
    const GroupProbe& node,
    const std::atomic<bool>& next_group_generated)
{
    EXPECT_EQ(node.failure(), nullptr);
    EXPECT_EQ(node.ended.load(), 2U);
    EXPECT_EQ(node.destroyed_frames.load(), 2U);
    EXPECT_TRUE(next_group_generated.load());
    EXPECT_EQ(node.processed.load(), 4U);
}
} // namespace

// A fast barrier cannot complete while an earlier task payload remains alive.
TEST(NodeBarrier,
     BarrierWaitsForEarlierTaskDestructionBeforeCallbackAndNextBuild)
{
    GroupProbe node(2, 2);
    bool first_task_entered = false;
    bool release_first_task = false;
    std::atomic<bool> next_group_generated{false};
    install_first_task_gate(node, first_task_entered, release_first_task,
                            next_group_generated);
    node.start();

    verify_first_group_is_blocked(node, first_task_entered, release_first_task,
                                  next_group_generated);
    node.task_changed.notify_all();
    node.join();
    verify_barrier_completes_after_release(node, next_group_generated);
}
