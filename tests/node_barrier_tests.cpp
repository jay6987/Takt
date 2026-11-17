// Verifies grouped barrier completion, cancellation, and failure draining.
#include "node_barrier_tests_test_support.h"

// Verifies all group sizes complete only after their payloads are destroyed.
TEST(NodeBarrier,
     EmptySmallAndUnevenGroupsCompleteAfterPayloadDestruction)
{
    for (const size_t workers : {1U, 3U})
        for (const size_t count : {0U, 1U, 7U})
        {
            GroupProbe node(workers, count);
            node.start();
            node.join();
            EXPECT_EQ(node.failure(), nullptr);
            EXPECT_EQ(node.processed.load(), count * 2);
            EXPECT_EQ(node.ended.load(), 2U);
            EXPECT_EQ(node.destroyed_frames.load(), 2U);
            EXPECT_TRUE(node.pipe->closed());
            EXPECT_FALSE(node.pipe->faulted());
        }
}

// Inject every execution failure and verify drain and original error retention.
TEST(NodeBarrier,
     EveryFailurePhaseDrainsAndDoesNotAdmitAnotherGroup)
{
    for (int phase = 0; phase < 5; ++phase)
    {
        GroupProbe node(3, 10);
        node.fail = phase;
        node.throw_failure_observer = true;
        node.start();
        node.join();
        ASSERT_NE(node.failure(), nullptr);
        try
        {
            std::rethrow_exception(node.failure());
        }
        catch (const std::domain_error& error)
        {
            EXPECT_STREQ(error.what(), "original group failure");
        }
        EXPECT_EQ(node.acquired.load(), 1U);
        EXPECT_EQ(node.ended.load(), 0U);
        EXPECT_EQ(node.live_items.load(), 0U);
        EXPECT_EQ(node.destroyed_frames.load(), 1U);
        EXPECT_TRUE(node.pipe->faulted());
    }
}

// Hold each reserved worker to measure in-flight bounds and cancellation drain.
TEST(NodeBarrier,
     PermanentWorkersOverlapAndCancellationDrainsRetainedContext)
{
    for (const size_t count : {1U, 100000U})
    {
        GroupProbe node(3, count);
        std::mutex mutex;
        std::condition_variable changed;
        std::array<bool, 3> entered{};
        bool release = false;
        node.work = [&](size_t worker, auto&)
        {
            std::unique_lock<std::mutex> lock(mutex);
            entered[worker] = true;
            changed.notify_all();
            if (!changed.wait_for(lock, std::chrono::seconds(5),
                                  [&] { return release; }))
                throw std::runtime_error("worker overlap timeout");
        };
        node.start();
        {
            std::unique_lock<std::mutex> lock(mutex);
            EXPECT_TRUE(changed.wait_for(
                lock, std::chrono::seconds(5),
                [&]
                {
                    return count == 1 ? entered[0] || entered[1] || entered[2]
                                      : entered[0] && entered[1] && entered[2];
                }));
            EXPECT_LE(node.generated.load(), node.worker_count());
            EXPECT_LE(node.live_items.load(), node.worker_count());
            EXPECT_EQ(node.destroyed_frames.load(), 0U);
            node.request_stop();
            release = true;
        }
        changed.notify_all();
        node.join();
        EXPECT_EQ(node.acquired.load(), 1U);
        EXPECT_EQ(node.destroyed_frames.load(), 1U);
        EXPECT_EQ(node.live_items.load(), 0U);
    }
}
