// Verifies grouped scheduling, bounded admission and ownership through failure.
#include <gtest/gtest.h>
#include <takt/takt.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace
{
class GroupProbe final : public takt::FrameTaskNode
{
  public:
    // Keep counters outside the context so destruction can verify task drain.
    GroupProbe(size_t workers,
               size_t count)
        : FrameTaskNode("groups",
                        workers,
                        2),
          count(count)
    {
        pipe->set_producer(
            {"groups", takt::concurrency(1), takt::frames_per_write(1)}, 0);
        pipe->set_consumer({"sink", takt::concurrency(1),
                            takt::frames_per_read(1), takt::overlap_frames(0)});
        pipe->initialize();
        register_pipe(pipe, true);
    }
    // Derived members must outlive worker and ingress callbacks.
    ~GroupProbe() override
    {
        request_stop();
        join();
    }
    size_t count;
    int fail = -1;
    bool throw_failure_observer = false;
    std::atomic<size_t> acquired{0}, generated{0}, processed{0}, ended{0};
    std::atomic<size_t> live_items{0}, destroyed_frames{0};
    std::thread::id ingress;
    std::function<void(size_t, Frame&, Task&)> work;
    std::function<void(uint64_t)> generating;
    std::shared_ptr<takt::Pipe<int>> pipe =
        std::make_shared<takt::Pipe<int>>("group-output");

  private:
    struct Context final : Frame
    {
        GroupProbe& owner;
        // Keep a reference to test counters across frame ownership transitions.
        explicit Context(GroupProbe& node) : owner(node) {}
        // A frame must never disappear while a queued/running payload remains.
        ~Context() override
        {
            EXPECT_EQ(owner.live_items.load(), 0U);
            ++owner.destroyed_frames;
        }
    };
    struct Item final : Task
    {
        GroupProbe& owner;
        // Track the number of bounded task payloads alive at once.
        explicit Item(GroupProbe& node) : owner(node)
        {
            ++owner.live_items;
        }
        // Destruction is part of drain, not merely a post-processing detail.
        ~Item() override
        {
            --owner.live_items;
        }
    };
    // Fail precisely one lifecycle phase without replacing its exception type.
    void maybe_fail(int phase)
    {
        if (fail == phase)
            throw std::domain_error("original group failure");
    }
    // Allocate before admission so even failed acquisition has an owned
    // context.
    std::unique_ptr<Frame> create_frame() override
    {
        return std::make_unique<Context>(*this);
    }
    // The next frame is not acquired until the prior successful callback/drain.
    bool acquire_frame(Frame& frame) override
    {
        EXPECT_EQ(frame.index, ended.load());
        EXPECT_EQ(live_items.load(), 0U);
        ingress = std::this_thread::get_id();
        if (frame.index == 2)
            return false;
        ++acquired;
        maybe_fail(0);
        return true;
    }
    // Group start always runs on ingress.
    void on_frame_start(Frame&) override
    {
        EXPECT_EQ(ingress, std::this_thread::get_id());
        maybe_fail(1);
    }
    // Seal explicitly; an empty queue before this point is not completion.
    std::unique_ptr<Task> build_frame_task(Frame&,
                                           uint64_t index) override
    {
        if (generating)
            generating(index);
        if (index == 3)
            maybe_fail(2);
        if (index == count)
            return nullptr;
        ++generated;
        return std::make_unique<Item>(*this);
    }
    // Exercise permanent workers rather than spawning nested threads.
    void process_frame_task(size_t worker,
                            Frame& frame,
                            Task& item) override
    {
        EXPECT_NE(ingress, std::this_thread::get_id());
        maybe_fail(3);
        if (work)
            work(worker, frame, item);
        ++processed;
    }
    // Only successful, sealed and fully drained groups reach completion.
    void on_frame_complete(Frame&) override
    {
        EXPECT_EQ(live_items.load(), 0U);
        EXPECT_EQ(ingress, std::this_thread::get_id());
        maybe_fail(4);
        ++ended;
    }
    // Secondary error observers must not prevent fault closure or task drain.
    void on_frame_failure(std::exception_ptr) override
    {
        if (throw_failure_observer)
            throw std::runtime_error("secondary observer failure");
    }
};

// Empty groups, fewer tasks than workers and uneven groups all seal correctly.
TEST(FrameTaskNode,
     EmptySmallAndUnevenGroupsCompleteAfterPayloadDestruction)
{
    for (const size_t workers : {1U, 3U})
        for (const size_t count : {0U, 1U, 7U})
        {
            GroupProbe node(workers, count);
            node.start();
            node.join();
            EXPECT_EQ(node.failure(), nullptr);
            EXPECT_EQ(node.completed_frames(), 2U);
            EXPECT_EQ(node.processed.load(), count * 2);
            EXPECT_EQ(node.ended.load(), 2U);
            EXPECT_EQ(node.destroyed_frames.load(), 3U);
            EXPECT_TRUE(node.pipe->closed());
            EXPECT_FALSE(node.pipe->faulted());
        }
}

// Inject every hook failure and verify original error, pipe fault and no
// advance.
TEST(FrameTaskNode,
     EveryFailurePhaseDrainsAndDoesNotAdmitAnotherFrame)
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
        EXPECT_EQ(node.completed_frames(), 0U);
        EXPECT_EQ(node.ended.load(), 0U);
        EXPECT_EQ(node.live_items.load(), 0U);
        EXPECT_EQ(node.destroyed_frames.load(), 1U);
        EXPECT_TRUE(node.pipe->faulted());
    }
}

// Hold every worker to measure the queue bound, then stop with work in flight.
TEST(FrameTaskNode,
     PermanentWorkersOverlapAndCancellationDrainsRetainedFrame)
{
    for (const size_t count : {1U, 100000U})
    {
        GroupProbe node(3, count);
        std::mutex mutex;
        std::condition_variable changed;
        std::array<bool, 3> entered{};
        bool release = false;
        node.work = [&](size_t worker, auto&, auto&)
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
            EXPECT_LE(node.generated.load(), 5U);
            EXPECT_LE(node.live_items.load(), 5U);
            EXPECT_EQ(node.destroyed_frames.load(), 0U);
            node.request_stop();
            release = true;
        }
        changed.notify_all();
        node.join();
        EXPECT_EQ(node.acquired.load(), 1U);
        EXPECT_EQ(node.completed_frames(), 0U);
        EXPECT_EQ(node.destroyed_frames.load(), 1U);
        EXPECT_EQ(node.live_items.load(), 0U);
    }
}

// Prove that a temporarily drained queue cannot complete an unsealed group.
TEST(FrameTaskNode,
     EarlyTaskCompletionWaitsForGenerationToSeal)
{
    GroupProbe node(1, 2);
    std::mutex mutex;
    std::condition_variable changed;
    size_t finished = 0;
    node.work = [&](size_t, auto&, auto&)
    {
        std::lock_guard<std::mutex> lock(mutex);
        ++finished;
        changed.notify_all();
    };
    node.generating = [&](uint64_t index)
    {
        if (index != 1)
            return;
        std::unique_lock<std::mutex> lock(mutex);
        const auto expected = node.completed_frames() * 2 + 1;
        if (!changed.wait_for(lock, std::chrono::seconds(5),
                              [&] { return finished >= expected; }))
            throw std::runtime_error("early task did not finish");
        EXPECT_EQ(node.ended.load(), node.completed_frames());
    };
    node.start();
    node.join();
    EXPECT_EQ(node.failure(), nullptr);
    EXPECT_EQ(node.completed_frames(), 2U);
    EXPECT_EQ(node.processed.load(), 4U);
}
} // namespace
