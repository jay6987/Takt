// Verifies task barriers, bounded admission and ownership through failure.
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
#include <vector>

namespace
{
class GroupProbe final : public takt::Node
{
  public:
    // Keep counters outside the ingress-owned context to observe its cleanup.
    GroupProbe(size_t workers,
               size_t count)
        : Node("groups",
               workers),
          count(count)
    {
        pipe->set_producer(
            {"groups", takt::concurrency(1), takt::frames_per_write(1)}, 0);
        pipe->set_consumer({"sink", takt::concurrency(1),
                            takt::frames_per_read(1), takt::overlap_frames(0)});
        pipe->initialize();
    }
    // Stop and join callbacks before derived state is released.
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
    std::atomic<size_t> barrier_items_destroyed{0};
    std::thread::id ingress;
    std::function<void(size_t, Task&)> work;
    std::function<void(uint64_t)> generating;
    std::mutex task_mutex;
    std::condition_variable task_changed;
    std::shared_ptr<takt::Pipe<int>> pipe =
        std::make_shared<takt::Pipe<int>>("group-output");

  protected:
    struct Context final
    {
        GroupProbe& owner;
        // Keep a reference to test counters across group ownership transitions.
        explicit Context(GroupProbe& node) : owner(node) {}
        // A group must outlive every queued or running task payload.
        ~Context()
        {
            EXPECT_EQ(owner.live_items.load(), 0U);
            ++owner.destroyed_frames;
        }
    };
    struct Item final : Task
    {
        GroupProbe& owner;
        Context* context;
        size_t local_index;

        // Track the number of bounded task payloads alive at once.
        Item(GroupProbe& node,
             Context& group,
             size_t task_index,
             bool barrier)
            : owner(node),
              context(&group),
              local_index(task_index)
        {
            this->barrier = barrier;
            ++owner.live_items;
        }
        // Destruction is part of drain, not merely a post-processing detail.
        ~Item() override
        {
            --owner.live_items;
            if (local_index == 1)
                ++owner.barrier_items_destroyed;
            owner.task_changed.notify_all();
        }
    };

  private:
    // Fail precisely one lifecycle phase without replacing its exception type.
    void maybe_fail(int phase)
    {
        if (fail == phase)
            throw std::domain_error("original group failure");
    }

    // Starts a lazily generated group while retaining it on ingress.
    bool start_group()
    {
        if (ended.load() == 2)
            return false;
        EXPECT_EQ(live_items.load(), 0U);
        ingress = std::this_thread::get_id();
        context = std::make_unique<Context>(*this);
        next_item = 0;
        ++acquired;
        maybe_fail(0);
        maybe_fail(1);
        return true;
    }

    // Build at most one task for each worker reservation, then mark the end.
    std::unique_ptr<Task> build_task(uint64_t sequence_id) override
    {
        (void)sequence_id;
        if (!context && !start_group())
            return nullptr;
        if (generating)
            generating(next_item);
        if (next_item == 3)
            maybe_fail(2);
        const bool empty = count == 0;
        const bool last = empty || next_item + 1 == count;
        auto item = std::make_unique<Item>(*this, *context, next_item, last);
        if (!empty)
        {
            ++next_item;
            ++generated;
        }
        return item;
    }

    // Run group work on permanent workers without waiting for peer workers.
    void process_task(size_t worker,
                      Task& task) override
    {
        EXPECT_NE(ingress, std::this_thread::get_id());
        auto& item = static_cast<Item&>(task);
        EXPECT_EQ(item.context, context.get());
        if (count != 0)
        {
            maybe_fail(3);
            if (work)
                work(worker, task);
            ++processed;
        }
    }

    // Completion runs on ingress after the barrier payload has been destroyed.
    void on_barrier_complete(uint64_t sequence) override
    {
        (void)sequence;
        EXPECT_EQ(live_items.load(), 0U);
        EXPECT_EQ(ingress, std::this_thread::get_id());
        maybe_fail(4);
        ++ended;
        context.reset();
    }

    // Release interrupted group state after all tasks have been drained.
    void on_ingress_manager_done() override
    {
        context.reset();
        if (!pipe->closed() && !pipe->faulted())
            pipe->close();
    }

    // Preserve failure closure even when the optional observer itself throws.
    void on_exception(const takt::NodeExceptionContext& exception) override
    {
        if (!pipe->faulted())
            pipe->close_with_error(exception.message);
        Node::on_exception(exception);
        if (throw_failure_observer)
            throw std::runtime_error("secondary observer failure");
    }

  public:
    // Cancellation closes owned pipes to interrupt external acquisitions.
    void request_stop() noexcept override
    {
        Node::request_stop();
        if (pipe && !pipe->closed() && !pipe->faulted())
        {
            try
            {
                pipe->close();
            }
            catch (...)
            {
            }
        }
    }

  private:
    std::unique_ptr<Context> context;
    size_t next_item = 0;
};

class VectorBarrierProbe final : public takt::Node
{
  public:
    // Keep both token-owning connections alive for the complete node lifetime.
    VectorBarrierProbe(
        std::shared_ptr<takt::Pipe<std::vector<float>>> input_pipe,
        std::shared_ptr<takt::Pipe<std::vector<float>>> output_pipe)
        : Node("vector-barrier",
               2),
          input(std::move(input_pipe)),
          output(std::move(output_pipe))
    {
    }

    // Wake pipe waits and join callbacks before releasing node state.
    ~VectorBarrierProbe() override
    {
        request_stop();
        join();
    }

    std::shared_ptr<takt::Pipe<std::vector<float>>> input;
    std::shared_ptr<takt::Pipe<std::vector<float>>> output;
    std::atomic<size_t> acquired{0};
    std::atomic<size_t> published{0};
    std::atomic<size_t> released{0};
    std::atomic<size_t> live_regions{0};
    std::atomic<bool> ordering_valid{true};
    std::mutex gate_mutex;
    std::condition_variable gate_changed;
    std::array<std::array<bool, 2>, 2> entered{};
    std::array<bool, 2> release_group{};

  protected:
    struct Context final
    {
        VectorBarrierProbe& owner;
        size_t group;
        takt::Pipe<std::vector<float>>::ReadToken read;
        takt::Pipe<std::vector<float>>::WriteToken write;

        // Record that the context outlived all region payloads.
        Context(VectorBarrierProbe& node,
                size_t group_index)
            : owner(node),
              group(group_index)
        {
        }

        // Verify all regions are destroyed before their frame context.
        ~Context()
        {
            EXPECT_EQ(owner.live_regions.load(), 0U);
            write = takt::Pipe<std::vector<float>>::WriteToken{};
            read = takt::Pipe<std::vector<float>>::ReadToken{};
            ++owner.released;
        }
    };

    struct Region final : Task
    {
        VectorBarrierProbe& owner;
        Context* context;
        size_t first;
        size_t last;

        // Count each live task payload for the context lifetime assertion.
        Region(VectorBarrierProbe& node,
               Context& frame,
               size_t begin,
               size_t end,
               bool barrier)
            : owner(node),
              context(&frame),
              first(begin),
              last(end)
        {
            this->barrier = barrier;
            ++owner.live_regions;
        }

        // Release this task's accounting before any barrier callback runs.
        ~Region() override
        {
            --owner.live_regions;
        }
    };

  private:
    // Acquire one pair of tokens and split its vector across permanent workers.
    std::unique_ptr<Task> build_task(uint64_t) override
    {
        if (!context)
        {
            if (acquired.load() != published.load() ||
                acquired.load() != released.load())
                ordering_valid = false;

            try
            {
                auto read = input->acquire_read();
                context = std::make_unique<Context>(*this, acquired.load());
                context->read = std::move(read);
                context->write = output->acquire_shot_end_write(1);
            }
            catch (const takt::PipeClosedAndEmptySignal&)
            {
                context.reset();
                output->close();
                return nullptr;
            }
            ++acquired;
            next_region = 0;
        }

        const auto size = context->read.value(0).size();
        const auto first = size * next_region / worker_count();
        const auto last = size * (next_region + 1) / worker_count();
        const bool barrier = next_region + 1 == worker_count();
        auto task =
            std::make_unique<Region>(*this, *context, first, last, barrier);
        ++next_region;
        return task;
    }

    // Hold each region at a test-controlled gate without worker-to-worker
    // waits.
    void process_task(size_t worker,
                      Task& base) override
    {
        auto& task = static_cast<Region&>(base);
        const auto group = task.context->group;
        {
            std::unique_lock<std::mutex> lock(gate_mutex);
            entered[group][worker] = true;
            gate_changed.notify_all();
            if (!gate_changed.wait_for(lock, std::chrono::seconds(5),
                                       [&] { return release_group[group]; }))
                throw std::runtime_error("region release timeout");
        }

        const auto& source = task.context->read.value(0);
        auto& destination = task.context->write.value(0);
        for (size_t i = task.first; i < task.last; ++i)
            destination[i] = source[i] + 100.0F * group;
    }

    // Publish and release the context only after every region is destroyed.
    void on_barrier_complete(uint64_t) override
    {
        EXPECT_EQ(live_regions.load(), 0U);
        context->write.publish();
        ++published;
        context.reset();
    }

    // Release retained tokens after failure or cancellation drains all tasks.
    void on_ingress_manager_done() override
    {
        context.reset();
    }

    // Fault both token-owning pipes while preserving the original exception.
    void on_exception(const takt::NodeExceptionContext& exception) override
    {
        input->close_with_error(exception.message);
        output->close_with_error(exception.message);
        Node::on_exception(exception);
    }

  public:
    // Close owned pipes so cancellation interrupts blocked token acquisition.
    void request_stop() noexcept override
    {
        Node::request_stop();
        close_pipe(input);
        close_pipe(output);
    }

  private:
    // Stop cleanup must not replace an execution failure.
    static void close_pipe(
        const std::shared_ptr<takt::Pipe<std::vector<float>>>& pipe) noexcept
    {
        try
        {
            pipe->close();
        }
        catch (...)
        {
        }
    }

    std::unique_ptr<Context> context;
    size_t next_region = 0;
};

// Empty groups and uneven task counts complete only at their final barrier.
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

// A fast barrier cannot complete while an earlier task payload remains alive.
TEST(NodeBarrier,
     BarrierWaitsForEarlierTaskDestructionBeforeCallbackAndNextBuild)
{
    GroupProbe node(2, 2);
    bool first_task_entered = false;
    bool release_first_task = false;
    std::atomic<bool> next_group_generated{false};
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
    node.start();

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
    node.task_changed.notify_all();
    node.join();
    EXPECT_EQ(node.failure(), nullptr);
    EXPECT_EQ(node.ended.load(), 2U);
    EXPECT_EQ(node.destroyed_frames.load(), 2U);
    EXPECT_TRUE(next_group_generated.load());
    EXPECT_EQ(node.processed.load(), 4U);
}

// Keep vector tokens on ingress and publish only after parallel regions drain.
TEST(NodeBarrier,
     VectorRegionsShareTokensAndGateTheNextFrameOnBarrierCleanup)
{
    auto input =
        std::make_shared<takt::Pipe<std::vector<float>>>("vector-input");
    auto output =
        std::make_shared<takt::Pipe<std::vector<float>>>("vector-output");
    input->set_producer(
        {"source", takt::concurrency(1), takt::frames_per_write(1)},
        std::vector<float>(8, 0.0F));
    input->set_consumer({"vector-node", takt::concurrency(1),
                         takt::frames_per_read(1), takt::overlap_frames(0)});
    input->set_slot_capacity(2);
    input->initialize();
    output->set_producer(
        {"vector-node", takt::concurrency(2), takt::frames_per_write(1)},
        std::vector<float>(8, 0.0F));
    output->set_consumer({"sink", takt::concurrency(1),
                          takt::frames_per_read(1), takt::overlap_frames(0)});
    output->set_slot_capacity(2);
    output->initialize();

    for (size_t group = 0; group < 2; ++group)
    {
        auto write = input->acquire_shot_end_write(1);
        auto& values = write.value(0);
        for (size_t i = 0; i < values.size(); ++i)
            values[i] = static_cast<float>(group * 10 + i);
        write.publish();
    }
    input->close();

    VectorBarrierProbe node(input, output);
    node.start();
    for (size_t group = 0; group < 2; ++group)
    {
        std::unique_lock<std::mutex> lock(node.gate_mutex);
        EXPECT_TRUE(node.gate_changed.wait_for(
            lock, std::chrono::seconds(5),
            [&] { return node.entered[group][0] && node.entered[group][1]; }));
        EXPECT_EQ(node.acquired.load(), group + 1);
        EXPECT_EQ(node.published.load(), group);
        EXPECT_EQ(node.released.load(), group);
        node.release_group[group] = true;
        lock.unlock();
        node.gate_changed.notify_all();
    }
    node.join();

    EXPECT_EQ(node.failure(), nullptr);
    EXPECT_TRUE(node.ordering_valid.load());
    EXPECT_EQ(node.acquired.load(), 2U);
    EXPECT_EQ(node.published.load(), 2U);
    EXPECT_EQ(node.released.load(), 2U);
    EXPECT_TRUE(output->closed());
    for (size_t group = 0; group < 2; ++group)
    {
        auto read = output->acquire_read();
        for (size_t i = 0; i < 8; ++i)
            EXPECT_FLOAT_EQ(read.value(0)[i],
                            static_cast<float>(group * 110 + i));
    }
}
} // namespace
