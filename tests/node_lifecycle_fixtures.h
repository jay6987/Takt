// Supplies counting and stoppable nodes for nested runtime lifecycle tests.
#pragma once
#include "node_tests_test_support_common.h"
namespace
{

/// Represents the counting node test fixture.
class CountingNode final : public takt::Node
{
  public:
    // Binds an execution counter used to verify nested startup.
    explicit CountingNode(std::atomic<int>& count)
        : takt::Node("counting-node"),
          count_(count)
    {
    }

  protected:
    /// Represents the worker task test fixture.
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one counter-update task per configured worker.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Counts successful execution of a nested worker task.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        count_.fetch_add(1, std::memory_order_relaxed);
    }

  private:
    std::atomic<int>& count_;
};

/// Represents the stoppable node test fixture.
class StoppableNode final : public takt::Node
{
  public:
    // Binds startup and cancellation observation signals for lifecycle tests.
    StoppableNode(std::promise<void>& started_promise,
                  std::atomic<bool>& observed_stop)
        : takt::Node("stoppable-node"),
          started_promise_(started_promise),
          observed_stop_(observed_stop)
    {
    }

  protected:
    /// Represents the worker task test fixture.
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task that waits for cooperative cancellation.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Signals startup, waits for stop and records observed cancellation.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        started_promise_.set_value();
        while (!stop_requested())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        observed_stop_.store(true, std::memory_order_relaxed);
    }

  private:
    std::promise<void>& started_promise_;
    std::atomic<bool>& observed_stop_;
};
} // namespace
