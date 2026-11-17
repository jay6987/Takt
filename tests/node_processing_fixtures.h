// Supplies doubling and summing nodes for pipe-driven worker tests.
#pragma once
#include "node_tests_test_support_common.h"
namespace
{
/// Represents the single worker double agent test fixture.
class SingleWorkerDoubleAgent final : public takt::Node
{
  public:
    // Binds pipes to a single persistent doubling worker.
    SingleWorkerDoubleAgent(takt::Pipe<int>& in,
                            takt::Pipe<int>& out)
        : takt::Node("single-worker-double"),
          in_(in),
          out_(out)
    {
    }

  protected:
    /// Represents the worker task test fixture.
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task per configured worker.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Doubles frames until normal input completion closes output.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        while (true)
        {
            try
            {
                auto input = in_.acquire_read();
                auto output = input.is_shot_end()
                                  ? out_.acquire_shot_end_write(1)
                                  : out_.acquire_write();
                output.value() = input.value() * 2;
                output.publish();
            }
            catch (const takt::PipeClosedAndEmptySignal&)
            {
                out_.close();
                break;
            }
        }
    }

  private:
    takt::Pipe<int>& in_;
    takt::Pipe<int>& out_;
};

/// Represents the multi worker sum agent test fixture.
class MultiWorkerSumAgent final : public takt::Node
{
  public:
    // Binds shared input and an atomic sum to the configured worker count.
    MultiWorkerSumAgent(takt::Pipe<int>& in,
                        std::atomic<int>& sum,
                        size_t workers)
        : takt::Node("multi-worker-sum",
                     workers),
          in_(in),
          sum_(sum)
    {
    }

  protected:
    /// Represents the worker task test fixture.
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task for each permanent sum worker.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Atomically accumulates input frames until the pipe is drained.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        while (true)
        {
            try
            {
                auto token = in_.acquire_read();
                sum_.fetch_add(token.value(), std::memory_order_relaxed);
            }
            catch (const takt::PipeClosedAndEmptySignal&)
            {
                break;
            }
        }
    }

  private:
    takt::Pipe<int>& in_;
    std::atomic<int>& sum_;
};
} // namespace
