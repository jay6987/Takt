// Supplies inert and doubling nodes for builder runtime binding tests.
#pragma once
#include "pipeline_builder_tests_test_support_common.h"
namespace
{
/// Represents the probe node test fixture.
class ProbeNode final : public takt::Node
{
  public:
    // Binds the counter used to verify actual node execution.
    explicit ProbeNode(std::atomic<int>& run_count)
        : takt::Node("probe"),
          run_count_(run_count)
    {
    }

  protected:
    /// Represents the worker task test fixture.
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent processing task per configured worker.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Records execution of the bound probe node.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        run_count_.fetch_add(1, std::memory_order_relaxed);
    }

  private:
    std::atomic<int>& run_count_;
};

/// Represents the double node test fixture.
class DoubleNode final : public takt::Node
{
  public:
    // Binds pipes to the nested frame-doubling worker.
    DoubleNode(std::shared_ptr<takt::Pipe<int>> in,
               std::shared_ptr<takt::Pipe<int>> out)
        : takt::Node("double-node"),
          in_(std::move(in)),
          out_(std::move(out))
    {
    }

  protected:
    /// Represents the worker task test fixture.
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent processing task per configured worker.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Doubles incoming frames until normal completion closes output.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        while (true)
        {
            try
            {
                auto r = in_->acquire_read();
                auto w = r.is_shot_end() ? out_->acquire_shot_end_write(1)
                                         : out_->acquire_write();
                w.value() = r.value() * 2;
                w.publish();
            }
            catch (const takt::PipeClosedAndEmptySignal&)
            {
                out_->close();
                break;
            }
        }
    }

  private:
    std::shared_ptr<takt::Pipe<int>> in_;
    std::shared_ptr<takt::Pipe<int>> out_;
};
} // namespace
