// Supplies single-worker and multi-worker nodes with injected failures.
#pragma once
#include "node_tests_test_support_common.h"
namespace
{

/// Represents the faulty single worker agent test fixture.
class FaultySingleWorkerAgent final : public takt::Node
{
  public:
    // Creates a single-worker node with deterministic processing failure.
    FaultySingleWorkerAgent() : takt::Node("faulty-single-worker") {}

    // Returns failure context captured after worker execution has joined.
    const takt::NodeExceptionContext& captured() const
    {
        return captured_;
    }

    // Reports whether the exception hook captured a worker failure.
    bool has_capture() const
    {
        return has_capture_;
    }

  protected:
    /// Represents the worker task test fixture.
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task that will trigger the injected failure.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Injects the known single-worker processing exception.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        throw std::runtime_error("boom");
    }

    // Saves exception context and preserves the default structured logger.
    void on_exception(const takt::NodeExceptionContext& context) override
    {
        captured_ = context;
        has_capture_ = true;
        takt::Node::on_exception(context);
    }

  private:
    takt::NodeExceptionContext captured_;
    bool has_capture_ = false;
};

/// Represents the faulty multi worker agent test fixture.
class FaultyMultiWorkerAgent final : public takt::Node
{
  public:
    // Creates two workers with deterministic processing failures.
    FaultyMultiWorkerAgent()
        : takt::Node("faulty-multi-worker",
                     2)
    {
    }

    // Returns captured context after all failing workers have joined.
    const takt::NodeExceptionContext& captured() const
    {
        return captured_;
    }

    // Reports whether any worker's exception hook captured a failure.
    bool has_capture() const
    {
        return has_capture_;
    }

  protected:
    /// Represents the worker task test fixture.
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task for each failing worker.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Injects the known multiworker processing exception.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        throw std::runtime_error("kaboom");
    }

    // Serializes captured failure context and preserves standard logging.
    void on_exception(const takt::NodeExceptionContext& context) override
    {
        {
            std::lock_guard<std::mutex> lk(capture_mutex_);
            captured_ = context;
            has_capture_ = true;
        }
        takt::Node::on_exception(context);
    }

  private:
    std::mutex capture_mutex_;
    takt::NodeExceptionContext captured_;
    bool has_capture_ = false;
};
} // namespace
