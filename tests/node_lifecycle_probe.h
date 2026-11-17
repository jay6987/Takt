// Supplies a configurable node probe for lifecycle hook failure injection.
#pragma once
#include "node_tests_test_support_common.h"
namespace
{

/// Represents the lifecycle probe test fixture.
class LifecycleProbe final : public takt::Node
{
  public:
    // Inject one failure into a framework-controlled execution phase.
    LifecycleProbe(int phase,
                   bool unknown = false,
                   bool throwing_observer = false)
        : Node("lifecycle"),
          phase_(phase),
          unknown_(unknown),
          throwing_observer_(throwing_observer)
    {
    }
    // Join while task destructors and hooks can still access probe state.
    ~LifecycleProbe() override
    {
        request_stop();
        join();
    }
    std::atomic<size_t> live{0}, processed{0}, barriers{0}, cleanup{0};
    std::atomic<bool> reported_unknown{false};

  private:
    /// Represents the probe task test fixture.
    struct ProbeTask final : Task
    {
        LifecycleProbe& owner;
        // Keep track of admitted payloads through successful or failed work.
        explicit ProbeTask(LifecycleProbe& probe) : owner(probe)
        {
            ++owner.live;
        }
        // Task destruction must precede ingress completion notifications.
        ~ProbeTask() override
        {
            --owner.live;
        }
    };
    // Throw the original cause without disguising nonstandard exceptions.
    void fail_at(int phase)
    {
        if (phase_ != phase)
            return;
        if (unknown_)
            throw 42;
        throw std::domain_error("original lifecycle failure");
    }
    // Emit a barrier payload, or a terminal stop payload for stop coverage.
    std::unique_ptr<Task> build_task(uint64_t sequence) override
    {
        fail_at(0);
        if (sequence != 0)
            return nullptr;
        auto task = std::make_unique<ProbeTask>(*this);
        task->barrier = true;
        if (phase_ == 6)
            task->kind = TaskKind::Stop;
        return task;
    }
    // Count work and exercise failure before the task completion hook.
    void process_task(size_t,
                      Task&) override
    {
        ++processed;
        fail_at(1);
    }
    // Exercise cleanup when a post-processing worker hook throws.
    void on_task_done(size_t,
                      const Task&) override
    {
        fail_at(2);
    }
    // Observe payload destruction before group publication on ingress.
    void on_barrier_complete(uint64_t) override
    {
        EXPECT_EQ(live.load(), 0U);
        fail_at(3);
        ++barriers;
    }
    // Check drain on successful EOF, failed processing and cancellation.
    void on_ingress_manager_done() override
    {
        EXPECT_EQ(live.load(), 0U);
        ++cleanup;
        fail_at(4);
    }
    // Exercise retained failures after a permanent worker terminates.
    void on_worker_done(size_t) override
    {
        fail_at(5);
    }
    // Observe nonstandard causes and optionally throw a secondary error.
    void on_exception(const takt::NodeExceptionContext& context) override
    {
        reported_unknown = context.unknown_exception;
        if (throwing_observer_)
            throw std::runtime_error("secondary observer failure");
        Node::on_exception(context);
    }
    int phase_;
    bool unknown_;
    bool throwing_observer_;
};
} // namespace
