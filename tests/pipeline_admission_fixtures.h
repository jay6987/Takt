// Supplies blocked workers and reserved-token nodes for admission tests.
#pragma once
#include "pipeline_contract_tests_test_support_common.h"
namespace
{

/// Represents the blocked node test fixture.
class BlockedNode final : public takt::Node
{
  public:
    // Blocks processing on a shared signal to expose bounded ingress behavior.
    explicit BlockedNode(std::shared_future<void> release)
        : Node("bounded"),
          release_(std::move(release))
    {
    }
    std::atomic<int> built{0};
    std::promise<void> first_built;

  private:
    // Produces four tasks and signals when the first task is built.
    std::unique_ptr<Task> build_task(uint64_t sequence) override
    {
        if (sequence == 4)
            return nullptr;
        auto task = std::make_unique<Task>();
        if (built.fetch_add(1) == 0)
            first_built.set_value();
        return task;
    }
    // Waits for the test to release the permanently blocked worker.
    void process_task(size_t,
                      Task&) override
    {
        release_.wait();
    }
    std::shared_future<void> release_;
};

/// Represents the reserved token node test fixture.
class ReservedTokenNode final : public takt::Node
{
  public:
    // Hold all workers to expose whether ingress prefetches pipe tokens.
    ReservedTokenNode(takt::Pipe<int>& input,
                      takt::Pipe<int>& output,
                      size_t workers,
                      std::shared_future<void> release)
        : Node("reserved",
               workers),
          input_(input),
          output_(output),
          release_(std::move(release))
    {
    }
    // Keep referenced tokens and synchronization state alive through join.
    ~ReservedTokenNode() override
    {
        request_stop();
        join();
    }
    std::promise<void> saturated;
    std::atomic<size_t> built{0}, started{0}, destroyed{0};

  private:
    /// Represents the token task test fixture.
    struct TokenTask final : Task
    {
        ReservedTokenNode& owner;
        takt::Pipe<int>::ReadToken input;
        takt::Pipe<int>::WriteToken output;
        // Count cleanup after processing rather than merely counting returns.
        explicit TokenTask(ReservedTokenNode& node) : owner(node) {}
        // Record retirement of each handed-off payload.
        ~TokenTask() override
        {
            ++owner.destroyed;
        }
    };
    // Acquire tokens only after a worker has been reserved for this payload.
    std::unique_ptr<Task> build_task(uint64_t sequence) override
    {
        if (sequence == worker_count() + 1)
            return nullptr;
        auto task = std::make_unique<TokenTask>(*this);
        task->input = input_.acquire_read();
        task->output = output_.acquire_shot_end_write(1);
        ++built;
        return task;
    }
    // Hold the workers until the test observes lazy acquisition.
    void process_task(size_t,
                      Task& base) override
    {
        auto& task = static_cast<TokenTask&>(base);
        if (++started == worker_count())
            saturated.set_value();
        release_.wait();
        task.output.value() = task.input.value() * 2;
        task.output.publish();
    }
    takt::Pipe<int>& input_;
    takt::Pipe<int>& output_;
    std::shared_future<void> release_;
};
} // namespace
