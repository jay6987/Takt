// Supplies a node paused during token preparation to test cancellation.
#pragma once
#include "pipeline_contract_tests_test_support_common.h"
namespace
{

/// Represents the preparing token node test fixture.
class PreparingTokenNode final : public takt::Node
{
  public:
    // Hold ingress after acquisition to exercise cancellation of a reservation.
    PreparingTokenNode(takt::Pipe<int>& input,
                       takt::Pipe<int>& output,
                       std::shared_future<void> release)
        : Node("preparing",
               2),
          input_(input),
          output_(output),
          release_(std::move(release))
    {
    }
    // Preserve hook and task state until all framework threads have joined.
    ~PreparingTokenNode() override
    {
        request_stop();
        join();
    }
    std::promise<void> preparing;
    std::atomic<size_t> destroyed{0}, processed{0};

  private:
    /// Represents the token task test fixture.
    struct TokenTask final : Task
    {
        PreparingTokenNode& owner;
        takt::Pipe<int>::ReadToken input;
        takt::Pipe<int>::WriteToken output;
        // Track a payload whose tokens have been acquired but not handed off.
        explicit TokenTask(PreparingTokenNode& node) : owner(node) {}
        // Count destruction when ingress cancels the pending handoff.
        ~TokenTask() override
        {
            ++owner.destroyed;
        }
    };
    // Simulate ingress preparation while keeping both tokens in the payload.
    std::unique_ptr<Task> build_task(uint64_t) override
    {
        auto task = std::make_unique<TokenTask>(*this);
        task->input = input_.acquire_read();
        task->output = output_.acquire_write();
        preparing.set_value();
        release_.wait();
        return task;
    }
    // Cancellation must prevent this payload from reaching a reserved worker.
    void process_task(size_t,
                      Task&) override
    {
        ++processed;
    }
    takt::Pipe<int>& input_;
    takt::Pipe<int>& output_;
    std::shared_future<void> release_;
};
} // namespace
