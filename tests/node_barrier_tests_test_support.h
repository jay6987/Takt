// Declares task-group and shared-vector probes for barrier lifecycle tests.
#pragma once

#include "takt/takt.h"
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace takt_test
{
/// Exercises bounded task groups, barriers, draining, and frame ownership.
class GroupProbe final : public takt::Node
{
  public:
    GroupProbe(size_t workers,
               size_t count);
    ~GroupProbe() override;
    void request_stop() noexcept override;

    size_t count;
    int fail = -1;
    bool throw_failure_observer = false;
    std::atomic<size_t> acquired{0};
    std::atomic<size_t> generated{0};
    std::atomic<size_t> processed{0};
    std::atomic<size_t> ended{0};
    std::atomic<size_t> live_items{0};
    std::atomic<size_t> destroyed_frames{0};
    std::atomic<size_t> barrier_items_destroyed{0};
    std::thread::id ingress;
    std::function<void(size_t, Task&)> work;
    std::function<void(uint64_t)> generating;
    std::mutex task_mutex;
    std::condition_variable task_changed;
    std::shared_ptr<takt::Pipe<int>> pipe =
        std::make_shared<takt::Pipe<int>>("group-output");

  protected:
    /// Retains one task group until all worker payloads have been destroyed.
    struct Context final
    {
        explicit Context(GroupProbe& node);
        ~Context();
        GroupProbe& owner;
    };

    /// Tracks one admitted task through worker processing and barrier cleanup.
    struct Item final : Task
    {
        Item(GroupProbe& node,
             Context& group,
             size_t task_index,
             bool barrier);
        ~Item() override;

        GroupProbe& owner;
        Context* context;
        size_t local_index;
    };

  private:
    void maybe_fail(int phase);
    bool start_group();
    std::unique_ptr<Task> build_task(uint64_t sequence_id) override;
    void process_task(size_t worker,
                      Task& task) override;
    void on_barrier_complete(uint64_t sequence) override;
    void on_ingress_manager_done() override;
    void on_exception(const takt::NodeExceptionContext& exception) override;

    std::unique_ptr<Context> context_;
    size_t next_item_ = 0;
};

/// Coordinates vector regions with shared frame tokens and a completion gate.
class VectorBarrierProbe final : public takt::Node
{
  public:
    VectorBarrierProbe(
        std::shared_ptr<takt::Pipe<std::vector<float>>> input_pipe,
        std::shared_ptr<takt::Pipe<std::vector<float>>> output_pipe);
    ~VectorBarrierProbe() override;
    void request_stop() noexcept override;

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
    /// Holds frame tokens until all region tasks have completed.
    struct Context final
    {
        Context(VectorBarrierProbe& node,
                size_t group_index);
        ~Context();

        VectorBarrierProbe& owner;
        size_t group;
        takt::Pipe<std::vector<float>>::ReadToken read;
        takt::Pipe<std::vector<float>>::WriteToken write;
    };

    /// Identifies a worker's range in the shared vector frame.
    struct Region final : Task
    {
        Region(VectorBarrierProbe& node,
               Context& frame,
               size_t begin,
               size_t end,
               bool barrier);
        ~Region() override;

        VectorBarrierProbe& owner;
        Context* context;
        size_t first;
        size_t last;
    };

  private:
    std::unique_ptr<Task> build_task(uint64_t sequence_id) override;
    void process_task(size_t worker,
                      Task& task) override;
    void on_barrier_complete(uint64_t sequence) override;
    void on_ingress_manager_done() override;
    void on_exception(const takt::NodeExceptionContext& exception) override;
    static void close_pipe(
        const std::shared_ptr<takt::Pipe<std::vector<float>>>& pipe) noexcept;

    std::unique_ptr<Context> context_;
    size_t next_region_ = 0;
};
} // namespace takt_test

using takt_test::GroupProbe;
using takt_test::VectorBarrierProbe;
