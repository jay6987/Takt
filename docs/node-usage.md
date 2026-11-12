# Node Usage Guide

`Node` is a unified task-driven abstraction with one ingress manager thread and
configurable worker threads.

## Single-Worker Pattern

```cpp
class DoubleNode final : public takt::Node
{
  public:
    DoubleNode(std::shared_ptr<takt::Pipe<int>> in,
               std::shared_ptr<takt::Pipe<int>> out)
        : takt::Node("double-node"), in_(std::move(in)), out_(std::move(out))
    {
    }

  protected:
    struct DoubleTask final : public takt::Node::Task
    {
        int value = 0;
        bool shot_end = false;
    };

    std::unique_ptr<takt::Node::Task> build_task(uint64_t) override
    {
        try
        {
            auto r = in_->acquire_read();
            auto task = std::make_unique<DoubleTask>();
            task->value = r.value();
            task->shot_end = r.is_shot_end();
            return task;
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            out_->close();
            return nullptr;
        }
    }

    void process_task(size_t, takt::Node::Task& task) override
    {
        auto& t = static_cast<DoubleTask&>(task);
        auto w = t.shot_end ? out_->acquire_shot_end_write(1)
                               : out_->acquire_write();
        w.value() = t.value * 2;
        w.publish();
    }

  private:
    std::shared_ptr<takt::Pipe<int>> in_;
    std::shared_ptr<takt::Pipe<int>> out_;
};
```

## Runtime Lifecycle

- Create name-only input/output pipes and share each connection with its two ends.
- Configure each pipe's producer/template and consumer, plus optional capacity,
  descriptor and buffer policy. Participant concurrency describes slot demand;
  it is not required to equal a node's worker count.
- Construct node with those same input/output pipes.
- Choose worker count: default 1, or pass explicit count in constructor.
- Optionally set metadata (`set_stage_name`, `add_input_port_name`, `add_output_port_name`).
- Prepare bound pipelines with `runtime->initialize()` before `runtime->start()`.
  If starting a node directly, explicitly initialize its standalone pipes first.
- Call node `start()`:
  - ingress manager reserves an idle worker before calling
    `build_task(sequence_id)`
  - worker threads repeatedly call `process_task(worker_index, task)`
- Push data and close upstream input pipe.
- Call `join()`.

## Runnable Example

- Source: `examples/node/main.cpp`
- Build target: `takt_node_example`

Run after build:

```bash
./build/examples/node/takt_node_example
```

## Grouped work with barriers

Use an ordinary `takt::Node` and mark the final task of each group with
`Task::barrier`. The ingress manager calls `on_barrier_complete(sequence_id)`
only after that barrier and every earlier admitted task have been destroyed.
The callback runs on ingress, so it can publish output and release the
ingress-held context before the next group's input is acquired. `Task` also
retains its global `sequence_id`; group-local task indices belong in derived
payloads. A null task ends normal admission, not a group. `TaskKind::Stop`
requests terminal cancellation; use a normal task with `barrier = true` to
complete a group successfully.

The following shape splits one vector frame into disjoint regions. The
ingress-owned context keeps move-only tokens alive while worker tasks reference
it. Configure each pipe with one-frame windows and matching vector templates
before use.

```cpp
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <takt/takt.h>

class SplitNode final : public takt::Node
{
  public:
    // Keep the configured pipes alive through token cleanup.
    SplitNode(std::shared_ptr<takt::Pipe<std::vector<float>>> input,
              std::shared_ptr<takt::Pipe<std::vector<float>>> output)
        : Node("split", 4), input_(std::move(input)),
          output_(std::move(output))
    {
    }

    // Stop execution before releasing this node's pipe references.
    ~SplitNode() override
    {
        request_stop();
        join();
    }

  protected:
    struct Context
    {
        takt::Pipe<std::vector<float>>::ReadToken input;
        takt::Pipe<std::vector<float>>::WriteToken output;
    };

    struct Region final : Task
    {
        Context* context = nullptr;
        size_t first = 0;
        size_t last = 0;
    };

    // Acquire one frame and lazily divide it among worker regions.
    std::unique_ptr<Task> build_task(uint64_t) override
    {
        if (!context_)
        {
            try
            {
                context_ = std::make_unique<Context>();
                context_->input = input_->acquire_read();
                context_->output = output_->acquire_write();
                next_region_ = 0;
                region_count_ = worker_count();
                region_size_ = context_->input.value(0).size();
            }
            catch (const takt::PipeClosedAndEmptySignal&)
            {
                context_.reset();
                output_->close();
                return nullptr;
            }
        }

        auto task = std::make_unique<Region>();
        task->context = context_.get();
        task->first = region_size_ * next_region_ / region_count_;
        task->last = region_size_ * (next_region_ + 1) / region_count_;
        ++next_region_;
        task->barrier = next_region_ == region_count_;
        return task;
    }

    // Write only this task's disjoint range of the shared output frame.
    void process_task(
        size_t,
        Task& base) override
    {
        auto& task = static_cast<Region&>(base);
        const auto& source = task.context->input.value(0);
        auto& destination = task.context->output.value(0);
        for (size_t i = task.first; i < task.last; ++i)
            destination[i] = transform(source[i]);
    }

    // Publish only after every region task and payload has completed.
    void on_barrier_complete(uint64_t) override
    {
        context_->output.publish();
        context_.reset();
    }

    // Release retained tokens after all work has been drained or discarded.
    void on_ingress_manager_done() override
    {
        context_.reset();
    }

    // Fault both owned connections while preserving the original exception.
    void on_exception(
        const takt::NodeExceptionContext& context) override
    {
        close_with_error(input_, context.message);
        close_with_error(output_, context.message);
        Node::on_exception(context);
    }

    // Close owned pipes so cancellation interrupts blocked acquisitions.
    void request_stop() noexcept override
    {
        Node::request_stop();
        close(input_);
        close(output_);
    }

  private:
    // Keep arithmetic separate so the worker operation is easy to replace.
    static float transform(float value)
    {
        return value * 2.0F;
    }

    // Close a pipe without allowing cleanup to escape a noexcept stop hook.
    static void close(
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

    // Fault-close a pipe when it has not already recorded a root cause.
    static void close_with_error(
        const std::shared_ptr<takt::Pipe<std::vector<float>>>& pipe,
        const std::string& message)
    {
        if (!pipe->faulted())
            pipe->close_with_error(message);
    }

    std::shared_ptr<takt::Pipe<std::vector<float>>> input_;
    std::shared_ptr<takt::Pipe<std::vector<float>>> output_;
    std::unique_ptr<Context> context_;
    size_t next_region_ = 0;
    size_t region_count_ = 0;
    size_t region_size_ = 0;
};
```

In production code, close owned pipes on stop and fault-close them from
`on_exception`. Do not wait on peer workers from `process_task`; the barrier
performs that synchronization. A temporarily idle worker does not complete a
group: only its explicit final barrier does. Empty or unknown-length groups can
use a no-op task marked as a barrier.

Admission reserves an idle worker before calling `build_task`; there is no
backlog queue and no speculative task construction. At most `worker_count()`
tasks are in flight. On failure or stop, admitted tasks are drained or
discarded before `on_ingress_manager_done`; uncommitted tokens are destroyed
there, and only successful barriers call `on_barrier_complete`. The first
original exception remains available through `failure()`.

A derived destructor must stop and join before its members and hook
implementations disappear. `request_stop()` wakes internal coordination only;
derived nodes must close their own pipes to interrupt blocked acquisition.
Keep associated Pipe objects alive through token cleanup. Do not call `join()`
from a worker/ingress hook. `Node::join()` synchronizes without rethrowing;
inspect `failure()` afterward or use `PipelineRuntime::join()` to report it.

The installed C++17 consumer in
[`tests/package_consumer/main.cpp`](../tests/package_consumer/main.cpp) exercises
an empty barrier group. The scheduler/ownership/error tests in
[`tests/node_barrier_tests.cpp`](../tests/node_barrier_tests.cpp) show nonempty
groups, permanent workers, bounded admission and cancellation.
