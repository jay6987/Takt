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
  - manager thread repeatedly calls `build_task(sequence_id)`
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

## Grouped frame execution

Derive from `takt::FrameTaskNode` when several workers should finish one frame
before the next frame is acquired. This reuses Node's permanent workers and
bounded queue; no nested pool or adapter-owned latch is needed.

Implement these hooks:

1. `create_frame()` allocates an owning `Frame` subclass before acquisition.
   Store move-only input/output tokens and per-frame numerical views in it.
2. `acquire_frame(Frame&)` acquires/prepares that context on ingress. Return
   false only for normal EOF; propagate errors. `frame.index` starts at zero.
3. Optional `on_frame_start(Frame&)` runs once on ingress.
4. `build_frame_task(Frame&, task_index)` lazily returns each work item, or null
   to seal the group. Empty groups are valid. The local task index and inner
   `Task::sequence_id` restart at zero for every frame; they are not frame IDs.
5. `process_frame_task(worker_index, Frame&, Task&)` runs on a permanent worker.
   Workers may share read-only input but must write disjoint ranges, or use
   exclusive preallocated worker-local state. The API does not enforce bounds
   or introduce a TokenView.
6. Optional `on_frame_complete(Frame&)` runs once on ingress after sealing and
   after every admitted task **and its payload destruction** has drained.
   Swap buffers and mark an output token published here. The context is then
   released, followed by the successful `completed_frames()` count.

Tasks may finish before generation seals without prematurely completing a frame.
The next acquire happens only after completion, context release and counting,
independently of Pipe capacity. `publish()` only marks a reservation; downstream
visibility requires token release.

Call protected `register_pipe(shared_ptr<PipeBase>, is_output)` during
configuration for all associated lifecycle dependencies. Normal EOF closes only
outputs. Failure preserves the original `exception_ptr` before fault-closing
registered pipes and stopping admission; cancellation normal-closes them to
wake blocked acquisition. Queued/running work drains before context destruction,
but abandoned work never counts as a successful frame. `failure()` reports
errors after `join()`; `join()` itself does not rethrow numerical hook failures.
An optional `on_frame_failure(exception_ptr)` records the original cause in an
application-wide error collector before pipes notify other nodes. This hook can
be called concurrently by failing workers and must be thread-safe.

Custom `PipeBase` implementations must now implement `close_with_error(string)`
as well as normal close. The standard `Pipe<T>` implements both. Fault closure
is distinct from EOF; neither token destruction nor a normal close certifies
successful processing.

As with Node, a derived destructor must call `request_stop(); join();` **before**
its members and hook implementations disappear. Keep associated Pipe objects
alive through token cleanup. Do not call `join()` from a worker/ingress hook.

The installed C++17 consumer in
[`tests/package_consumer/main.cpp`](../tests/package_consumer/main.cpp) exercises
an empty grouped frame. The scheduler/ownership/error tests in
[`tests/frame_task_node_tests.cpp`](../tests/frame_task_node_tests.cpp) show
nonempty groups, permanent workers, bounded admission and cancellation.
