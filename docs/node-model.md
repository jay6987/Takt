# Node Model

## Node

- Unified task-driven model with configurable `worker_count`.
- Default `worker_count` is 1.
- `task_queue_capacity` defaults to 1 and must be positive. Admission blocks
  before `build_task` while the queue is full.
- `build_task(sequence_id)` runs in an ingress manager thread.
- `process_task(worker_index, task)` runs in worker threads.
- Typical pattern:

```cpp
struct DoubleTask final : public takt::Node::Task
{
    int input = 0;
};

std::unique_ptr<takt::Node::Task> build_task(uint64_t) override {
    try {
        auto r = input_pipe_.acquire_read();
        auto task = std::make_unique<DoubleTask>();
        task->input = r.value();
        return task;
    } catch (const takt::PipeClosedAndEmptySignal&) {
        output_pipe_.close();
        return nullptr;
    }
}

void process_task(size_t, takt::Node::Task& task) override {
    auto& t = static_cast<DoubleTask&>(task);
    auto w = output_pipe_.acquire_write();
    w.value() = transform(t.input);
    w.publish();
}
```

## Worker Count

- `Node("name")` creates a single-worker node.
- `Node("name", N)` creates an N-worker node.
- Manager count is fixed to 1 per node; workers are configurable.

## Lifecycle

1. Create name-only pipes and construct nodes holding those same connection objects
2. Configure both ends/template and optional pipe policies; set node metadata
3. Initialize bound `PipelineRuntime` (including nested runtimes), or initialize
   standalone pipes explicitly before directly starting nodes
4. `start()`:
   - starts ingress manager thread
   - starts `worker_count` worker threads
5. Upstream writes and then closes input pipe
6. `join()`

Runtime `start()` requires prior explicit `initialize()` and never allocates
baskets implicitly. A directly started `SubgraphNode` likewise requires its
internal runtime to be initialized before start.

## Important Design Choice

Node has an internal task queue between ingress manager and workers.

Pipe capacity and token ownership still define the end-to-end pacing and
backpressure of the graph.

## Token-owned image tasks

The copied scalar example above is not the intended image transport pattern.
An image task should own move-only `Pipe<std::vector<float>>::ReadToken` and
`WriteToken` members acquired by `build_task`. Move those tokens into the task,
enqueue the task, and process directly in their baskets. Call `publish()` only
after successful processing; destruction retires the write and releases the
read basket. Do not copy the input image or release its read token during task
construction. Preallocate each pipe's vector template and each worker's scratch,
and configure the consumer's window once before initialization. With
`read_size == 1` and `overlap_size == 0`, `acquire_read()` reads one frame; larger
windows are returned by the same API as a batch token.

## Failure and shutdown

Nodes retain the first original exception in `failure()`. Worker failure stops
admission and discards unprocessed queued tasks, so their uncommitted write
tokens fault the affected pipes rather than publishing invalid output.
`PipelineRuntime::join()` joins all nodes and then rethrows the first recorded
failure; `Node::join()` alone remains a non-reporting synchronization operation.
Subgraph join also finishes its bridge workers before reporting failure.
Typed same-Pipe port mappings are Direct and launch no bridge workers.
All mapped boundary pipes still participate in initialization and stop;
real compute nodes must propagate errors and EOF on shared pipes just as on
ordinary graph connections. Subgraph nesting does not replace these node duties.

Pipe-owning nodes must close their bound input/output pipes with an explicit
error from `on_exception`, and close output after ingress finishes. Closing
output does not cancel previously reserved write baskets. A stop override or
external shutdown must close pipes to wake threads blocked in pipe acquisition;
`Node::request_stop()` only wakes the internal task queue. Derived destructors
must stop and join before destroying pipe references or worker scratch.

`PipelineRuntime::request_stop()` additionally closes registered pipes and
propagates stop through nested runtimes, so directly constructed runtimes should
receive pipes that are not exposed through bound endpoints. Runtime stop is
terminal: stopping before initialization cancels preparation and prevents later
initialize/start. Direct node shutdown still needs explicit pipe closure.
