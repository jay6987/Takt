# Node Model

## Node

- Unified task-driven model with configurable `worker_count`.
- Default `worker_count` is 1.
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

1. Construct node
2. Set optional metadata (`stage`, `input pipe`, `output pipe` names)
3. `start()`:
   - starts ingress manager thread
   - starts `worker_count` worker threads
4. Upstream writes and then closes input pipe
5. `join()`

## Important Design Choice

Node has an internal task queue between ingress manager and workers.

Pipe capacity and token ownership still define the end-to-end pacing and
backpressure of the graph.
