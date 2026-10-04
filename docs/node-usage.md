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
        bool segment_end = false;
    };

    std::unique_ptr<takt::Node::Task> build_task(uint64_t) override
    {
        try
        {
            auto r = in_->acquire_read();
            auto task = std::make_unique<DoubleTask>();
            task->value = r.value();
            task->segment_end = r.is_segment_end();
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
        auto w = out_->acquire_write(t.segment_end);
        w.value() = t.value * 2;
        w.publish();
    }

  private:
    std::shared_ptr<takt::Pipe<int>> in_;
    std::shared_ptr<takt::Pipe<int>> out_;
};
```

## Runtime Lifecycle

- Construct node with input/output pipes.
- Choose worker count: default 1, or pass explicit count in constructor.
- Optionally set metadata (`set_stage_name`, `add_input_port_name`, `add_output_port_name`).
- Call `start()`:
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
