# Structured Exception Context

`NodeExceptionContext` provides metadata-rich failure context for observability.

Fields:

- `node_name`
- `stage_name`
- `input_port_names`
- `output_port_names`
- `message`
- `unknown_exception`
- `has_worker_index`
- `worker_index`

## How to Set Metadata

```cpp
node.set_stage_name("decode-stage");
node.add_input_port_name("raw-frames");
node.add_output_port_name("decoded-frames");
```

## Override Exception Callback

Node:

```cpp
void on_exception(const takt::NodeExceptionContext& context) override {
    // forward to telemetry, metrics, or custom logger
}
```

When `worker_count > 1`:

```cpp
void on_exception(const takt::NodeExceptionContext& context) override {
    // includes worker metadata when exception is from process_task()
}
```

## Default Behavior

Base implementations log context-rich messages via the global logger.
