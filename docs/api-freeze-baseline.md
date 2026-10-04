# API Freeze Baseline (2026-07-01)

This document defines the frozen public API surface for the current release cycle.

## Freeze Scope

The public API is defined by `include/takt/takt.h` and the headers it exports:

- `include/takt/recordreplay/recordreplay.h`
- `include/takt/logging/logger.h`
- `include/takt/pipeline/channel.h`
- `include/takt/pipeline/builder.h`
- `include/takt/pipeline/node.h`
- `include/takt/pipeline/pipeline_builder.h`
- `include/takt/pipeline/pipeline_runtime.h`
- `include/takt/pipeline/pipe.h`
- `include/takt/pipeline/pipe_endpoint.h`
- `include/takt/pipeline/subgraph_node.h`

## Freeze Rules

- No rename/removal/signature change for public symbols in this cycle.
- Additive API changes require explicit release-note entry.
- Behavior changes that alter error/close/lifecycle semantics require new tests.

## Public Declarations Snapshot

### Pipeline Graph Model

- `enum class PortDirection`
- `struct PortSpec`
- `struct NodeSpec`
- `struct EdgeSpec`
- `struct NodePortRef`
- `class PipelineBuilder`
- `class PipelineRuntime`

### Node Model

- `struct NodeExceptionContext`
- `class NodeBase`
- `class Node`
- `class SubgraphNode`

### Pipe Model

- `using PipeDescriptor`
- `class PipeBuilder`
- `template <typename T> class Pipe`
- `class AnyPipeEndpoint`
- `template <typename T> class TypedPipeEndpoint`
- `struct AnyPipePacket`
- `struct PipeClosedAndEmptySignal`
- `struct PipeFaultedError`
- `struct PipeWriteAbandonedError`

### Record/Replay and Logging

- `template <typename T> struct PipeCodec`
- `class RecordReplayRegistry`
- `class ILogger`

## Evidence

- Snapshot command: `grep -RnE "^(class|struct|enum class|using )" include/takt --include='*.h'`
- Export hub: `include/takt/takt.h`
