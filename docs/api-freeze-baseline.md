# API Freeze Baseline (2026-07-01)

This document defines the frozen public API surface for the current release cycle.

## Freeze Scope

The public API is defined by `include/takt/takt.h` and the headers it exports:

- `include/takt/recordreplay/recordreplay.h`
- `include/takt/logging/logger.h`
- `include/takt/pipeline/channel.h`
- `include/takt/pipeline/pipe_base.h` (breaking, unreleased lifecycle migration)
- `include/takt/pipeline/pipe_config.h` (breaking, unreleased lifecycle migration)
- `include/takt/pipeline/node.h`
- `include/takt/pipeline/pipeline_builder.h`
- `include/takt/pipeline/graphviz.h` (additive, unreleased)
- `include/takt/pipeline/pipeline_runtime.h`
- `include/takt/pipeline/pipe.h`
- `include/takt/pipeline/pipe_endpoint.h`
- `include/takt/pipeline/subgraph_node.h`

## Freeze Rules

- No rename/removal/signature change for public symbols in this cycle.
- Additive API changes require explicit release-note entry.
- Behavior changes that alter error/close/lifecycle semantics require new tests.

The approved unreleased lifecycle and named-configuration migrations explicitly
supersede the original freeze for pipes/runtime preparation. `PipeBuilder` and
immediately initialized Pipe constructors are removed. `ProducerConfig` and
`ConsumerConfig` replace aggregate initialization with typed constructors;
numeric arguments require distinct named wrappers in fixed order, without a
legacy integer overload. Existing defaults and numeric fields remain unchanged.
See [Changelog](../CHANGELOG.md) and
[Pipe Usage](pipe-usage.md) for the required migration; C++17 remains the minimum.

## Public Declarations Snapshot

### Pipeline Graph Model

- `enum class PortDirection`
- `struct PortSpec`
- `struct NodeSpec`
- `struct EdgeSpec`
- `struct NodePortRef`
- `enum class MappingTransport` and `struct MappingTransportInfo` (additive, unreleased)
- `class PipelineBuilder`
- `class PipelineRuntime`
- `struct PipelineGraph` (additive, unreleased)
- `struct DotOptions` (additive, unreleased)
- `enum class DotView` (additive, unreleased; Connections is the default)
- `std::string to_dot(const PipelineGraph&, DotOptions)` (additive, unreleased)

### Node Model

- `struct NodeExceptionContext`
- `class NodeBase`
- `class Node`
- `class SubgraphNode`
- `SubgraphNode::MappingLink` (`BridgeLink` remains an alias for all mapped pairs)
- `SubgraphNode::mapping_count()`, `bridge_count()`, `launched_bridge_worker_count()`

### Pipe Model

- `using PipeDescriptor`
- `class PipeBase`
- `struct ProducerConfig`
- `struct ConsumerConfig`
- `struct Concurrency`, `struct FramesPerWrite`, `struct FramesPerRead`, `struct OverlapFrames`
- `constexpr Concurrency concurrency(size_t) noexcept`
- `constexpr FramesPerWrite frames_per_write(size_t) noexcept`
- `constexpr FramesPerRead frames_per_read(size_t) noexcept`
- `constexpr OverlapFrames overlap_frames(size_t) noexcept`
- `enum class BufferPolicy`
- `template <typename T> class Pipe`
- `PipeBase::set_slot_capacity(size_t)` and `PipeBase::slot_capacity()`
- `Pipe<T>::queued_frame_count()`, `Pipe<T>::acquire_write()`,
  `Pipe<T>::acquire_shot_end_write(size_t)`, and `Pipe<T>::acquire_read()`
- `WriteToken::frame_count()` and `ReadToken::frame_count()`
- `class AnyPipeEndpoint`
- `AnyPipeEndpoint::supports_direct_to(const AnyPipeEndpoint&) const noexcept`
- `MappingTransportInfo classify_mapping(const AnyPipeEndpoint&, const AnyPipeEndpoint&)`
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
