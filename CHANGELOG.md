# Changelog

All notable changes to this project will be documented in this file.

This project follows Semantic Versioning.

## [Unreleased]

### Added
- Standalone LLVM 23 quality gate and CI enforcement for 80-column C++ lines,
  file/function descriptions and one-parameter-per-line declarations.
- Independent >=80% production line/branch coverage, ASan+UBSan and TSan gates,
  with regression fixtures shared by the Lynx integration.
- Logger dispatch and invalid builder-input/metadata regression tests.
- Direct subgraph mappings for compatible typed endpoints sharing one Pipe;
  custom endpoints explicitly opt in through `supports_direct_to`.
- Value-owned Direct/Bridge/Unresolved mapping transport metadata, local mapping
  and bridge launch counts, and `DotView::Interfaces` for logical port inspection.
- Explicit `PipeBase` preparation, `ProducerConfig`, `ConsumerConfig`, capacity
  policy requests and configured-batch acquisition APIs.
- `PipelineRuntime::initialize()` prepares endpoint pipes and nested runtimes;
  directly constructed runtimes may receive an explicit pipe collection.
- Scoped `PipelineBuilder::create(name)` creates independent state with
  owner-managed lifetimes for bound nodes and payload storage.
- `PipelineBuilder::graph()` and `to_dot()` export topology without running workers.
- `PipelineGraph`, `DotOptions` and free `to_dot()` support descriptive graphs,
  nested Graphviz clusters, collapsed subgraphs, ports and edge labels.
- Bound subgraphs retain value-owned topology snapshots for deterministic recursive export.
- Opt-out read-history policy for ordinary non-overlap, token-owned image streams.
- Error closure that rejects new writes, drains the successful prefix and reports failure to readers.
- Bounded node task admission and first-failure inspection; runtime join reports node failures after joining all nodes.

### Changed
- **Breaking:** Removed `PipelineBuilder::declare(name)` and its process-wide
  named-state registry. Use `create(name)` for independent graphs, even with
  identical names; pass a builder reference or copy its handle for explicit
  sharing instead of looking it up by name.
- Reformatted and documented owned C++ sources without changing public APIs.
- Installed-package smoke validation now runs the consumer, not only builds it.
- Owned test, quality-tool and example targets treat warnings as errors.
- **Breaking:** `ProducerConfig` and `ConsumerConfig` require strong named numeric
  arguments: `concurrency(...)`, `frames_per_write(...)`, `frames_per_read(...)`
  and `overlap(...)`, in fixed order. Bare integers and mismatched types no
  longer compile. Name-only/default construction and omitted trailing arguments
  retain concurrency 1, one frame per write/read and overlap 0; stored numeric
  fields and runtime validation are unchanged.
- Expanded DOT defaults to cross-cluster Direct connections and explicit Bridge
  worker nodes; descriptive mappings without metadata remain Unresolved.
  Collapsed subgraphs summarize local mapping modes.
- Subgraph lifecycle registration and stop cover all mapped pipes independently
  of bridge workers. Direct boundaries configure only real compute participants;
  unsupported same-Pipe custom mappings fail before starting workers.
- **Breaking:** pipes are constructed with a name only, wired/configured on one
  stable object, and explicitly initialized before acquisition. Runtime `start()`
  requires prior `initialize()`. Configuration freezes after initialization.
- Default capacity is the larger endpoint slot demand; max-concurrency requests
  use their sum. Explicit positive capacities override both without a concurrency
  or batch lower bound, and unused capacity demand is not calculated.
- Examples, installed-package consumer and tests use the explicit lifecycle;
  the C++17 requirement is unchanged.
- Installed upgrades require a fresh prefix or removal of the obsolete
  `include/takt/pipeline/builder.h`; directory installation does not prune
  old headers. The clean-prefix smoke consumer rejects a stale builder header.
- Runtime stop closes registered pipes and is terminal; stopping before
  initialization cancels uninitialized pipes and prevents later initialize/start.
  `PipeBase` implementations must provide `close()` for runtime cancellation.
- Typed subgraph bridges retain read/write tokens and swap basket storage rather than copying through `std::any`.
- Endpoint implementations must provide `close_with_error`; vector bridges exchange reusable storage ownership.
- `fault_message()` returns a synchronized string snapshot instead of an unsynchronized reference.

### Fixed
- Typed bridge mismatch reporting waits for a source packet, preserving packet
  transfer ordering rather than asynchronously rejecting a producer before its first write.
- Closing a pipe no longer reports end-of-stream before previously reserved writers finish.
- An abandoned write no longer exposes later frames across the failed logical position.
- A closed incomplete segment can drain its successfully published prefix.
- Ordinary nontrivial payloads no longer require a codec when record/replay is disabled.
- Replay failure releases the reserved read slots before rethrowing.
- Consecutive read reservations now advance past segment markers held by earlier active tokens.

### Removed
- **Breaking:** `PipeBuilder` and its header/implementation, and immediately
  initialized Pipe constructors. Replace them with one shared `Pipe<T>("name")`,
  `set_producer(config, template)`, `set_consumer(config)` and explicit standalone
  or runtime initialization. No compatibility shim or global pipe-name registry
  is retained.

## [0.2.0] - 2026-10-31

### Added
- Move-only `ScopedRecord<T>` and `ScopedReplay<T>` guards with explicit `reset()` support.
- Installable CMake package with exported `takt::takt` target and `find_package(Takt 0.2 CONFIG)` support.
- Community and governance documents: `CONTRIBUTING.md`, `SECURITY.md`, and `CODE_OF_CONDUCT.md`.
- Compile-time checks that keep registry binding operations private.

### Changed
- `README.md` links community docs and includes an explicit license section.
- README documents both `add_subdirectory` and installed-package workflows; examples and tests default off when used as a dependency.
- Record/replay terminology replaces dump/load across public APIs, examples, tests, and documentation.
- Explicit guard cleanup reports stream flush/close failures; destructors remain best-effort and non-throwing.
- A pipe permits at most one active record binding and one active replay binding at a time.
- Security Policy identifies `master` as the supported development branch.

### Fixed
- Synchronize record/replay stream access and cleanup per pipe, without holding the global registry lock during file opens.
- Prevent stale guards from clearing replacement bindings after a cleanup failure.
- Add move-assignment, cleanup-failure, overlapping-binding, and parallel-test filename regression coverage.
- Add an installed-package consumer smoke test to CTest.

### Removed
- Remove public `set_record`, `set_replay`, `clear_record`, `clear_replay`, `attach_record`, and `attach_replay` functions. Use `make_scoped_record()` and `make_scoped_replay()` instead.

## [0.1.0] - 2026-07-01

### Added
- Initial open-source release baseline.
