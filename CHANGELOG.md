# Changelog

All notable changes to this project will be documented in this file.

This project follows Semantic Versioning.

## [Unreleased]

### Added
- Scoped `PipelineBuilder::create(name)` creates independent state without retaining
  bound nodes and payload storage in the named registry.
- `PipelineBuilder::graph()` and `to_dot()` export topology without running workers.
- `PipelineGraph`, `DotOptions` and free `to_dot()` support descriptive graphs,
  nested Graphviz clusters, collapsed subgraphs, ports and edge labels.
- Bound subgraphs retain value-owned topology snapshots for deterministic recursive export.
- Opt-out read-history policy for ordinary non-overlap, token-owned image streams.
- Error closure that rejects new writes, drains the successful prefix and reports failure to readers.
- Bounded node task admission and first-failure inspection; runtime join reports node failures after joining all nodes.

### Changed
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
