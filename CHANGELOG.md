# Changelog

All notable changes to this project will be documented in this file.

This project follows Semantic Versioning.

## [Unreleased]

## [0.2.0] - TBD

### Added
- Move-only `ScopedRecord<T>` and `ScopedReplay<T>` guards with explicit `reset()` support.
- Community and governance documents: `CONTRIBUTING.md`, `SECURITY.md`, and `CODE_OF_CONDUCT.md`.
- Compile-time checks that keep registry binding operations private.

### Changed
- `README.md` links community docs and includes an explicit license section.
- Record/replay terminology replaces dump/load across public APIs, examples, tests, and documentation.
- Explicit guard cleanup reports stream flush/close failures; destructors remain best-effort and non-throwing.
- A pipe permits at most one active record binding and one active replay binding at a time.

### Fixed
- Synchronize record/replay stream access and cleanup per pipe, without holding the global registry lock during file opens.
- Prevent stale guards from clearing replacement bindings after a cleanup failure.
- Add move-assignment, cleanup-failure, overlapping-binding, and parallel-test filename regression coverage.

### Removed
- Remove public `set_record`, `set_replay`, `clear_record`, `clear_replay`, `attach_record`, and `attach_replay` functions. Use `make_scoped_record()` and `make_scoped_replay()` instead.

## [0.1.0] - 2026-07-01

### Added
- Initial open-source release baseline.
