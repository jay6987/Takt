# Takt Release Checklist

This checklist is for moving Takt from internal/POC quality to a releasable state.

## Usage

- Owner: fill each item owner before execution.
- Status: use `[ ]` not done, `[x]` done.
- Evidence: attach command output, PR link, or test report link.
- Exit rule: all P0 and P1 items must be checked before release tag.

## Scope

- In scope: API stability, safety hardening, runtime correctness, test strength, documentation, release process.
- Out of scope: new architecture experiments, non-critical feature additions.

## P0: Must Have Before Tag

### 1) Public API Freeze
- [x] Freeze public headers and symbols.
- [x] Confirm no breaking rename/signature changes in this cycle.
- Owner: zhiwei
- Evidence: `docs/api-freeze-baseline.md`; declaration snapshot command output recorded on 2026-07-01.
- Acceptance:
  - No public API changes in the final two pre-release PRs.

### 2) Bridge Type Safety Hardening
- [x] Add explicit error path for endpoint type mismatch.
- [x] Error message includes node/port names and expected vs actual type info.
- Owner: zhiwei
- Evidence: `include/takt/pipeline/pipe_endpoint.h`, `src/pipeline/subgraph_node.cpp`, `src/pipeline/pipeline_builder.cpp`, `tests/pipeline_builder_tests.cpp` (`PipeEndpointWriteReportsTypeMismatch`).
- Acceptance:
  - Mismatch is deterministic and diagnosable from one error report.

### 3) Unified Error Propagation
- [x] Ensure node worker failures propagate to runtime-level failure handling.
- [x] Ensure subgraph bridge worker failures propagate consistently.
- Owner: zhiwei
- Evidence: `src/pipeline/subgraph_node.cpp` (bridge error capture + `join()` rethrow), `tests/pipeline_builder_tests.cpp` (`SubgraphBridgeFailurePropagatesToRuntimeJoin`).
- Acceptance:
  - Any worker/bridge failure is visible at top-level control path.

### 4) Lifecycle Concurrency Validation
- [x] Validate `start/join/request_stop/close` semantics under concurrency.
- [x] Validate subgraph nested lifecycle ordering.
- Owner: zhiwei
- Evidence: `tests/pipeline_builder_tests.cpp` (`SubgraphLifecycleRequestStopJoinNoDeadlock`), plus existing nested subgraph lifecycle tests in `tests/node_tests.cpp`.
- Acceptance:
  - No deadlock/hang in repeated lifecycle stress runs.

### 5) Full Regression Baseline
- [x] Build all targets.
- [x] Run full CTest suite.
- [x] Ensure no flaky failures in three consecutive runs.
- Owner: zhiwei
- Evidence: CMake build green; full CTest suite green 3 consecutive runs on 2026-07-01.
- Acceptance:
  - 3/3 green on full test suite.

## P1: Strongly Recommended For Stable Release

### 6) Stress and Soak
- [x] Add/execute high-concurrency stress case.
- [x] Add/execute soak run (at least 30 minutes).
- Owner: zhiwei
- Evidence: Added `tests/pipe_tests.cpp` (`HighConcurrencyStressNoLossNoDuplication`). Executed `./scripts/p1_6_soak.sh` on 2026-07-03 with summary `SOAK_STATUS=PASSED`, `duration_sec=1801`, `iterations=635`, `first_maxrss_kb=10768384`, `last_maxrss_kb=10739712`, `min_maxrss_kb=10502144`, `max_maxrss_kb=11153408`.
- Acceptance:
  - No crash, no resource growth trend indicating leak, no data corruption.

### 7) Edge-Case Coverage
- [x] Empty/isolated/cyclic graph behavior verified.
- [x] Duplicate edge behavior explicitly defined and tested.
- [x] Subgraph mapping error paths covered.
- Owner: zhiwei
- Evidence: `tests/pipeline_builder_tests.cpp` (`AllowsCyclesInGraphModel`, `BuildRuntimeInjectsDeclaredPortsWithoutChannels`, `AllowsDuplicateSameEndpointEdges`, `BindSubgraphRejectsMissingRequiredMapping`).
- Acceptance:
  - Contract-level edge cases have passing tests and documented behavior.

### 8) Observability Minimum
- [x] Ensure key logs include pipeline/node/edge context.
- [x] Ensure failure logs include stop reason and exception summary.
- Owner: zhiwei
- Evidence: `src/pipeline/node.cpp` (unified failure log fields), `src/pipeline/subgraph_node.cpp` (bridge failure log fields), `src/pipeline/pipeline_builder.cpp` (`build_runtime()` injects pipeline name), `tests/node_tests.cpp` (`FailureLogsIncludeObservabilityContext`), `tests/pipeline_builder_tests.cpp` (`SubgraphBridgeFailurePropagatesToRuntimeJoin` log assertions).
- Acceptance:
  - One failure log is enough to identify failing component and reason.

### 9) Documentation Minimum
- [x] Quickstart validates against current API.
- [x] Subgraph mapping usage is documented with a runnable snippet.
- [x] Troubleshooting section includes common runtime/bridge errors.
- Owner: zhiwei
- Evidence: `docs/quickstart.md` updated with subgraph mapping pattern and troubleshooting section.
- Acceptance:
  - New user can run first example without source code diving.

### 10) Release Process and Versioning
- [x] Changelog template finalized.
- [x] SemVer policy documented.
- [x] Release steps repeatable from clean checkout.
- Owner: zhiwei
- Evidence: `CHANGELOG.md` template and `docs/release-process.md` dry-run release steps.
- Acceptance:
  - One dry-run release succeeds end-to-end.

## Suggested Execution Order

1. API Freeze
2. Bridge Type Safety
3. Unified Error Propagation
4. Lifecycle Validation
5. Regression Baseline
6. Stress/Soak
7. Edge Cases
8. Observability
9. Documentation
10. Release Dry Run

## Release Gate

- P0 items complete: required
- P1 items complete: strongly recommended for external release
- Final decision owner:
- Final release tag:
