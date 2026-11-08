# Contributing to Takt

Thank you for your interest in contributing.

## Getting Started

1. Fork the repository and create a feature branch.
2. Configure and build locally:
   - `cmake -S . -B build -DTAKT_BUILD_EXAMPLES=ON -DTAKT_BUILD_TESTS=ON`
   - `cmake --build build -j4`
3. Run tests:
   - `ctest --test-dir build --output-on-failure`

## Contribution Scope

Good contributions include:

- Bug fixes with regression tests.
- API-safe improvements with tests and docs.
- Documentation and examples that match current behavior.

## Code Quality Expectations

- Keep changes focused and minimal.
- Preserve backward compatibility unless intentionally planned.
- Follow formatting and linting configuration in:
  - `.clang-format`
  - `.clang-tidy`
  - `.editorconfig`

### Required quality gate

Use LLVM 23 (Clang, clang-format, llvm-cov, llvm-profdata and libclang) and
CMake >=3.25. Set `TAKT_LLVM_ROOT` to a nonstandard LLVM installation.

```sh
cmake -P scripts/quality.cmake
git config --local core.hooksPath .githooks
```

The gate runs independently of Lynx, on the current checkout, and fails rather
than skipping missing tools, invalid ASTs or missing coverage profiles. CI
runs the same entrypoint. Require the `quality` and platform build checks in
branch protection; local hooks alone can be bypassed.

All owned C++ files in `include`, `src`, `tests`, `examples` and `tools` must:

- Keep every physical line at most 80 columns, including comments and literals.
  Tabs count as four-column stops; UTF-8 bytes are counted conservatively.
- Start with a substantive English responsibility comment.
- Attach a substantive description to every function, including constructors,
  destructors, operators, helpers and deleted/defaulted members. A documented
  declaration covers its matching out-of-line definition. Lambdas are exempt.
- Document written GoogleTest cases, not framework-generated members.
- Put each declaration/definition parameter on its own physical line, even
  for short signatures. The first may follow the function name. Calls and
  nested function-pointer signatures are not subject to this extra rule.

The shared libclang checker validates all owned files against production,
example, compile-control and installed-consumer ASTs. Unreachable sources
fail the complete audit. Formatting is checked with LLVM 23, never silently
rewritten by the gate. Reviewers must still assess comment accuracy.

Production line coverage and branch coverage must **both be >=80%**, compared
using integer counts without rounding. Tests and tools are not production
coverage. Every production `.cpp` must appear in the report; difficult code
is not excluded. Takt and Lynx use the same coverage/contract implementations
and are evaluated separately, so neither can mask the other's low coverage.
Lynx retains its additional 400-line file and 60-line body limits; this change
does not impose those unrelated size limits on Takt.

Coverage, ASan+UBSan and TSan run in separate Debug build directories. Coverage
uses `-O1`, as in Lynx, while sanitizer builds retain normal Debug settings.
ASan/UBSan/TSan halt on errors; TSan is never combined with ASan. Standalone
configuration also exposes `TAKT_COVERAGE=ON` and
`TAKT_SANITIZER=address-undefined` or `thread`; production builds default to
uninstrumented. Package smoke tests build **and run** the installed consumer.

The pre-commit hook requires all tracked changes staged and no unrelated
untracked files, then checks whitespace and the full gate. Work and commits
inside the Takt checkout are independent of the parent Lynx repository.

## Pull Request Checklist

Before opening a PR, ensure:

1. The project builds cleanly.
2. Tests pass locally.
3. New behavior includes tests.
4. User-facing changes are documented in `README.md` and/or `docs/`.
5. Significant changes are recorded in `CHANGELOG.md` under `Unreleased`.

## Commit and PR Guidance

- Use clear commit messages.
- Describe motivation, design choices, and compatibility impact.
- Link related issues when applicable.

## Reporting Bugs

When filing an issue, include:

- OS and compiler/toolchain versions.
- Minimal reproduction steps.
- Expected and actual behavior.
- Relevant logs or stack traces.
