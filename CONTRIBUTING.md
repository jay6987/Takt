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
