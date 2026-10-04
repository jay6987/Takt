# Release Process

## Versioning Policy

Takt uses Semantic Versioning:

- `MAJOR`: incompatible API changes.
- `MINOR`: backward-compatible new features.
- `PATCH`: backward-compatible fixes.

Rules:

- Any public API rename/removal/signature break requires MAJOR bump.
- Additive APIs can be MINOR.
- Internal bug fixes with no API/behavior break are PATCH.

## Dry-Run Release Steps

1. Ensure release checklist is complete:
   - `docs/release-checklist.md`
2. Clean build and full tests:
   - `cmake -S . -B build -DTAKT_BUILD_EXAMPLES=ON -DTAKT_BUILD_TESTS=ON`
   - `cmake --build build -j4`
   - `ctest --test-dir build --output-on-failure`
3. Update `CHANGELOG.md`:
   - move key items from `Unreleased` into target version.
4. Tag candidate (dry run):
   - `git tag vX.Y.Z-rc1`
5. Validate checkout + build from tag:
   - `git checkout vX.Y.Z-rc1`
   - repeat build/test commands.
6. If green, create final tag:
   - `git tag vX.Y.Z`

## Release Artifact Expectations

- Source tree builds on supported toolchains.
- `tests/takt_smoke_test` passes all tests.
- `docs/quickstart.md` examples compile and run.
