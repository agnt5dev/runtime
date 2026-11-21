# Changelog

What changed in each release of the AGNT5 runtime. Versions follow
[Semantic Versioning](https://semver.org); before 1.0, a minor release can
change behavior or configuration, and its entry says how to upgrade.

## Unreleased

### Changed

- Agents declared `per="user"`, which keep one instance per user of an app,
  need agent instances, which this runtime doesn't have. A worker serving one
  is refused when it registers (`FAILED_PRECONDITION`, naming the agent)
  instead of its calls running as plain runs, and the PostgreSQL and SQLite
  stores refuse instance data.

## 0.1.0 - 2026-11-15

The first release.

### Added

- A single-process runtime for AGNT5 workers. It runs functions, workflows and
  agents durably: each step is recorded in a journal, a crashed worker's runs
  move to a healthy worker, and a sleep or a pause for a person's answer
  survives a restart.
- Storage on PostgreSQL, a local SQLite file, or Turso.
- `agnt5-runtime --version`, and the version in the startup log.
- The `ghcr.io/agnt5dev/agnt5-runtime` container image, published for each
  release.

### Known issues

- A run resumed from a durable timer stays leased to the worker that suspended
  it. If that worker crashes, a healthy worker takes the run only after the
  lease expires, about 60 seconds later.
