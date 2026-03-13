# Anthropic Tokio Fork - Development Guide

## Version Bumping

Every PR merged to an `anthropic-*` branch MUST bump the `+anthropic.N` version suffix.
Increment `N` by 1 from whatever the current value is.

### Files to update

All publishable crates need matching version suffixes:

- `tokio/Cargo.toml` - main tokio crate
- `tokio-macros/Cargo.toml` - proc macros
- `tokio-stream/Cargo.toml` - stream utilities
- `tokio-test/Cargo.toml` - test utilities
- `tokio-util/Cargo.toml` - additional utilities

Example: if current versions end in `+anthropic.3`, update them all to `+anthropic.4`.

### Version format

`<upstream_version>+anthropic.<N>`

Examples:
- `1.49.0+anthropic.1` (first anthropic release based on tokio 1.49.0)
- `1.49.0+anthropic.2` (second anthropic release)

The `+anthropic.N` suffix is a semver build metadata tag — it does not affect dependency resolution but uniquely identifies our builds in Artifactory.

## Publishing

Publishing to the `crates-internal` Artifactory registry happens automatically via GitHub Actions when changes are pushed to an `anthropic-*` branch. See `.github/workflows/publish.yml`.

## Stall Detection Feature

The `stall-detection` feature is our primary addition. See `ANTHROPIC.md` for user-facing documentation.

Key implementation files:
- `tokio/src/runtime/stall_detection.rs` - monitor thread, signal handler, frame-pointer walker
- `tokio/src/runtime/scheduler/multi_thread/worker.rs` - generation counter increments
- `tokio/src/runtime/metrics/worker.rs` - WorkerMetrics fields
- `tokio/src/runtime/builder.rs` - builder API methods
