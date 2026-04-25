# Anthropic Tokio Fork - Development Guide

## Version Bumping

Every PR merged to an `anthropic-*` branch MUST bump the patch version in `tokio/Cargo.toml`.

### Files to update

- `tokio/Cargo.toml` - main tokio crate

The other workspace crates (`tokio-macros`, `tokio-stream`, `tokio-test`, `tokio-util`) are not published from this fork; the published `tokio` crate depends on them via crates.io.

### Version format

`<upstream_major>.<upstream_minor>.<P>+anthropic` where `P = N * 1000 + upstream_patch`

`N` is the anthropic release counter and never resets — it increments on every PR, including across rebases onto new upstream patch versions, so the version is always monotonic.

Examples based on upstream 1.49.0:
- `1.49.1000+anthropic` (first anthropic release)
- `1.49.2000+anthropic` (second anthropic release)

After rebasing onto upstream 1.49.1 (N continues):
- `1.49.3001+anthropic`
- `1.49.4001+anthropic`

The `+anthropic` suffix is a semver build metadata tag and does not affect dependency resolution.

## Publishing

Publishing to the `crates-internal` Artifactory registry happens automatically via GitHub Actions when changes are pushed to an `anthropic-*` branch. See `.github/workflows/publish.yml`.

## Stall Detection Feature

The `stall-detection` feature is our primary addition. See `ANTHROPIC.md` for user-facing documentation.

Key implementation files:
- `tokio/src/runtime/stall_detection.rs` - monitor thread, signal handler, frame-pointer walker
- `tokio/src/runtime/scheduler/multi_thread/worker.rs` - generation counter increments
- `tokio/src/runtime/metrics/worker.rs` - WorkerMetrics fields
- `tokio/src/runtime/builder.rs` - builder API methods
