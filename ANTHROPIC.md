# Anthropic Tokio Fork

This is Anthropic's fork of [tokio](https://github.com/tokio-rs/tokio), published to our internal Artifactory registry as `crates-internal`.

## Version Convention

Versions use the upstream major.minor with a fork-owned patch number `P = N * 1000 + upstream_patch`, plus a constant `+anthropic` build-metadata tag:

- `1.49.1000+anthropic` = first fork release based on tokio 1.49.0
- `1.49.2000+anthropic` = second fork release
- `1.49.3001+anthropic` = third fork release, after rebasing onto tokio 1.49.1
- `1.52.7003+anthropic` = seventh fork release, after rebasing onto tokio 1.52.3

## Features Added

### `stall-detection`

Detects when a tokio worker thread is stalled (blocked in a task poll for too long) and reports diagnostics including stack traces via `tracing`.

```rust
let rt = tokio::runtime::Builder::new_multi_thread()
    .enable_stall_detection()
    .stall_detection_poll_interval(Duration::from_millis(100))
    .stall_detection_escalation_threshold(Duration::from_secs(10))
    .build()
    .unwrap();
```

## Publishing

Publishing happens automatically when changes are pushed to the `anthropic-1.52.3` branch. The GitHub Actions workflow uses OIDC authentication with Artifactory.

### Prerequisites

1. The `anthropics/tokio` repo must be added to the OIDC config in `anthropics/terraform-config`
2. A `publish-cli` GitHub environment must be configured in repo settings
3. The `jfrog/setup-jfrog-cli` action must be allowed in repo settings

## Using in the Monorepo

In the workspace `Cargo.toml`:

```toml
[patch.crates-io]
tokio = { version = "1.52.7003+anthropic", registry = "crates-internal" }
```
