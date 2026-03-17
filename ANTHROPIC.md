# Anthropic Tokio Fork

This is Anthropic's fork of [tokio](https://github.com/tokio-rs/tokio), published to our internal Artifactory registry as `crates-internal`.

## Version Convention

Versions follow the `+anthropic.N` suffix convention per `go/fork`:
- `1.49.0+anthropic.1` = tokio 1.49.0 + stall detection feature

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

Publishing happens automatically when changes are pushed to the `anthropic-1.49.0` branch. The GitHub Actions workflow uses OIDC authentication with Artifactory.

### Prerequisites

1. The `anthropics/tokio` repo must be added to the OIDC config in `anthropics/terraform-config`
2. A `publish-cli` GitHub environment must be configured in repo settings
3. The `jfrog/setup-jfrog-cli` action must be allowed in repo settings

## Using in the Monorepo

In the workspace `Cargo.toml`:

```toml
[patch.crates-io]
tokio = { version = "1.49.0+anthropic.1", registry = "crates-internal" }
```
