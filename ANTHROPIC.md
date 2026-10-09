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

### Deterministic time stepping (`test-util`)

Extensions to the paused-clock (`tokio::time::pause()`) machinery for
deterministic simulation and stepped testing, all gated on the `test-util`
feature:

- Timers created while the clock is paused fire at their exact nanosecond
  deadlines (instead of the timer wheel's millisecond rounding), with
  same-instant timers firing in registration order.
- `tokio::time::quiesce()` / `quiesce_until(deadline)`: run a paused
  `current_thread` runtime until nothing more can happen at or below the
  given virtual-time bound, land the clock exactly on the bound, and report
  when the next pending timer is due.

See the rustdoc on those functions for the full contracts.

### Sharded I/O driver (`io_shards`)

`Builder::io_shards(n)` gives a multi-thread runtime `n` epoll/kqueue
instances instead of one, each polled by its own group of workers. With one
instance, only one thread per runtime can be in `epoll_wait`, and every
`epoll_ctl` waits while a reader of `/proc/<pid>/fdinfo/<epfd>` (a host agent,
for example) walks the whole set under the epoll mutex. Shards divide both
costs by `n`. On a 180-worker TCP request/response benchmark, `io_shards(8)`
gave +20% req/s at 64 connections (+7% CPU per request); it had no effect
below about 32 workers. It does not change overload behaviour; that is
`max_io_events_per_busy_tick`.

`1` is the default. `TOKIO_IO_SHARDS=n` sets a process-wide default; because
it reaches every runtime in the process, it is lowered until each shard has at
least 8 workers. An explicit `io_shards(n)` is only clamped to the worker
count. Costs while idle: one wakeup per shard per
`Builder::io_shard_sweep_interval` (default 10 ms), and every timer deadline
wakes all `n` drivers. Not meant for use with a paused (`test-util`) clock.

See the rustdoc on `Builder::io_shards`, `io_shard_sweep_interval` and
`max_io_events_per_busy_tick` for the full contracts.

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
