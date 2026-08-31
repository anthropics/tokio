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

`Builder::io_shards(n)` (or `TOKIO_IO_SHARDS=n`) gives a multi-thread runtime
`n` epoll/kqueue instances instead of one. Workers are split into `n`
contiguous groups, each parking on its own shard; sockets are placed by
`SO_INCOMING_CPU` mapped to the CPU's L3 domain (Linux; round-robin otherwise
or with `TOKIO_IO_SHARD_KEY=rr`). Before a worker parks, and at its
maintenance tick, it does a zero-timeout poll of the other shards
("help sweep"; `TOKIO_IO_SHARD_HELP=0` disables it) so readiness on a shard
whose group is busy is not stranded. Shards share one timer wheel; signals and
io_uring completions are serviced by shard 0. Default `1` is the stock
single-driver behaviour. Caveats: with the help sweep disabled a shard is
polled only by its own group, so a group whose workers never yield can delay
that shard's I/O; `io_shards > 1` is not meant to be combined with a paused
(`test-util`) clock.

Why: with one driver only one thread per runtime can be in `epoll_wait`, and on
runtimes with many workers readiness for every socket funnels through it. On a
180-worker TCP request/response benchmark `io_shards(8)` measured +26% req/s
at 64 connections (p50 −25%, +13% CPU per request), +39% at 2048 connections
(p50 −28%, +37% CPU per request), +11% with 64 KiB messages; no effect below
~32 workers. `TOKIO_IO_POLL_DEBUG=1` prints per-shard poll/event counters every
250 ms.

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
