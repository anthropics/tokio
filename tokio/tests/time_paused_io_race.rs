#![cfg(all(feature = "full", feature = "test-util", unix))]

//! Regression for the paused-clock auto-advance vs same-runtime IO interaction.
//!
//! Under `start_paused = true`, when the only pending timer is a `timeout`
//! wrapping an IO operation that a sibling task on the same runtime will
//! satisfy, auto-advance must let the sibling task run rather than jumping
//! the clock straight to the timeout's deadline.
//!
//! The auto-advance veto (`did_wake`) does not observe IO readiness delivered
//! during the zero-timeout poll: those wakes take the local-queue scheduling
//! path because the scheduler core is in-context for the duration of the
//! park. Upstream tolerates this because auto-advance targets the next
//! timer's wheel-slot start, so an upper-level timer cascades one level per
//! park iteration without firing, returning to the run loop between each --
//! and each return runs the IO-woken sibling. Advancing to the exact
//! deadline collapses that into one pass and fires the timer before the
//! sibling ever runs.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn paused_timeout_yields_to_same_runtime_io() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("s.sock");
    let listener = tokio::net::UnixListener::bind(&sock).unwrap();

    // Sibling server on the same runtime: accept one connection, echo a reply.
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 64];
        let _ = s.read(&mut buf).await;
        s.write_all(b"pong").await.unwrap();
    });

    let start = Instant::now();
    let timeout = Duration::from_secs(60);

    let got = tokio::time::timeout(timeout, async {
        let mut s = tokio::net::UnixStream::connect(&sock).await.unwrap();
        s.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        s.read_exact(&mut buf).await.unwrap();
        buf
    })
    .await;

    assert_eq!(
        got.ok(),
        Some(*b"pong"),
        "IO satisfied by a sibling task must complete before auto-advance \
         reaches the timeout deadline"
    );
    assert!(
        Instant::now() - start < timeout,
        "auto-advance reached the timeout deadline despite pending IO"
    );
}
