#![cfg(feature = "full")]
#![cfg(feature = "test-util")]

use std::future::{poll_fn, Future};
use std::task::Poll;
use std::time::Duration;
use tokio::time::{self, sleep, Instant};

// A timer extended (via the lock-free `Sleep::reset`-to-later fast path)
// while still sitting in an early wheel slot must not occlude another
// timer with an earlier deadline in a later slot. The auto-advance target
// must be a sound lower bound, never an overshoot.
#[tokio::test(start_paused = true)]
async fn extended_entry_does_not_overshoot_other_timer() {
    let start = Instant::now();

    // A in level-0 slot 10; B in level-1 slot 1.
    let a = sleep(Duration::from_nanos(10));
    tokio::pin!(a);
    let b = sleep(Duration::from_nanos(100));
    tokio::pin!(b);
    poll_fn(|cx| {
        let _ = a.as_mut().poll(cx);
        let _ = b.as_mut().poll(cx);
        Poll::Ready(())
    })
    .await;

    // Extend A: true_when -> 1000, registered_when stays 10 (still slot 10).
    a.as_mut().reset(start + Duration::from_nanos(1000));

    b.as_mut().await;
    assert_eq!(
        Instant::now(),
        start + Duration::from_nanos(100),
        "auto-advance overshot B's deadline"
    );

    // A subsequent quiesce step reports A's true deadline (re-keyed by the
    // auto-advance above), and never resolves before A is past the bound.
    let state = time::quiesce_until(start + Duration::from_nanos(500)).await;
    assert_eq!(state.now, start + Duration::from_nanos(500));
    assert_eq!(state.next_timer, Some(start + Duration::from_nanos(1000)));
}

// Same race observed via quiesce resolution alone: the step must not
// resolve while B (deadline 100) is at-or-below the bound (500).
#[tokio::test(start_paused = true)]
async fn extended_entry_does_not_resolve_quiesce_early() {
    let start = Instant::now();
    let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let a = sleep(Duration::from_nanos(10));
    tokio::pin!(a);
    poll_fn(|cx| {
        let _ = a.as_mut().poll(cx);
        Poll::Ready(())
    })
    .await;

    {
        let fired = fired.clone();
        tokio::spawn(async move {
            sleep(Duration::from_nanos(100)).await;
            fired.store(true, std::sync::atomic::Ordering::SeqCst);
        });
    }
    tokio::task::yield_now().await;

    a.as_mut().reset(start + Duration::from_nanos(1000));

    let state = time::quiesce_until(start + Duration::from_nanos(500)).await;

    // B (100ns) is within the 500ns bound: it MUST have fired, the clock
    // landed on the bound, and only A (1000ns) remains pending.
    assert!(
        fired.load(std::sync::atomic::Ordering::SeqCst),
        "step resolved before B fired"
    );
    assert_eq!(state.now, start + Duration::from_nanos(500));
    assert_eq!(state.next_timer, Some(start + Duration::from_nanos(1000)));
}
