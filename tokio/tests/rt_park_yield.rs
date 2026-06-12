#![warn(rust_2018_idioms)]
#![cfg(feature = "full")]
#![cfg(not(miri))] // measures real elapsed time
#![cfg(not(target_os = "wasi"))] // Wasi doesn't support threads

use std::time::{Duration, Instant};

use tokio::runtime;

// Yield-parks -- the scheduler's periodic maintenance polls, which pass a zero
// park limit to the driver -- must stay non-blocking on a runtime whose clock
// is running, regardless of what deadlines the time driver has pending. If the
// driver turns them into real sleeps, every busy runtime is throttled to one
// maintenance park per millisecond.
//
// 61_000 yields cross the scheduler's maintenance interval (event_interval,
// default 61) about 1000 times. Non-blocking parks finish the whole loop in
// under ~300ms even on slow CI runners with this binary's sibling tests
// spinning concurrently; parks that sleep >= 1ms each need >= 1 second. The
// 500ms threshold separates the two in both directions.
async fn spin_yields() {
    for _ in 0..61_000 {
        tokio::task::yield_now().await;
    }
}

fn assert_yield_parks_nonblocking(rt: runtime::Runtime, with_pending_timer: bool) {
    rt.block_on(async {
        let far = if with_pending_timer {
            // A far-future timer: every park computes a future wake-up deadline.
            let jh = tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            });
            // Let the spawned task run once so its timer registers with the driver.
            tokio::task::yield_now().await;
            Some(jh)
        } else {
            // No timers at all: every park takes the no-deadline (limit-only) path.
            None
        };

        let start = Instant::now();
        tokio::spawn(spin_yields()).await.unwrap();
        let elapsed = start.elapsed();

        if let Some(far) = far {
            far.abort();
        }

        assert!(
            elapsed < Duration::from_millis(500),
            "yield-heavy workload took {elapsed:?}; maintenance parks appear to \
             be sleeping instead of polling"
        );
    });
}

#[test]
fn current_thread_yield_parks_nonblocking_with_pending_timer() {
    let rt = runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    assert_yield_parks_nonblocking(rt, true);
}

#[test]
fn current_thread_yield_parks_nonblocking_without_timers() {
    let rt = runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    assert_yield_parks_nonblocking(rt, false);
}

#[test]
fn multi_thread_yield_parks_nonblocking_with_pending_timer() {
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .unwrap();
    assert_yield_parks_nonblocking(rt, true);
}
