#![warn(rust_2018_idioms)]
#![cfg(feature = "full")]
#![cfg(not(miri))] // Whole-runtime tests; too slow on miri.

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;

use tokio::time::{self, Duration, Instant};
use tokio_test::{assert_pending, task};

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_until_fires_timers_within_bound() {
    let start = Instant::now();
    let fired = Arc::new(AtomicUsize::new(0));

    for i in 1..=5u64 {
        let fired = fired.clone();
        tokio::spawn(async move {
            time::sleep(Duration::from_millis(i * 10)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    // Bound at 30ms: the 10/20/30ms timers fire (inclusive bound); 40/50ms do not.
    let state = time::quiesce_until(start + Duration::from_millis(30)).await;

    assert_eq!(fired.load(SeqCst), 3);
    // The last timer fires exactly at the bound; the clock lands on it.
    assert_eq!(state.now, start + Duration::from_millis(30));
    // The 40ms timer was registered while paused => store-resident => exact.
    assert_eq!(state.next_timer, Some(start + Duration::from_millis(40)));
    // The clock has not moved between resolution and return.
    assert_eq!(Instant::now(), state.now);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_until_drains_transitive_work() {
    let start = Instant::now();
    let result = Arc::new(AtomicUsize::new(0));

    let (tx, rx) = tokio::sync::oneshot::channel();

    {
        let result = result.clone();
        tokio::spawn(async move {
            // Woken by the channel send below (not by a timer).
            rx.await.unwrap();
            // Chain further: spawn another task and wait for it.
            let result2 = result.clone();
            tokio::spawn(async move {
                result2.store(42, SeqCst);
            })
            .await
            .unwrap();
        });
    }

    tokio::spawn(async move {
        time::sleep(Duration::from_millis(5)).await;
        tx.send(()).unwrap();
    });

    let state = time::quiesce_until(start + Duration::from_millis(10)).await;

    // The full chain (timer -> channel -> task -> spawned task) completed.
    assert_eq!(result.load(SeqCst), 42);
    // The 5ms timer fired on the way; the clock landed on the bound.
    assert_eq!(state.now, start + Duration::from_millis(10));
    assert_eq!(state.next_timer, None);
    assert_eq!(Instant::now(), state.now);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_until_no_timers_lands_on_bound() {
    let start = Instant::now();

    let state = time::quiesce_until(start + Duration::from_millis(100)).await;

    // Nothing fired, but the clock still lands exactly on the bound.
    assert_eq!(state.now, start + Duration::from_millis(100));
    assert_eq!(state.next_timer, None);
    assert_eq!(Instant::now(), start + Duration::from_millis(100));
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_until_bound_is_inclusive() {
    let start = Instant::now();
    let fired_at_bound = Arc::new(AtomicUsize::new(0));
    let fired_after_bound = Arc::new(AtomicUsize::new(0));

    {
        let fired_at_bound = fired_at_bound.clone();
        tokio::spawn(async move {
            time::sleep_until(start + Duration::from_millis(10)).await;
            fired_at_bound.fetch_add(1, SeqCst);
        });
    }
    {
        let fired_after_bound = fired_after_bound.clone();
        tokio::spawn(async move {
            time::sleep_until(start + Duration::from_millis(11)).await;
            fired_after_bound.fetch_add(1, SeqCst);
        });
    }

    let state = time::quiesce_until(start + Duration::from_millis(10)).await;

    assert_eq!(fired_at_bound.load(SeqCst), 1);
    assert_eq!(fired_after_bound.load(SeqCst), 0);
    assert_eq!(state.now, start + Duration::from_millis(10));
    assert_eq!(state.next_timer, Some(start + Duration::from_millis(11)));
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_unbounded_resolves_when_wheel_empties() {
    let start = Instant::now();
    let fired = Arc::new(AtomicUsize::new(0));

    for i in 1..=3u64 {
        let fired = fired.clone();
        tokio::spawn(async move {
            time::sleep(Duration::from_millis(i * 10)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    let state = time::quiesce().await;

    assert_eq!(fired.load(SeqCst), 3);
    assert_eq!(state.now, start + Duration::from_millis(30));
    assert_eq!(state.next_timer, None);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_unbounded_empty_wheel_does_not_hang() {
    let start = Instant::now();

    let state = time::quiesce().await;

    assert_eq!(state.now, start);
    assert_eq!(state.next_timer, None);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_next_timer_exact_for_far_store_timer() {
    let start = Instant::now();

    // A timer far in the future, registered while the clock is paused: it is
    // store-resident, so the reported next_timer is its exact deadline. (A
    // wheel-resident timer this far out reports a slot-aligned lower bound
    // instead; see wheel_resident_keeps_lower_bound.)
    tokio::spawn(async move {
        time::sleep_until(start + Duration::from_millis(10_000)).await;
    });

    let state = time::quiesce_until(start + Duration::from_millis(10)).await;

    // No timer fired; the clock landed on the bound.
    assert_eq!(state.now, start + Duration::from_millis(10));
    assert_eq!(
        state.next_timer,
        Some(start + Duration::from_millis(10_000))
    );
}

/// When the only pending timer is wheel-resident (registered before the clock was
/// paused), lives in an upper wheel level, and the quiesce bound reaches past that
/// timer's occupied-slot start, tokio's existing auto-advance moves the clock to
/// the slot start (a refinement hop) before the bound check can resolve the
/// waiter. The universal invariants still hold: now <= bound, next_timer is a
/// lower bound strictly after now, and Instant::now() == reported now. (A timer
/// registered while paused never hops: its exact deadline is visible to the
/// resolver immediately.)
#[cfg(feature = "test-util")]
#[tokio::test]
async fn quiesce_until_far_timer_refinement_hops() {
    let start = Instant::now();
    let deadline = start + Duration::from_millis(5_000);

    // Register while the clock is RUNNING so the timer lands in the timer wheel;
    // registered after the pause it would be store-resident and the step below
    // would resolve exactly, without any refinement hop.
    let mut sleep = task::spawn(time::sleep_until(deadline));
    assert_pending!(sleep.poll());

    time::pause();

    let bound = start + Duration::from_millis(4_500);
    let state = time::quiesce_until(bound).await;

    // The 5000ms timer did not fire.
    assert_pending!(sleep.poll());
    // The bound reaches past the start of the timer's occupied level-2 slot
    // (~4096ms), so the clock hopped there...
    assert!(state.now > start, "now: {:?}", state.now);
    // ...but `now` never exceeds the bound.
    assert!(state.now <= bound, "now: {:?}", state.now);
    // next_timer is a lower bound strictly after now, never later than the deadline.
    let next = state.next_timer.expect("timer still pending");
    assert!(next > state.now);
    assert!(next <= deadline);
    // The clock has not moved between resolution and return.
    assert_eq!(Instant::now(), state.now);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_until_bound_in_past_drains_without_advancing() {
    let start = Instant::now();

    // Move the clock forward 100ms first.
    time::advance(Duration::from_millis(100)).await;
    let now = Instant::now();
    assert_eq!(now, start + Duration::from_millis(100));

    // A pending timer in the future, registered while the clock is paused: it is
    // store-resident, so the reported next_timer is its exact deadline.
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(20)).await;
    });

    // Bound before the current instant: resolves once nothing is runnable, clock
    // unchanged, pending timer untouched.
    let state = time::quiesce_until(start).await;

    assert_eq!(state.now, now);
    assert_eq!(state.next_timer, Some(now + Duration::from_millis(20)));
    assert_eq!(Instant::now(), now);
}

#[cfg(feature = "test-util")]
#[test]
fn quiesce_as_block_on_root_future() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    // Read the virtual clock (requires the runtime context).
    let start = {
        let _enter = rt.enter();
        Instant::now()
    };

    let fired = Arc::new(AtomicUsize::new(0));
    {
        let fired = fired.clone();
        let _enter = rt.enter();
        rt.spawn(async move {
            time::sleep(Duration::from_millis(10)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    // NOTE: the Quiesce future is constructed HERE, outside any runtime context.
    // This must not panic: all validation happens on first poll, inside block_on.
    let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(20)));

    assert_eq!(fired.load(SeqCst), 1);
    assert_eq!(state.now, start + Duration::from_millis(20));
    assert_eq!(state.next_timer, None);
}

#[cfg(feature = "test-util")]
#[test]
fn quiesce_windowed_stepping() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let start = {
        let _enter = rt.enter();
        Instant::now()
    };

    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    {
        let log = log.clone();
        let _enter = rt.enter();
        rt.spawn(async move {
            for i in 1..=6u64 {
                time::sleep_until(start + Duration::from_millis(i * 5)).await;
                log.lock().unwrap().push(i);
            }
        });
    }

    // Step in 10ms windows: each window should run exactly two of the 5ms-spaced
    // events.
    let mut window_end = start;
    let mut reports = Vec::new();
    for _ in 0..3 {
        window_end += Duration::from_millis(10);
        let state = rt.block_on(time::quiesce_until(window_end));
        reports.push(state);
    }

    assert_eq!(*log.lock().unwrap(), vec![1, 2, 3, 4, 5, 6]);
    // Window reports: clock stops at the last event of each window.
    assert_eq!(reports[0].now, start + Duration::from_millis(10));
    assert_eq!(reports[1].now, start + Duration::from_millis(20));
    assert_eq!(reports[2].now, start + Duration::from_millis(30));
    assert_eq!(
        reports[0].next_timer,
        Some(start + Duration::from_millis(15))
    );
    assert_eq!(
        reports[1].next_timer,
        Some(start + Duration::from_millis(25))
    );
    assert_eq!(reports[2].next_timer, None);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_awaited_in_spawned_task() {
    let start = Instant::now();

    tokio::spawn(async move {
        time::sleep(Duration::from_millis(10)).await;
    });

    let waiter =
        tokio::spawn(async move { time::quiesce_until(start + Duration::from_millis(20)).await });

    let state = waiter.await.unwrap();
    assert_eq!(state.now, start + Duration::from_millis(20));
    assert_eq!(state.next_timer, None);
}

// ===== Drain-park hook re-check coverage =====
//
// The tests in this section exercise the scheduler hook's runnable-work re-check:
// work that becomes visible only at the drain park (IO readiness, cross-thread
// wakes, outstanding blocking tasks). The hook's zero-timeout driver poll consumes
// any pending wakeup, so discovering work there and parking anyway would hang the
// runtime forever.

/// IO readiness that arrives between the scheduler's last driver poll and the
/// drain park is surfaced by the hook's zero-timeout poll. The woken task must run
/// (the park must be skipped) instead of the runtime parking on a wakeup that the
/// poll just consumed.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_io_readiness_discovered_at_drain_park() {
    use std::io::Write;
    use tokio::io::AsyncReadExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    let (mut server, _) = listener.accept().await.unwrap();

    let read_task = tokio::spawn(async move {
        let mut buf = [0u8; 5];
        server.read_exact(&mut buf).await.unwrap();
        buf
    });
    // Let the read task register IO interest.
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    // Readiness arrives between the last driver poll and the drain park.
    client.write_all(b"hello").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Unbounded quiesce on an empty wheel: must NOT hang.
    let state = time::quiesce().await;
    assert!(state.next_timer.is_none());
    assert_eq!(&read_task.await.unwrap(), b"hello");
}

/// An outstanding `spawn_blocking` task inhibits quiesce resolution: the hook's
/// blocking-inhibit branch parks the runtime, and the blocking task's completion
/// (which releases the inhibit and then unparks the driver) lets the quiesce
/// resolve afterwards.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_waits_for_outstanding_blocking_task() {
    use std::sync::atomic::AtomicBool;

    let done = Arc::new(AtomicBool::new(false));

    let blocking = {
        let done = done.clone();
        tokio::task::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(200));
            done.store(true, SeqCst);
        })
    };

    // Unbounded quiesce: must not resolve while the blocking task is outstanding
    // (outstanding blocking work implies future wakes).
    let state = time::quiesce().await;

    // The store happens before the blocking task completes, which happens before
    // the inhibit release that allows the quiesce to resolve.
    assert!(
        done.load(SeqCst),
        "quiesce resolved while a blocking task was still outstanding"
    );
    assert!(state.next_timer.is_none());
    blocking.await.unwrap();
}

/// A cross-thread wake (a foreign thread completing a oneshot a spawned task is
/// awaiting) that lands during a quiesce step must run the woken task before the
/// quiesce resolves, and must never strand the runtime in a park whose wakeup was
/// already consumed.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_cross_thread_wake_during_step() {
    use std::sync::atomic::AtomicBool;

    let (tx, rx) = tokio::sync::oneshot::channel::<u32>();
    let received = Arc::new(AtomicBool::new(false));

    let task = {
        let received = received.clone();
        tokio::spawn(async move {
            let value = rx.await.unwrap();
            received.store(true, SeqCst);
            value
        })
    };

    // Let the task register with the channel.
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }

    // The blocking-pool thread is a foreign thread: the send wakes the channel task
    // through the cross-thread schedule path (inject queue push + driver unpark).
    // The send happens before the blocking task completes, so the quiesce (which
    // cannot resolve until the blocking task's inhibit is released) is still in
    // progress when the wake lands.
    let blocking = tokio::task::spawn_blocking(move || {
        tx.send(42).unwrap();
    });

    let state = time::quiesce().await;

    // The cross-thread woken task ran to completion before the quiesce resolved.
    assert!(
        received.load(SeqCst),
        "quiesce resolved before the cross-thread woken task ran"
    );
    assert!(state.next_timer.is_none());
    assert_eq!(task.await.unwrap(), 42);
    blocking.await.unwrap();
}

/// A `Quiesce` future first-polled from a thread that only holds a `Handle::enter`
/// guard must wake the target runtime: registration unparks the runtime's driver so
/// a parked runtime re-runs its drain-park hook and notices the new waiter. Without
/// the unpark, the waiter would only resolve when the runtime woke for some other
/// reason.
#[cfg(feature = "test-util")]
#[test]
fn quiesce_from_enter_guard_thread_wakes_parked_runtime() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let handle = rt.handle().clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
    let (root_tx, root_rx) = tokio::sync::oneshot::channel::<()>();

    // Park the runtime on its own thread, blocked on a future that resolves only
    // after the quiesce below completes.
    let rt_thread = std::thread::spawn(move || {
        rt.block_on(async move {
            ready_tx.send(()).unwrap();
            root_rx.await.unwrap();
        });
    });

    // Wait for the runtime to start, then give it time to reach its park. (If it has
    // not parked yet, the test still passes -- the hook sees the waiter on the way to
    // the park -- it just does not exercise the interesting interleaving.)
    ready_rx.recv().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));

    // From this thread, holding only an enter guard, an unbounded quiesce must
    // resolve: nothing else will wake the parked runtime.
    {
        let _enter = handle.enter();
        let state = futures::executor::block_on(time::quiesce());
        assert!(state.next_timer.is_none());
    }

    // Unblock the root future and shut down cleanly.
    root_tx.send(()).unwrap();
    rt_thread.join().unwrap();
}

// ===== Misuse panics =====

/// Awaiting `Quiesce` on a multi-thread runtime panics with a
/// message naming the current_thread requirement.
#[cfg(feature = "test-util")]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[should_panic(expected = "requires the `current_thread` Tokio runtime")]
async fn quiesce_multi_thread_panics() {
    let _ = time::quiesce().await;
}

/// Awaiting `Quiesce` on a runtime whose clock is not paused
/// panics with a message naming the paused-clock requirement.
#[cfg(feature = "test-util")]
#[tokio::test]
#[should_panic(expected = "requires the runtime's clock to be paused")]
async fn quiesce_unpaused_clock_panics() {
    // Clock not paused (no start_paused, no time::pause()).
    let _ = time::quiesce().await;
}

/// Polling `Quiesce` outside any runtime context panics with the
/// standard tokio context-missing message.
#[cfg(feature = "test-util")]
#[test]
#[should_panic(expected = "must be called from the context of a Tokio 1.x runtime")]
fn quiesce_poll_outside_runtime_panics() {
    // Construction is lazy and does NOT panic...
    let mut quiesce = task::spawn(time::quiesce());
    // ...polling outside a runtime does.
    let _ = quiesce.poll();
}

/// Awaiting `Quiesce` on a runtime built without a time driver
/// panics with the standard timers-disabled message.
#[cfg(feature = "test-util")]
#[test]
#[should_panic(expected = "timers are disabled")]
fn quiesce_without_time_driver_panics() {
    // No enable_time(): the runtime has a clock but no time driver.
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    rt.block_on(async {
        // Pause the clock first (works without a time driver) so the failure is
        // unambiguously about the missing driver, not the unpaused clock.
        time::pause();
        time::quiesce().await
    });
}

/// `resume()` while any quiesce waiter is registered panics
/// (stepping and a running wall clock are mutually exclusive).
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
#[should_panic(expected = "cannot be called while a `quiesce()` is in progress")]
async fn resume_during_quiesce_panics() {
    let start = Instant::now();

    // A spawned task holds an unbounded quiesce open (never resolves: the timer
    // below keeps the wheel non-empty).
    tokio::spawn(async {
        let _ = time::quiesce().await;
    });
    // A pending timer so the quiesce waiter cannot resolve.
    tokio::spawn(async move {
        time::sleep_until(start + Duration::from_millis(100)).await;
    });

    // Let the spawned tasks run (and the waiter register) by yielding a few times.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    time::resume();
}

/// `advance()` while any quiesce waiter is registered panics: an
/// explicit advance would move the clock past the step's bound and break
/// reproducibility.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
#[should_panic(expected = "cannot be called while a `quiesce()` is in progress")]
async fn advance_during_quiesce_panics() {
    let start = Instant::now();

    // A spawned task holds an unbounded quiesce open (never resolves: the timer
    // below keeps the wheel non-empty).
    tokio::spawn(async {
        let _ = time::quiesce().await;
    });
    // A pending timer so the quiesce waiter cannot resolve.
    tokio::spawn(async move {
        time::sleep_until(start + Duration::from_millis(100)).await;
    });

    // Let the spawned tasks run (and the waiter register) by yielding a few times.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    time::advance(Duration::from_millis(10)).await;
}

/// A `Quiesce` future that outlives its runtime panics with the standard
/// runtime-shutdown message when polled, not an internal registry error. The driver
/// drains the waiter registry at shutdown, so the waiter is gone by the time this
/// poll runs.
#[cfg(feature = "test-util")]
#[test]
#[should_panic(expected = "A Tokio 1.x context was found, but it is being shutdown.")]
fn quiesce_polled_after_shutdown_panics() {
    use futures::task::noop_waker_ref;
    use std::future::Future;
    use std::task::Context;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let mut quiesce = Box::pin(time::quiesce());

    // First poll registers the waiter with the runtime's time driver.
    {
        let _enter = rt.enter();
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(quiesce.as_mut().poll(&mut cx).is_pending());
    }

    // Shutting the runtime down drains the waiter registry.
    drop(rt);

    // The orphaned future must report the shutdown when polled again.
    let mut cx = Context::from_waker(noop_waker_ref());
    let _ = quiesce.as_mut().poll(&mut cx);
}

/// A `Quiesce` future first-polled after its runtime has already shut down panics
/// with the standard runtime-shutdown message. Registration checks for shutdown
/// under the waiter-registry lock, so a waiter can never land on a dead driver --
/// nothing would ever wake it, and an awaiting caller would hang forever.
#[cfg(feature = "test-util")]
#[test]
#[should_panic(expected = "A Tokio 1.x context was found, but it is being shutdown.")]
fn quiesce_first_polled_after_shutdown_panics() {
    use futures::task::noop_waker_ref;
    use std::future::Future;
    use std::task::Context;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let handle = rt.handle().clone();

    // Shut the runtime down before the future is ever polled.
    drop(rt);

    // The first poll (registration) must observe the shutdown and panic; it must
    // not register a waiter on the dead driver and return `Pending`.
    let mut quiesce = Box::pin(time::quiesce());
    let _enter = handle.enter();
    let mut cx = Context::from_waker(noop_waker_ref());
    let _ = quiesce.as_mut().poll(&mut cx);
}

/// The first-poll-after-shutdown panic leaves no state behind: the refused
/// registration never bumps the waiter count that backs the `resume()`/`advance()`
/// mutual-exclusion check, so time APIs keep working both through the shut-down
/// runtime's still-live handle and on a fresh runtime.
#[cfg(feature = "test-util")]
#[test]
fn quiesce_first_poll_after_shutdown_does_not_poison_time_apis() {
    use futures::task::noop_waker_ref;
    use std::future::Future;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::task::Context;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let handle = rt.handle().clone();

    drop(rt);

    // The first poll panics: registration is refused on the shut-down driver.
    let mut quiesce = Box::pin(time::quiesce());
    let poll_result = catch_unwind(AssertUnwindSafe(|| {
        let _enter = handle.enter();
        let mut cx = Context::from_waker(noop_waker_ref());
        let _ = quiesce.as_mut().poll(&mut cx);
    }));
    assert!(poll_result.is_err(), "first poll after shutdown must panic");

    // No phantom waiter was left behind: `resume()` through the shut-down
    // runtime's handle works (it would panic with "cannot be called while a
    // `quiesce()` is in progress" if the refused registration had bumped the
    // waiter count).
    {
        let _enter = handle.enter();
        time::resume();
    }

    // Dropping the never-registered future is a no-op as well.
    drop(quiesce);

    // Time APIs on a fresh runtime are unaffected.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    rt.block_on(async {
        tokio::spawn(async {
            time::sleep(Duration::from_millis(1)).await;
        });
        let state = time::quiesce().await;
        assert!(state.next_timer.is_none());
    });
}

/// Driver shutdown drains the quiesce waiter registry; it must also reset the
/// waiter count that backs the `resume()`/`advance()` mutual-exclusion check. A
/// stale count would make those APIs, called through a still-live `Handle` of the
/// shut-down runtime, panic with a misleading
/// "cannot be called while a `quiesce()` is in progress" message.
#[cfg(feature = "test-util")]
#[test]
fn shutdown_with_quiesce_waiter_does_not_poison_time_apis() {
    use futures::task::noop_waker_ref;
    use std::future::Future;
    use std::task::Context;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let handle = rt.handle().clone();

    let mut quiesce = Box::pin(time::quiesce());

    // First poll registers the waiter with the runtime's time driver.
    {
        let _enter = rt.enter();
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(quiesce.as_mut().poll(&mut cx).is_pending());
    }

    // Shut the runtime down with the waiter still registered: the driver drains
    // the registry wholesale.
    drop(rt);

    // Dropping the orphaned future cannot affect the count: its registry entry is
    // already gone, so deregistration is a no-op.
    drop(quiesce);

    // The shut-down runtime's time driver must not report phantom waiters.
    // `resume()` consults the waiter count through the current context's handle;
    // with a correctly reset count it proceeds (and succeeds, since the clock is
    // still paused).
    let _enter = handle.enter();
    time::resume();
}

// ===== Interaction semantics =====

/// An outstanding `spawn_blocking` task defers quiesce resolution;
/// the step returns only after the blocking task completed AND the async task
/// awaiting it has been polled (its completion processed).
///
/// Complements `quiesce_waits_for_outstanding_blocking_task` (unbounded quiesce
/// observing the blocking closure's own side effect): this test uses a bounded step
/// and observes the completion processing of an async task awaiting the blocking
/// task's `JoinHandle`, plus that the only clock movement is the final landing
/// on the bound.
#[cfg(feature = "test-util")]
#[test]
fn quiesce_until_processes_blocking_task_completion() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let start = {
        let _enter = rt.enter();
        Instant::now()
    };

    let completion_processed = Arc::new(AtomicUsize::new(0));
    {
        let completion_processed = completion_processed.clone();
        let _enter = rt.enter();
        rt.spawn(async move {
            tokio::task::spawn_blocking(|| {
                std::thread::sleep(Duration::from_millis(100));
            })
            .await
            .unwrap();
            // This line is "the completion has been processed".
            completion_processed.fetch_add(1, SeqCst);
        });
    }

    let wall_start = std::time::Instant::now();
    let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(10)));
    let elapsed = wall_start.elapsed();

    // The step waited for the blocking task (~100ms wall time) and the awaiting
    // task ran to completion before resolution.
    assert_eq!(completion_processed.load(SeqCst), 1);
    assert!(
        elapsed >= Duration::from_millis(100),
        "elapsed: {elapsed:?}"
    );
    // No timer was involved; the clock landed on the bound.
    assert_eq!(state.now, start + Duration::from_millis(10));
    assert_eq!(state.next_timer, None);
}

/// An outstanding `spawn_blocking` task with a timer at-or-below the bound makes
/// the step wait in real time; the blocking task's completion (inhibit release +
/// unpark, from the pool thread) lets the in-progress step complete (timer fires,
/// work runs, then resolution) without restarting it.
#[cfg(feature = "test-util")]
#[test]
fn quiesce_waits_for_blocking_when_timer_within_bound() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let start = {
        let _enter = rt.enter();
        Instant::now()
    };

    let blocking = {
        let _enter = rt.enter();
        tokio::task::spawn_blocking(|| {
            std::thread::sleep(std::time::Duration::from_millis(100));
        })
    };

    let fired = Arc::new(AtomicUsize::new(0));
    {
        let fired = fired.clone();
        let _enter = rt.enter();
        rt.spawn(async move {
            time::sleep_until(start + Duration::from_millis(5)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    let wall_start = std::time::Instant::now();

    // Timer at 5ms <= bound 10ms: cannot resolve while the outstanding blocking
    // task inhibits the auto-advance needed to fire it.
    let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(10)));
    let elapsed = wall_start.elapsed();

    // The inhibit was honored: the step did not complete before the release...
    assert!(
        elapsed >= Duration::from_millis(100),
        "elapsed: {elapsed:?}"
    );
    // ...and the release completed the in-progress step promptly.
    assert!(elapsed < Duration::from_secs(10), "elapsed: {elapsed:?}");
    // The timer fired (after the release) and its work ran before resolution.
    assert_eq!(fired.load(SeqCst), 1);
    assert_eq!(state.now, start + Duration::from_millis(10));
    assert_eq!(state.next_timer, None);
    rt.block_on(blocking).unwrap();
}

/// Only one quiesce step may be in progress at a time: resolution lands the
/// clock on the step's bound, and the clock can only land on one bound.
/// Registering a second step while one is in progress panics.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
#[should_panic(expected = "cannot be called while another quiesce step is in progress")]
async fn second_concurrent_quiesce_panics() {
    let start = Instant::now();

    // First step, in a spawned task. It stays in progress while this (root) task
    // keeps the runtime busy below: a step only resolves at a drain-park, and the
    // runtime never goes idle between the yields and the second registration.
    tokio::spawn(async move { time::quiesce_until(start + Duration::from_millis(10)).await });

    // Let the first waiter register (first poll of its future).
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }

    // Second registration while the first is in progress: panics at first poll.
    let _ = time::quiesce_until(start + Duration::from_millis(20)).await;
}

/// Steps run back-to-back compose: a resolved step ends when its future resolves,
/// so a new step may begin immediately afterwards -- including from a different
/// task than the one that ran the previous step.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn sequential_steps_from_different_tasks() {
    let start = Instant::now();

    tokio::spawn(async move {
        time::sleep_until(start + Duration::from_millis(5)).await;
    });

    // Step 1 in a spawned task.
    let first = tokio::spawn(async move {
        time::quiesce_until(start + Duration::from_millis(10)).await
    });
    let ra = first.await.unwrap();
    assert_eq!(ra.now, start + Duration::from_millis(10));
    assert_eq!(ra.next_timer, None);

    // Step 2 from this task, immediately after: the previous step is over.
    let rb = time::quiesce_until(start + Duration::from_millis(30)).await;
    assert_eq!(rb.now, start + Duration::from_millis(30));
    assert_eq!(rb.next_timer, None);
    assert_eq!(Instant::now(), start + Duration::from_millis(30));
}

/// Dropping an unresolved `Quiesce` deregisters its waiter, and
/// subsequent auto-advance behavior is unchanged.
///
/// Deregistration is observed two ways: (1) `advance()` works again (it panics while
/// any waiter is registered), and (2) ordinary auto-advance still fires timers.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn dropping_unresolved_quiesce_deregisters() {
    let start = Instant::now();

    // A pending timer that keeps the wheel non-empty (so the waiter cannot resolve).
    tokio::spawn(async move {
        time::sleep_until(start + Duration::from_millis(100)).await;
    });
    // Make sure the spawned task has registered its timer.
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }

    {
        // Manually poll a quiesce future so it registers, then drop it unresolved.
        let mut quiesce = task::spawn(time::quiesce_until(start + Duration::from_millis(200)));
        assert_pending!(quiesce.poll());
    } // <- dropped here; the waiter must be deregistered

    // (1) If a waiter were still registered, this would panic
    //     ("cannot be called while a `quiesce()` is in progress").
    time::advance(Duration::from_millis(1)).await;

    // (2) The dropped step is over: a new step may begin.
    let state = time::quiesce_until(start + Duration::from_millis(50)).await;
    assert_eq!(state.now, start + Duration::from_millis(50));

    // (3) Normal auto-advance still works: the 100ms timer fires by sleeping to it.
    time::sleep_until(start + Duration::from_millis(100)).await;
    assert_eq!(Instant::now(), start + Duration::from_millis(100));
}

/// Many paused runtimes in one process step independently, driven
/// concurrently from different controller threads, without affecting each other's
/// clocks or reports.
#[cfg(feature = "test-util")]
#[test]
fn many_runtimes_step_independently_from_threads() {
    // Each "island" gets its own timer cadence; each is stepped by its own thread.
    // The barrier makes all islands start their stepping loops together, so the
    // steps genuinely overlap rather than potentially running one island at a time.
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let mut threads = Vec::new();

    for island in 1..=4u64 {
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap();

            let start = {
                let _enter = rt.enter();
                Instant::now()
            };

            let log = Arc::new(std::sync::Mutex::new(Vec::new()));
            {
                let log = log.clone();
                let _enter = rt.enter();
                // Island i fires events every i*10 ms.
                rt.spawn(async move {
                    for n in 1..=4u64 {
                        time::sleep_until(start + Duration::from_millis(n * island * 10)).await;
                        log.lock().unwrap().push(n * island * 10);
                    }
                });
            }

            // Wait for every island to finish its setup before stepping.
            barrier.wait();

            // Step in 4 windows of island*10 ms each: exactly one event per window.
            let mut nows = Vec::new();
            for w in 1..=4u64 {
                let state = rt.block_on(time::quiesce_until(
                    start + Duration::from_millis(w * island * 10),
                ));
                // Report positions are island-local virtual offsets.
                nows.push(state.now - start);
            }

            let events = log.lock().unwrap().clone();
            (island, events, nows)
        }));
    }

    for th in threads {
        let (island, log, nows) = th.join().unwrap();
        // Each island saw exactly its own cadence, unaffected by the other islands
        // stepping concurrently in the same process.
        let expected_log: Vec<u64> = (1..=4).map(|n| n * island * 10).collect();
        assert_eq!(log, expected_log, "island {island}");
        let expected_nows: Vec<Duration> = (1..=4)
            .map(|n| Duration::from_millis(n * island * 10))
            .collect();
        assert_eq!(nows, expected_nows, "island {island}");
    }
}

/// The API behaves identically on `LocalRuntime`, including with
/// !Send tasks spawned via `spawn_local`.
#[cfg(feature = "test-util")]
#[test]
fn quiesce_on_local_runtime() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build_local(tokio::runtime::LocalOptions::default())
        .unwrap();

    let start = {
        let _enter = rt.enter();
        Instant::now()
    };

    let fired = Arc::new(AtomicUsize::new(0));
    {
        let fired = fired.clone();
        let _enter = rt.enter();
        rt.spawn_local(async move {
            // A !Send value held across an await proves this is really a local task.
            let rc = std::rc::Rc::new(1u64);
            time::sleep_until(start + Duration::from_millis(10)).await;
            fired.fetch_add(*rc as usize, SeqCst);
        });
    }

    let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(20)));

    assert_eq!(fired.load(SeqCst), 1);
    assert_eq!(state.now, start + Duration::from_millis(20));
    assert_eq!(state.next_timer, None);
    assert_eq!(
        {
            let _enter = rt.enter();
            Instant::now()
        },
        state.now
    );
}

/// EXPLORATORY (non-contractual): `Quiesce` awaited inside `LocalSet::run_until`.
///
/// `LocalSet` is explicitly out of scope for the quiesce contract; this test
/// documents observed behavior rather than a guarantee. If it fails after a tokio
/// upgrade, re-evaluate rather than treating it as a regression of the quiesce
/// contract.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_inside_local_set_run_until_exploratory() {
    let start = Instant::now();
    let local = tokio::task::LocalSet::new();

    let fired = Arc::new(AtomicUsize::new(0));
    {
        let fired = fired.clone();
        local.spawn_local(async move {
            time::sleep_until(start + Duration::from_millis(10)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    let state = local
        .run_until(time::quiesce_until(start + Duration::from_millis(20)))
        .await;

    // Observed behavior: LocalSet's self-waking design composes with quiesce; the
    // local task's timer fires and the step resolves after it.
    assert_eq!(fired.load(SeqCst), 1);
    assert_eq!(state.now, start + Duration::from_millis(20));
}

// ===== Determinism =====

/// A windowed stepping loop with messages injected between windows
/// produces identical `QuiescedState` reports and an identical application event log
/// on every run with the same inputs.
///
/// The workload: a "server" task that receives messages and echoes a timestamped
/// event after a per-message delay, plus a periodic "ticker". All timer durations
/// derive deterministically from the message contents.
#[cfg(feature = "test-util")]
#[test]
fn windowed_stepping_is_deterministic() {
    // Reports and event logs use Durations relative to the runtime's own start so
    // they are comparable across runs (absolute Instants differ between runtimes).
    type Report = (Duration, Option<Duration>);
    type EventLog = Vec<(Duration, String)>;

    fn run_world() -> (Vec<Report>, EventLog) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();

        let start = {
            let _enter = rt.enter();
            Instant::now()
        };

        let log: Arc<std::sync::Mutex<EventLog>> = Arc::new(std::sync::Mutex::new(Vec::new()));

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        // Server: each received message schedules an echo event after a delay
        // derived from the message length.
        {
            let log = log.clone();
            let _enter = rt.enter();
            rt.spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let delay = Duration::from_millis(1 + (msg.len() as u64 % 5));
                    time::sleep(delay).await;
                    let now = Instant::now();
                    log.lock()
                        .unwrap()
                        .push((now - start, format!("echo:{msg}")));
                }
            });
        }

        // Ticker: an event every 3ms for the first 30ms.
        {
            let log = log.clone();
            let _enter = rt.enter();
            rt.spawn(async move {
                for i in 1..=10u64 {
                    time::sleep_until(start + Duration::from_millis(i * 3)).await;
                    let now = Instant::now();
                    log.lock().unwrap().push((now - start, format!("tick:{i}")));
                }
            });
        }

        // Controller loop: 8 windows of 5ms; inject one message per window.
        let mut reports = Vec::new();
        let mut window_end = start;
        for w in 0..8u64 {
            // Inject an external message between windows. Message lengths vary so
            // the per-message echo delays (derived from the length) span the whole
            // 1..=5ms range instead of collapsing to a single value, making echo
            // and tick events interleave differently from window to window.
            tx.send(format!("msg-{w}-{}", "x".repeat(w as usize)))
                .unwrap();

            window_end += Duration::from_millis(5);
            let state = rt.block_on(time::quiesce_until(window_end));
            reports.push((state.now - start, state.next_timer.map(|t| t - start)));
        }
        drop(tx);

        let events = log.lock().unwrap().clone();
        (reports, events)
    }

    let (reports_1, log_1) = run_world();
    let (reports_2, log_2) = run_world();

    assert_eq!(
        reports_1, reports_2,
        "QuiescedState report sequences differ between runs"
    );
    assert_eq!(log_1, log_2, "application event logs differ between runs");

    // Sanity: the workload actually did something.
    assert!(!log_1.is_empty());
    assert!(log_1.iter().any(|(_, e)| e.starts_with("echo:")));
    assert!(log_1.iter().any(|(_, e)| e.starts_with("tick:")));
}

// ===== Nanosecond precision =====

/// A sub-millisecond bound fires a timer at or below it.
/// The 300us timer (registered while paused, so exact) fires within a 500us bound,
/// and the clock lands exactly on the bound.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn sub_ms_bound_fires_timer_within_bound() {
    let start = Instant::now();
    let fired = Arc::new(AtomicUsize::new(0));

    {
        let fired = fired.clone();
        tokio::spawn(async move {
            time::sleep_until(start + Duration::from_micros(300)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    let state = time::quiesce_until(start + Duration::from_micros(500)).await;

    assert_eq!(fired.load(SeqCst), 1);
    assert_eq!(state.now, start + Duration::from_micros(500));
    assert_eq!(state.next_timer, None);
    assert_eq!(Instant::now(), state.now);
}

/// A timer strictly beyond a sub-millisecond bound does
/// not fire; the clock lands on the bound, and `next_timer` reports the
/// store-resident timer's deadline exactly.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn sub_ms_bound_leaves_later_timer_pending() {
    let start = Instant::now();
    let fired = Arc::new(AtomicUsize::new(0));

    {
        let fired = fired.clone();
        tokio::spawn(async move {
            time::sleep_until(start + Duration::from_micros(700)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    let state = time::quiesce_until(start + Duration::from_micros(500)).await;

    assert_eq!(fired.load(SeqCst), 0);
    assert_eq!(state.now, start + Duration::from_micros(500));
    assert_eq!(state.next_timer, Some(start + Duration::from_micros(700)));
    assert_eq!(Instant::now(), start + Duration::from_micros(500));
}

/// The bound is inclusive at nanosecond precision -- a
/// timer with a deadline exactly equal to the bound fires within the step.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn sub_ms_bound_inclusive_at_ns() {
    let start = Instant::now();
    let fired = Arc::new(AtomicUsize::new(0));

    {
        let fired = fired.clone();
        tokio::spawn(async move {
            time::sleep_until(start + Duration::from_micros(500)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    let state = time::quiesce_until(start + Duration::from_micros(500)).await;

    assert_eq!(fired.load(SeqCst), 1);
    assert_eq!(state.now, start + Duration::from_micros(500));
    assert_eq!(state.next_timer, None);
    assert_eq!(Instant::now(), state.now);
}

/// `next_timer` is the exact deadline of the earliest
/// store-resident timer, not a millisecond-aligned lower bound.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn next_timer_exact_for_store_timers() {
    let start = Instant::now();

    tokio::spawn(async move {
        time::sleep_until(start + Duration::from_micros(1300)).await;
    });

    let state = time::quiesce_until(start + Duration::from_millis(1)).await;

    assert_eq!(state.now, start + Duration::from_millis(1));
    assert_eq!(state.next_timer, Some(start + Duration::from_micros(1300)));
    assert_eq!(Instant::now(), start + Duration::from_millis(1));
}

/// With a wheel-resident (registered before the pause)
/// timer as the earliest pending, `next_timer` keeps the documented slot-aligned
/// lower-bound contract: at or before the true deadline, strictly after `now`.
#[cfg(feature = "test-util")]
#[tokio::test]
async fn wheel_resident_keeps_lower_bound() {
    let t0 = Instant::now();
    // 1000ms out: above the wheel's bottom level (64 one-millisecond slots), so the
    // wheel's reported expiration is the start of the occupied level-1 slot (960ms)
    // -- genuinely below the deadline. Far enough out that real-time scheduling
    // stalls between `t0` and the pause below cannot push `now` past that slot
    // start, which would break the `next > state.now` assertion.
    let deadline = t0 + Duration::from_millis(1000);

    // Register while the clock is RUNNING: the first poll places the timer in the
    // wheel, where it stays after the pause below.
    let mut sleep = task::spawn(time::sleep_until(deadline));
    assert_pending!(sleep.poll());

    time::pause();

    let state = time::quiesce_until(t0 + Duration::from_millis(10)).await;

    // The timer did not fire...
    assert_pending!(sleep.poll());
    // ...and next_timer is the documented lower bound: at or before the true
    // deadline, strictly after `now`.
    let next = state.next_timer.expect("wheel timer still pending");
    assert!(next <= deadline, "next_timer: {next:?}");
    assert!(next > state.now, "next_timer: {next:?}");
}

/// A windowed stepping run mixing sub-
/// millisecond and whole-millisecond timers, executed twice in-process, produces
/// identical `QuiescedState` reports and an identical application event log.
///
/// Clone of `windowed_stepping_is_deterministic` with sub-millisecond echo delays
/// and stepping windows that are not millisecond-aligned.
#[cfg(feature = "test-util")]
#[test]
fn run_twice_determinism_with_sub_ms() {
    type Report = (Duration, Option<Duration>);
    type EventLog = Vec<(Duration, String)>;

    fn run_world() -> (Vec<Report>, EventLog) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();

        let start = {
            let _enter = rt.enter();
            Instant::now()
        };

        let log: Arc<std::sync::Mutex<EventLog>> = Arc::new(std::sync::Mutex::new(Vec::new()));

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        // Server: each received message schedules an echo event after a
        // sub-millisecond delay derived from the message length.
        {
            let log = log.clone();
            let _enter = rt.enter();
            rt.spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let delay = Duration::from_micros(150 + 175 * (msg.len() as u64 % 5));
                    time::sleep(delay).await;
                    let now = Instant::now();
                    log.lock()
                        .unwrap()
                        .push((now - start, format!("echo:{msg}")));
                }
            });
        }

        // Ticker: a whole-millisecond event every 1ms for the first 10ms.
        {
            let log = log.clone();
            let _enter = rt.enter();
            rt.spawn(async move {
                for i in 1..=10u64 {
                    time::sleep_until(start + Duration::from_millis(i)).await;
                    let now = Instant::now();
                    log.lock().unwrap().push((now - start, format!("tick:{i}")));
                }
            });
        }

        // Controller loop: 8 windows of 1250us (not millisecond-aligned); inject
        // one message per window.
        let mut reports = Vec::new();
        let mut window_end = start;
        for w in 0..8u64 {
            // Message lengths vary so the per-message echo delays span the whole
            // 150..=850us range, interleaving differently with the whole-ms ticks
            // from window to window.
            tx.send(format!("msg-{w}-{}", "x".repeat(w as usize)))
                .unwrap();

            window_end += Duration::from_micros(1250);
            let state = rt.block_on(time::quiesce_until(window_end));
            reports.push((state.now - start, state.next_timer.map(|t| t - start)));
        }
        drop(tx);

        let events = log.lock().unwrap().clone();
        (reports, events)
    }

    let (reports_1, log_1) = run_world();
    let (reports_2, log_2) = run_world();

    assert_eq!(
        reports_1, reports_2,
        "QuiescedState report sequences differ between runs"
    );
    assert_eq!(log_1, log_2, "application event logs differ between runs");

    // Sanity: the workload mixed sub-millisecond and whole-millisecond events.
    assert!(log_1.iter().any(|(_, e)| e.starts_with("echo:")));
    assert!(log_1.iter().any(|(_, e)| e.starts_with("tick:")));
    assert!(log_1
        .iter()
        .any(|(at, _)| at.subsec_nanos() % 1_000_000 != 0));
}

// A quiesce_until step must never move the clock past its bound, even when a
// timer registered before a mid-run pause() lives in an upper wheel level and
// the paused clock sits at a fractional-millisecond position. The wheel hop
// must land exactly on the tick boundary; a hop computed in whole milliseconds
// from the fractional position overshoots it past the bound.
#[cfg(feature = "test-util")]
#[test]
fn quiesce_until_bound_holds_for_pre_pause_wheel_timer() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();

    rt.block_on(async {
        let start = Instant::now();

        // Registered while the clock is running: lands in the wheel, and the
        // 70ms deadline puts it in an upper level whose slot starts at 64ms.
        let wheel_sleep = tokio::spawn(async move {
            time::sleep_until(start + Duration::from_millis(70)).await;
        });
        // Let the spawned task run once so its timer registers.
        tokio::task::yield_now().await;

        time::pause();

        // Leave the paused clock at a fractional-millisecond position.
        time::sleep(Duration::from_micros(500)).await;

        // The bound lies between the wheel slot start (64ms) and the position
        // a whole-millisecond hop from the fractional clock position would
        // land at (~64.5ms).
        let bound = start + Duration::from_micros(64_400);
        let state = time::quiesce_until(bound).await;

        assert!(
            state.now <= bound,
            "quiesce_until resolved past its bound: now = {:?} > bound = {:?} (over by {:?})",
            state.now,
            bound,
            state.now - bound,
        );

        wheel_sleep.abort();
    });
}
