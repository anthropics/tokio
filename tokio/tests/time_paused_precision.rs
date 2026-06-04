//! Exact-deadline behavior of timers registered while the clock is paused.
//!
//! Timers registered under a paused clock fire at exact nanosecond
//! deadlines, auto-advance moves virtual time exactly onto those deadlines,
//! and same-deadline timers fire in registration order. All assertions are
//! exact `Instant`/`Duration` equality -- that exactness is the feature.

#![warn(rust_2018_idioms)]
#![cfg(feature = "full")]
#![cfg(not(miri))] // Whole-runtime tests; too slow on Miri.

use std::future::{pending, poll_fn, Future};
use std::task::Poll;
use std::time::Duration;
use tokio::time::{self, sleep, sleep_until, Instant, MissedTickBehavior};

#[tokio::test(start_paused = true)]
async fn sub_ms_sleep_is_exact() {
    let start = Instant::now();
    sleep(Duration::from_micros(100)).await;
    assert_eq!(start.elapsed(), Duration::from_micros(100));
}

#[tokio::test(start_paused = true)]
async fn sleep_until_now_elapses_zero() {
    // (a) Tick-aligned at runtime start: a due-now sleep completes with zero
    // elapsed virtual time (regression pin; the wheel already passes this
    // case via its synchronous elapsed fire).
    let start = Instant::now();
    sleep_until(Instant::now()).await;
    assert_eq!(start.elapsed(), Duration::ZERO);

    // (b) Mid-tick: park the paused clock between ms ticks first. The wheel
    // rounds a due-now deadline up to the next tick here; the exact store
    // must complete it with zero elapsed virtual time.
    time::advance(Duration::from_micros(300)).await;
    let start = Instant::now();
    sleep_until(Instant::now()).await;
    assert_eq!(start.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn sub_ms_distinctions_preserved() {
    let start = Instant::now();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    for (name, micros) in [("a", 500u64), ("b", 700u64)] {
        let tx = tx.clone();
        tokio::spawn(async move {
            sleep(Duration::from_micros(micros)).await;
            tx.send((name, Instant::now())).unwrap();
        });
    }
    drop(tx);

    let (first_name, first_at) = rx.recv().await.unwrap();
    let (second_name, second_at) = rx.recv().await.unwrap();
    assert!(rx.recv().await.is_none());

    assert_eq!(first_name, "a");
    assert_eq!(first_at, start + Duration::from_micros(500));
    assert_eq!(second_name, "b");
    assert_eq!(second_at, start + Duration::from_micros(700));
}

#[tokio::test(start_paused = true)]
async fn sub_ms_timeout_is_exact() {
    let start = Instant::now();
    let res = time::timeout(Duration::from_micros(750), pending::<()>()).await;
    assert!(res.is_err());
    assert_eq!(start.elapsed(), Duration::from_micros(750));
}

#[tokio::test(start_paused = true)]
async fn sub_ms_interval_ticks_exactly() {
    let start = Instant::now();
    let period = Duration::from_micros(250);
    let mut interval = time::interval_at(start + period, period);

    for i in 1..=4u32 {
        interval.tick().await;
        assert_eq!(start.elapsed(), period * i);
    }
}

// AC1.5's missed-tick clause: each `MissedTickBehavior` keeps its documented
// semantics at sub-ms periods. All three tests miss ticks the same way: tick
// once on schedule, then advance virtual time more than 5ms past the next
// scheduled tick (the strategies only engage when a tick is more than 5ms
// late) to an instant that is NOT a multiple of the period from start, so the
// three strategies' next-tick choices are mutually distinguishable by exact
// equality.

#[tokio::test(start_paused = true)]
async fn burst_semantics_at_sub_ms_period() {
    let period = Duration::from_micros(250);
    let start = Instant::now();
    let mut interval = time::interval_at(start + period, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Burst);

    assert_eq!(interval.tick().await, start + period);

    // Park virtual time at start + 6350us: ticks 2..=25 (500us..=6250us) are
    // now overdue.
    time::advance(Duration::from_micros(6100)).await;
    let missed_at = Instant::now();
    assert_eq!(missed_at, start + Duration::from_micros(6350));

    // Burst: every missed tick is delivered immediately, back-to-back, each
    // carrying its original scheduled instant, and the clock does not move.
    for k in 2..=25u32 {
        assert_eq!(interval.tick().await, start + period * k);
        assert_eq!(Instant::now(), missed_at);
    }

    // Caught up: the schedule continues on the original cadence.
    assert_eq!(interval.tick().await, start + period * 26);
    assert_eq!(Instant::now(), start + period * 26);
}

#[tokio::test(start_paused = true)]
async fn delay_semantics_at_sub_ms_period() {
    let period = Duration::from_micros(250);
    let start = Instant::now();
    let mut interval = time::interval_at(start + period, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    assert_eq!(interval.tick().await, start + period);

    time::advance(Duration::from_micros(6100)).await;
    let missed_at = Instant::now();
    assert_eq!(missed_at, start + Duration::from_micros(6350));

    // The overdue tick itself is delivered immediately and still carries its
    // original scheduled instant.
    assert_eq!(interval.tick().await, start + period * 2);
    assert_eq!(Instant::now(), missed_at);

    // Delay: the next tick comes exactly one period after the delayed poll,
    // abandoning the multiples-of-period-from-start cadence (missed_at is
    // deliberately not on that cadence).
    assert_eq!(interval.tick().await, missed_at + period);
    assert_eq!(Instant::now(), missed_at + period);

    // Subsequent ticks continue at one period from the delayed schedule.
    assert_eq!(interval.tick().await, missed_at + period * 2);
    assert_eq!(Instant::now(), missed_at + period * 2);
}

#[tokio::test(start_paused = true)]
async fn skip_semantics_at_sub_ms_period() {
    let period = Duration::from_micros(250);
    let start = Instant::now();
    let mut interval = time::interval_at(start + period, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    assert_eq!(interval.tick().await, start + period);

    time::advance(Duration::from_micros(6100)).await;
    let missed_at = Instant::now();
    assert_eq!(missed_at, start + Duration::from_micros(6350));

    // The single overdue tick is delivered immediately with its original
    // scheduled instant; the other missed ticks are skipped entirely.
    assert_eq!(interval.tick().await, start + period * 2);
    assert_eq!(Instant::now(), missed_at);

    // Skip: the next tick lands on the next upcoming multiple of the period
    // from start (26 * 250us = 6.5ms) -- not one period after the last
    // yielded tick (750us), and not one period after the poll (6.6ms).
    assert_eq!(interval.tick().await, start + period * 26);
    assert_eq!(Instant::now(), start + period * 26);

    // The cadence continues on the original multiples of the period.
    assert_eq!(interval.tick().await, start + period * 27);
    assert_eq!(Instant::now(), start + period * 27);
}

#[tokio::test(start_paused = true)]
async fn non_aligned_deadline_exact() {
    let start = Instant::now();
    sleep(Duration::from_nanos(1_000_001)).await;
    assert_eq!(start.elapsed(), Duration::from_nanos(1_000_001));
}

#[tokio::test(start_paused = true)]
async fn reset_later_and_earlier_exact() {
    // Reset to a LATER sub-ms deadline after the timer has registered: the
    // lock-free extend fast path, with the store lazily reinserting the entry
    // at its true deadline.
    let start = Instant::now();
    let s = sleep(Duration::from_micros(300));
    tokio::pin!(s);
    poll_fn(|cx| {
        assert!(s.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    s.as_mut().reset(start + Duration::from_micros(800));
    s.await;
    assert_eq!(start.elapsed(), Duration::from_micros(800));

    // Reset to an EARLIER deadline: the extend fast path refuses, so this is
    // a full re-register.
    let start = Instant::now();
    let s = sleep(Duration::from_micros(900));
    tokio::pin!(s);
    poll_fn(|cx| {
        assert!(s.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    s.as_mut().reset(start + Duration::from_micros(400));
    s.await;
    assert_eq!(start.elapsed(), Duration::from_micros(400));
}

#[tokio::test]
async fn pre_pause_timer_keeps_ms_rounding() {
    // Boundary of the guarantee: a timer registered BEFORE a mid-run pause()
    // stays wheel-resident and keeps today's ms-rounded firing; a timer
    // registered after the pause is exact.
    let t0 = Instant::now();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();

    let jh = tokio::spawn(async move {
        let s = sleep(Duration::from_micros(100));
        tokio::pin!(s);
        // Poll once so the timer registers against the still-running clock,
        // then signal the main task to pause.
        poll_fn(|cx| {
            let _ = s.as_mut().poll(cx);
            Poll::Ready(())
        })
        .await;
        started_tx.send(()).unwrap();
        s.await;
        t0.elapsed()
    });

    started_rx.await.unwrap();
    time::pause();

    // The real clock ran briefly before the pause, so exact equality is not
    // assertable; assert the today-envelope (deadline reached, ms-rounded
    // completion).
    let elapsed = jh.await.unwrap();
    assert!(elapsed >= Duration::from_micros(100), "{elapsed:?}");
    assert!(elapsed <= Duration::from_millis(2), "{elapsed:?}");

    // Registered after the pause: exact.
    let start = Instant::now();
    sleep(Duration::from_micros(100)).await;
    assert_eq!(start.elapsed(), Duration::from_micros(100));
}

#[tokio::test(start_paused = true)]
async fn clock_lands_exactly_on_deadline() {
    for dur in [
        Duration::from_micros(300),
        Duration::from_millis(1),
        Duration::from_micros(4_700),
    ] {
        let before = Instant::now();
        sleep(dur).await;
        assert_eq!(Instant::now(), before + dur);
    }
}

#[tokio::test(start_paused = true)]
async fn advance_then_sub_ms_sleep_no_overshoot() {
    let start = Instant::now();
    time::advance(Duration::from_micros(500)).await;
    assert_eq!(Instant::now(), start + Duration::from_micros(500));

    let before = Instant::now();
    sleep(Duration::from_micros(100)).await;
    assert_eq!(before.elapsed(), Duration::from_micros(100));
    assert_eq!(Instant::now(), start + Duration::from_micros(600));
}

#[tokio::test(start_paused = true)]
async fn advance_past_store_deadline_fires() {
    let jh = tokio::spawn(async {
        sleep(Duration::from_micros(300)).await;
    });
    // Let the spawned task run once so its timer registers in the store.
    tokio::task::yield_now().await;

    // An explicit advance() past the store deadline fires it on the next
    // runtime touch, without moving the clock again.
    time::advance(Duration::from_millis(1)).await;
    let now = Instant::now();

    jh.await.unwrap();
    assert_eq!(Instant::now(), now);
}

#[tokio::test(start_paused = true)]
async fn inhibits_still_hold_with_sub_ms_pending() {
    let start = Instant::now();
    let jh = tokio::spawn(async move {
        sleep(Duration::from_micros(100)).await;
        Instant::now()
    });
    tokio::task::yield_now().await;

    // An outstanding `spawn_blocking` task inhibits auto-advance; gate its
    // completion on a channel so the inhibit is deterministically held across
    // the yields below.
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let blocking = tokio::task::spawn_blocking(move || {
        release_rx.recv().unwrap();
    });
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    assert!(!jh.is_finished());
    assert_eq!(Instant::now(), start);

    release_tx.send(()).unwrap();
    blocking.await.unwrap();
    let fired_at = jh.await.unwrap();
    assert_eq!(fired_at, start + Duration::from_micros(100));
}

#[tokio::test(start_paused = true)]
async fn same_deadline_fires_in_registration_order() {
    // Completion order observed through the channel reflects waker order,
    // which the store's pop order determines on the current_thread runtime.
    let start = Instant::now();
    let deadline = start + Duration::from_micros(400);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    for name in ["a", "b", "c"] {
        let tx = tx.clone();
        tokio::spawn(async move {
            sleep_until(deadline).await;
            tx.send(name).unwrap();
        });
        // Run the task to its first poll so its timer registers before the
        // next task spawns -- the registration order under test.
        tokio::task::yield_now().await;
    }
    drop(tx);

    let mut order = Vec::new();
    while let Some(name) = rx.recv().await {
        order.push(name);
    }
    assert_eq!(order, ["a", "b", "c"]);
    assert_eq!(start.elapsed(), Duration::from_micros(400));
}

#[tokio::test(start_paused = true)]
async fn resume_with_pending_store_timer() {
    // A timer registered in the exact store while the clock is paused must
    // survive a mid-flight resume(): the now-running clock picks it up and
    // fires it at or after its deadline, never before.
    let start = Instant::now();
    let jh = tokio::spawn(async move {
        sleep(Duration::from_micros(300)).await;
        start.elapsed()
    });
    // Run the spawned task to its first poll so the timer registers in the
    // store while the clock is still paused.
    tokio::task::yield_now().await;

    time::resume();

    let elapsed = jh.await.unwrap();
    // Never early: exact assertion on the deadline half of the contract.
    assert!(elapsed >= Duration::from_micros(300), "{elapsed:?}");
    // The clock runs in real time after resume(), so lateness is up to
    // scheduling jitter; bound it generously rather than exactly.
    assert!(elapsed <= Duration::from_millis(250), "{elapsed:?}");
}
