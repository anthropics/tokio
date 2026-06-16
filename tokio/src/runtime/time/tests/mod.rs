#![cfg(not(target_os = "wasi"))]

use std::{task::Context, time::Duration};

#[cfg(not(loom))]
use futures::task::noop_waker_ref;

use crate::loom::sync::atomic::{AtomicBool, Ordering};
use crate::loom::sync::Arc;
use crate::loom::thread;

use super::TimerEntry;

fn block_on<T>(f: impl std::future::Future<Output = T>) -> T {
    #[cfg(loom)]
    return loom::future::block_on(f);

    #[cfg(not(loom))]
    {
        let rt = crate::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(f)
    }
}

fn model(f: impl Fn() + Send + Sync + 'static) {
    #[cfg(loom)]
    loom::model(f);

    #[cfg(not(loom))]
    f();
}

fn rt(start_paused: bool) -> crate::runtime::Runtime {
    crate::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(start_paused)
        .build()
        .unwrap()
}

// Loom models below anchor deadlines on the driver's start_time (not
// clock.now()): the wheel is keyed in nanoseconds, so a deadline taken from a
// real-clock read varies by a few hundred ns between loom executions, putting
// the entry in different wheel slots and making the model nondeterministic.

#[test]
fn single_timer() {
    model(|| {
        let rt = rt(false);
        let handle = rt.handle();
        let start = handle.inner.driver().time().time_source().start_time();

        let handle_ = handle.clone();
        let jh = thread::spawn(move || {
            let entry = TimerEntry::new(handle_.inner.clone(), start + Duration::from_secs(1));
            pin!(entry);

            block_on(std::future::poll_fn(|cx| entry.as_mut().poll_elapsed(cx))).unwrap();
        });

        thread::yield_now();

        // advance 2s
        handle.inner.driver().time().process_at_time(2_000_000_000);

        jh.join().unwrap();
    })
}

#[test]
fn drop_timer() {
    model(|| {
        let rt = rt(false);
        let handle = rt.handle();
        let start = handle.inner.driver().time().time_source().start_time();

        let handle_ = handle.clone();
        let jh = thread::spawn(move || {
            let entry = TimerEntry::new(handle_.inner.clone(), start + Duration::from_secs(1));
            pin!(entry);

            let _ = entry
                .as_mut()
                .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));
            let _ = entry
                .as_mut()
                .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));
        });

        thread::yield_now();

        // advance 2s in the future.
        handle.inner.driver().time().process_at_time(2_000_000_000);

        jh.join().unwrap();
    })
}

#[test]
fn change_waker() {
    model(|| {
        let rt = rt(false);
        let handle = rt.handle();
        let start = handle.inner.driver().time().time_source().start_time();

        let handle_ = handle.clone();
        let jh = thread::spawn(move || {
            let entry = TimerEntry::new(handle_.inner.clone(), start + Duration::from_secs(1));
            pin!(entry);

            let _ = entry
                .as_mut()
                .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));

            block_on(std::future::poll_fn(|cx| entry.as_mut().poll_elapsed(cx))).unwrap();
        });

        thread::yield_now();

        // advance 2s
        handle.inner.driver().time().process_at_time(2_000_000_000);

        jh.join().unwrap();
    })
}

#[test]
fn reset_future() {
    model(|| {
        let finished_early = Arc::new(AtomicBool::new(false));

        let rt = rt(false);
        let handle = rt.handle();

        let handle_ = handle.clone();
        let finished_early_ = finished_early.clone();
        let start = handle.inner.driver().time().time_source().start_time();

        let jh = thread::spawn(move || {
            let entry = TimerEntry::new(handle_.inner.clone(), start + Duration::from_secs(1));
            pin!(entry);

            let _ = entry
                .as_mut()
                .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));

            entry.as_mut().reset(start + Duration::from_secs(2), true);

            // shouldn't complete before 2s
            block_on(std::future::poll_fn(|cx| entry.as_mut().poll_elapsed(cx))).unwrap();

            finished_early_.store(true, Ordering::Relaxed);
        });

        thread::yield_now();

        let handle = handle.inner.driver().time();

        // start == driver start, so 1500/2500 ms map to those exact tick values.
        handle.process_at_time(1_500_000_000);

        assert!(!finished_early.load(Ordering::Relaxed));

        handle.process_at_time(2_500_000_000);

        jh.join().unwrap();

        assert!(finished_early.load(Ordering::Relaxed));
    })
}

#[cfg(not(loom))]
fn normal_or_miri<T>(normal: T, miri: T) -> T {
    if cfg!(miri) {
        miri
    } else {
        normal
    }
}

#[test]
#[cfg(not(loom))]
fn poll_process_levels() {
    let rt = rt(false);
    let handle = rt.handle();

    // Base deadlines on the driver's start time so each entry's wheel tick is
    // exactly `i`, keeping the level-walk assertions below deterministic on a
    // running clock.
    let start = handle.inner.driver().time().time_source().start_time();

    let mut entries = vec![];

    for i in 0..normal_or_miri(1024, 64) {
        let mut entry = Box::pin(TimerEntry::new(
            handle.inner.clone(),
            start + Duration::from_millis(i),
        ));

        let _ = entry
            .as_mut()
            .poll_elapsed(&mut Context::from_waker(noop_waker_ref()));

        entries.push(entry);
    }

    for t in 1..normal_or_miri(1024, 64) {
        handle
            .inner
            .driver()
            .time()
            .process_at_time(t as u64 * 1_000_000);

        for (deadline, future) in entries.iter_mut().enumerate() {
            let mut context = Context::from_waker(noop_waker_ref());
            if deadline <= t {
                assert!(future.as_mut().poll_elapsed(&mut context).is_ready());
            } else {
                assert!(future.as_mut().poll_elapsed(&mut context).is_pending());
            }
        }
    }
}

#[test]
#[cfg(not(loom))]
fn poll_process_levels_targeted() {
    let mut context = Context::from_waker(noop_waker_ref());

    let rt = rt(false);
    let handle = rt.handle();

    // A start-time-based deadline keeps this entry at exactly tick 193e6 ns.
    let start = handle.inner.driver().time().time_source().start_time();

    let e1 = TimerEntry::new(handle.inner.clone(), start + Duration::from_millis(193));
    pin!(e1);
    assert!(e1.as_mut().poll_elapsed(&mut context).is_pending());

    let handle = handle.inner.driver().time();

    handle.process_at_time(62 * 1_000_000);
    assert!(e1.as_mut().poll_elapsed(&mut context).is_pending());
    handle.process_at_time(192 * 1_000_000);
    handle.process_at_time(192 * 1_000_000);
}

#[test]
#[cfg(not(loom))]
fn instant_to_tick_max() {
    use crate::runtime::time::entry::MAX_SAFE_MILLIS_DURATION;

    let rt = rt(true);
    let handle = rt.handle().inner.driver().time();

    let start_time = handle.time_source.start_time();
    let long_future = start_time + std::time::Duration::from_millis(MAX_SAFE_MILLIS_DURATION + 1);

    assert!(handle.time_source.instant_to_tick(long_future) <= MAX_SAFE_MILLIS_DURATION);
}

#[test]
#[cfg(not(loom))]
fn instant_to_nanos_exact_and_saturating() {
    use crate::runtime::time::entry::STATE_DEREGISTERED;

    let rt = rt(false);
    let handle = rt.handle();
    let time_source = handle.inner.driver().time().time_source();
    let start = time_source.start_time();

    // Exact: nanosecond offsets survive the round trip unchanged.
    let t = start + Duration::from_nanos(1_234_567);
    assert_eq!(time_source.instant_to_nanos(t), 1_234_567);
    assert_eq!(time_source.nanos_to_instant(1_234_567), t);

    // Before the driver epoch saturates to zero (mirrors instant_to_tick's
    // saturating_duration_since).
    assert_eq!(
        time_source.instant_to_nanos(start - Duration::from_secs(1)),
        0
    );

    // Far-future instants saturate below the state-cell sentinels.
    let max = time_source.instant_to_nanos(t + Duration::from_secs(u32::MAX as u64) * 40);
    assert!(max < STATE_DEREGISTERED - 1);
}

#[test]
#[cfg(not(loom))]
fn paused_registration_lands_in_wheel_at_exact_ns() {
    let rt = rt(true);
    let handle = rt.handle();

    let entry = TimerEntry::new(
        handle.inner.clone(),
        handle.inner.driver().clock().now() + Duration::from_micros(100),
    );
    pin!(entry);
    // Registration happens on first poll.
    let _ = entry
        .as_mut()
        .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));

    let time = handle.inner.driver().time();
    let lock = time.inner.lock();
    assert_eq!(lock.wheel.next_when(), Some(100_000));
}

#[test]
#[cfg(not(loom))]
fn paused_sub_ms_entry_fires_at_end_of_time() {
    // Register a 100us timer on a paused runtime, then drive the driver the
    // way shutdown does: process_at_time(u64::MAX). The entry must fire
    // Ok(()).
    let rt = rt(true);
    let handle = rt.handle();

    let entry = TimerEntry::new(
        handle.inner.clone(),
        handle.inner.driver().clock().now() + Duration::from_micros(100),
    );
    pin!(entry);
    assert!(entry
        .as_mut()
        .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()))
        .is_pending());

    handle.inner.driver().time().process_at_time(u64::MAX);

    assert!(entry
        .as_mut()
        .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()))
        .is_ready());
}

#[test]
#[cfg(not(loom))]
#[should_panic(expected = "is being shutdown")]
fn paused_registration_after_shutdown_panics() {
    let rt = rt(true);
    let handle = rt.handle().inner.clone();
    drop(rt); // shuts the driver down

    let entry = TimerEntry::new(
        handle,
        crate::time::Instant::now() + Duration::from_micros(100),
    );
    pin!(entry);
    // Registration on a shut-down driver is rejected inside reregister
    // (fires Err(shutdown) into the state cell). The user-visible rejection
    // is the poll_elapsed assert, which panics.
    entry
        .as_mut()
        .reset(crate::time::Instant::now() + Duration::from_micros(100), true);
    let _ = entry
        .as_mut()
        .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));
}

#[test]
#[cfg(not(loom))]
fn dropping_paused_timer_clears_wheel() {
    let rt = rt(true);
    let handle = rt.handle();

    {
        let entry = TimerEntry::new(
            handle.inner.clone(),
            handle.inner.driver().clock().now() + Duration::from_micros(100),
        );
        pin!(entry);
        let _ = entry
            .as_mut()
            .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));
        assert!(handle
            .inner
            .driver()
            .time()
            .inner
            .lock()
            .wheel
            .next_when()
            .is_some());
        // entry dropped here -> PinnedDrop -> cancel -> clear_entry
    }

    assert_eq!(
        handle.inner.driver().time().inner.lock().wheel.next_when(),
        None
    );
}

#[test]
fn paused_timer_drop_vs_process() {
    model(|| {
        let rt = rt(true);
        let handle = rt.handle();

        let start = handle.inner.driver().clock().now();

        let jh = thread::spawn({
            let handle = handle.inner.clone();
            move || {
                let entry = TimerEntry::new(handle, start + Duration::from_micros(500));
                pin!(entry);
                let _ = entry
                    .as_mut()
                    .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));
                // drop without waiting -- races the firing below
            }
        });

        handle.inner.driver().time().process_at_time(u64::MAX);

        jh.join().unwrap();
    });
}

#[test]
fn paused_timer_reset_vs_fire() {
    model(|| {
        let rt = rt(true);
        let handle = rt.handle();

        let start = handle.inner.driver().clock().now();

        let jh = thread::spawn({
            let handle = handle.inner.clone();
            move || {
                let entry = TimerEntry::new(handle, start + Duration::from_micros(500));
                pin!(entry);
                let _ = entry
                    .as_mut()
                    .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));
                // Lock-free extend (later deadline) racing the driver firing
                // at u64::MAX below: either the extend wins (entry stays
                // registered, then fired by a later poll) or the fire wins
                // (reset's extend fails -> full re-register on a fired entry
                // re-arms it). Both must be memory-safe and deadlock-free;
                // the entry must end fired or pending, never lost.
                entry
                    .as_mut()
                    .reset(start + Duration::from_micros(800), true);
                let _ = entry
                    .as_mut()
                    .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));
            }
        });

        let time = handle.inner.driver().time();
        time.process_at_time(u64::MAX);
        // Sweep again so an entry re-registered after the first sweep cannot
        // be stranded unfired across the join below.
        time.process_at_time(u64::MAX);

        jh.join().unwrap();
    });
}

#[test]
#[cfg(not(loom))]
fn wheel_next_when_is_exact_for_upper_levels() {
    let rt = rt(true);
    let handle = rt.handle();
    let clock = handle.inner.driver().clock();
    let time = handle.inner.driver().time();
    let start = time.time_source().start_time();
    assert_eq!(start, clock.now());

    // 10s lands at level 5 of the ns-tick wheel; the slot start (which
    // `next_expiration_time` returns) is several hundred ms below the
    // deadline.
    let entry = TimerEntry::new(handle.inner.clone(), start + Duration::from_secs(10));
    pin!(entry);
    let _ = entry
        .as_mut()
        .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));

    let lock = time.inner.lock();
    let slot_start = lock.wheel.next_expiration_time().unwrap();
    assert!(slot_start < 10_000_000_000);
    assert_eq!(lock.wheel.next_when(), Some(10_000_000_000));
}
