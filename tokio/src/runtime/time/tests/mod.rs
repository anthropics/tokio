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

#[test]
fn single_timer() {
    model(|| {
        let rt = rt(false);
        let handle = rt.handle();

        let handle_ = handle.clone();
        let jh = thread::spawn(move || {
            let entry = TimerEntry::new(
                handle_.inner.clone(),
                handle_.inner.driver().clock().now() + Duration::from_secs(1),
            );
            pin!(entry);

            block_on(std::future::poll_fn(|cx| entry.as_mut().poll_elapsed(cx))).unwrap();
        });

        thread::yield_now();

        let time = handle.inner.driver().time();
        let clock = handle.inner.driver().clock();

        // advance 2s
        time.process_at_time(time.time_source().now(clock) + 2_000_000_000);

        jh.join().unwrap();
    })
}

#[test]
fn drop_timer() {
    model(|| {
        let rt = rt(false);
        let handle = rt.handle();

        let handle_ = handle.clone();
        let jh = thread::spawn(move || {
            let entry = TimerEntry::new(
                handle_.inner.clone(),
                handle_.inner.driver().clock().now() + Duration::from_secs(1),
            );
            pin!(entry);

            let _ = entry
                .as_mut()
                .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));
            let _ = entry
                .as_mut()
                .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));
        });

        thread::yield_now();

        let time = handle.inner.driver().time();
        let clock = handle.inner.driver().clock();

        // advance 2s in the future.
        time.process_at_time(time.time_source().now(clock) + 2_000_000_000);

        jh.join().unwrap();
    })
}

#[test]
fn change_waker() {
    model(|| {
        let rt = rt(false);
        let handle = rt.handle();

        let handle_ = handle.clone();
        let jh = thread::spawn(move || {
            let entry = TimerEntry::new(
                handle_.inner.clone(),
                handle_.inner.driver().clock().now() + Duration::from_secs(1),
            );
            pin!(entry);

            let _ = entry
                .as_mut()
                .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));

            block_on(std::future::poll_fn(|cx| entry.as_mut().poll_elapsed(cx))).unwrap();
        });

        thread::yield_now();

        let time = handle.inner.driver().time();
        let clock = handle.inner.driver().clock();

        // advance 2s
        time.process_at_time(time.time_source().now(clock) + 2_000_000_000);

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
        let start = handle.inner.driver().clock().now();

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

        handle.process_at_time(
            handle
                .time_source()
                .instant_to_tick(start + Duration::from_millis(1500)),
        );

        assert!(!finished_early.load(Ordering::Relaxed));

        handle.process_at_time(
            handle
                .time_source()
                .instant_to_tick(start + Duration::from_millis(2500)),
        );

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

    // This test exists to walk the wheel's levels, so the entries must be
    // wheel-resident: a paused runtime would route them to the exact store,
    // where the tick walk below could not exercise level cascades.
    {
        let lock = handle.inner.driver().time().inner.lock();
        assert_eq!(lock.exact.len(), 0);
        assert!(lock.wheel.next_expiration_time().is_some());
    }

    for t in 1..normal_or_miri(1024, 64) {
        handle.inner.driver().time().process_at_time(t as u64);

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

    // As in `poll_process_levels`: an unpaused runtime and a start-time-based
    // deadline keep this entry wheel-resident at exactly tick 193.
    let start = handle.inner.driver().time().time_source().start_time();

    let e1 = TimerEntry::new(handle.inner.clone(), start + Duration::from_millis(193));
    pin!(e1);
    // Registration happens on first poll; poll now so the wheel-residency
    // assertion below observes the registered entry.
    assert!(e1.as_mut().poll_elapsed(&mut context).is_pending());

    let handle = handle.inner.driver().time();

    {
        let lock = handle.inner.lock();
        assert_eq!(lock.exact.len(), 0);
        assert!(lock.wheel.next_expiration_time().is_some());
    }

    handle.process_at_time(62);
    assert!(e1.as_mut().poll_elapsed(&mut context).is_pending());
    handle.process_at_time(192);
    handle.process_at_time(192);
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
fn paused_registration_lands_in_exact_store() {
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
    assert_eq!(lock.exact.len(), 1);
    assert_eq!(lock.wheel.next_expiration_time(), None);
}

#[test]
#[cfg(not(loom))]
fn unpaused_registration_lands_in_wheel() {
    let rt = rt(false);
    let handle = rt.handle();

    let entry = TimerEntry::new(
        handle.inner.clone(),
        handle.inner.driver().clock().now() + Duration::from_secs(1),
    );
    pin!(entry);
    // Registration happens on first poll.
    let _ = entry
        .as_mut()
        .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));

    let time = handle.inner.driver().time();
    let lock = time.inner.lock();
    assert_eq!(lock.exact.len(), 0);
    assert!(lock.wheel.next_expiration_time().is_some());
}

#[test]
#[cfg(not(loom))]
fn pre_pause_timer_stays_in_wheel_after_pause() {
    // Routing half of the pre-pause contract: register on a running
    // (pausable) clock, then pause; the wheel-resident entry stays put, and a
    // NEW registration goes to the store.
    let rt = rt(false);
    let handle = rt.handle();
    let clock = handle.inner.driver().clock();

    let entry_a = TimerEntry::new(handle.inner.clone(), clock.now() + Duration::from_secs(1));
    pin!(entry_a);
    // Register (first poll) while the clock is still running.
    let _ = entry_a
        .as_mut()
        .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));

    {
        let _enter = rt.enter();
        crate::time::pause();
    }

    let entry_b = TimerEntry::new(
        handle.inner.clone(),
        clock.now() + Duration::from_micros(100),
    );
    pin!(entry_b);
    let _ = entry_b
        .as_mut()
        .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));

    let time = handle.inner.driver().time();
    let lock = time.inner.lock();
    assert_eq!(lock.exact.len(), 1);
    assert!(lock.wheel.next_expiration_time().is_some());
}

#[test]
#[cfg(not(loom))]
fn exact_store_entry_fires_via_process_at_time() {
    // Register a 100us timer on a paused runtime, then drive the driver the
    // way shutdown does: process_at_time(u64::MAX). The entry must fire
    // Ok(()) -- same semantics as wheel entries at shutdown.
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
    // Registration on a shut-down driver is rejected inside reregister,
    // before route dispatch (fires Err(shutdown) into the state cell) --
    // identical for both routes. The user-visible rejection is the
    // poll_elapsed assert, which panics.
    entry
        .as_mut()
        .reset(crate::time::Instant::now() + Duration::from_micros(100), true);
    let _ = entry
        .as_mut()
        .poll_elapsed(&mut Context::from_waker(futures::task::noop_waker_ref()));
}

#[test]
#[cfg(not(loom))]
fn dropping_paused_timer_clears_exact_store() {
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
        assert_eq!(handle.inner.driver().time().inner.lock().exact.len(), 1);
        // entry dropped here -> PinnedDrop -> cancel -> clear_entry
    }

    assert_eq!(handle.inner.driver().time().inner.lock().exact.len(), 0);
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
                // at u64::MAX below: either the extend wins (entry reinserted
                // at its true deadline, then fired by a later pop) or the
                // fire wins (reset's extend fails -> full re-register on a
                // fired entry re-arms it). Both must be memory-safe and
                // deadlock-free; the entry must end fired or pending, never
                // lost.
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
fn timer_shared_exact_key_roundtrip() {
    use crate::runtime::time::TimerShared;

    let shared = Box::pin(TimerShared::new());

    assert_eq!(shared.exact_key(), None);

    // SAFETY: single-threaded test; the entry is in no driver structure, so
    // the "driver lock or &mut TimerEntry" access rule is trivially upheld.
    unsafe {
        shared.set_exact_key(1_234_567, 42);
        assert_eq!(shared.exact_key(), Some((1_234_567, 42)));

        shared.clear_exact_key();
        assert_eq!(shared.exact_key(), None);
    }
}
