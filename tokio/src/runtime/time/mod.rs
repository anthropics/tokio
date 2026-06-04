// Currently, rust warns when an unsafe fn contains an unsafe {} block. However,
// in the future, this will change to the reverse. For now, suppress this
// warning and generally stick with being explicit about unsafety.
#![allow(unused_unsafe)]
#![cfg_attr(not(feature = "rt"), allow(dead_code))]

//! Time driver.

mod entry;
pub(crate) use entry::TimerEntry;
use entry::{EntryList, TimerHandle, TimerShared, MAX_SAFE_MILLIS_DURATION};

mod handle;
pub(crate) use self::handle::Handle;

mod source;
pub(crate) use source::TimeSource;

mod wheel;

cfg_test_util! {
    mod exact;
    use exact::ExactStore;
}

#[cfg(all(tokio_unstable, feature = "rt-multi-thread"))]
use super::time_alt;

use crate::loom::sync::atomic::{AtomicBool, Ordering};
use crate::loom::sync::Mutex;
use crate::runtime::driver::{self, IoHandle, IoStack};
use crate::time::error::Error;
use crate::time::{Clock, Duration};
use crate::util::WakeList;

use std::fmt;
use std::{num::NonZeroU64, ptr::NonNull};

/// Time implementation that drives [`Sleep`][sleep], [`Interval`][interval], and [`Timeout`][timeout].
///
/// A `Driver` instance tracks the state necessary for managing time and
/// notifying the [`Sleep`][sleep] instances once their deadlines are reached.
///
/// It is expected that a single instance manages many individual [`Sleep`][sleep]
/// instances. The `Driver` implementation is thread-safe and, as such, is able
/// to handle callers from across threads.
///
/// After creating the `Driver` instance, the caller must repeatedly call `park`
/// or `park_timeout`. The time driver will perform no work unless `park` or
/// `park_timeout` is called repeatedly.
///
/// For a running clock, the driver has a resolution of one millisecond: any
/// unit of time that falls between milliseconds is rounded up to the next
/// millisecond. Timers registered while the clock is paused (`test-util`
/// feature) are instead kept in an exact, nanosecond-keyed store and fire at
/// their exact deadlines.
///
/// When an instance is dropped, any outstanding [`Sleep`][sleep] instance that has not
/// elapsed will be notified with an error. At this point, calling `poll` on the
/// [`Sleep`][sleep] instance will result in panic.
///
/// # Implementation
///
/// The time driver is based on the [paper by Varghese and Lauck][paper].
///
/// A hashed timing wheel is a vector of slots, where each slot handles a time
/// slice. As time progresses, the timer walks over the slot for the current
/// instant, and processes each entry for that slot. When the timer reaches the
/// end of the wheel, it starts again at the beginning.
///
/// The implementation maintains six wheels arranged in a set of levels. As the
/// levels go up, the slots of the associated wheel represent larger intervals
/// of time. At each level, the wheel has 64 slots. Each slot covers a range of
/// time equal to the wheel at the lower level. At level zero, each slot
/// represents one millisecond of time.
///
/// The wheels are:
///
/// * Level 0: 64 x 1 millisecond slots.
/// * Level 1: 64 x 64 millisecond slots.
/// * Level 2: 64 x ~4 second slots.
/// * Level 3: 64 x ~4 minute slots.
/// * Level 4: 64 x ~4 hour slots.
/// * Level 5: 64 x ~12 day slots.
///
/// When the timer processes entries at level zero, it will notify all the
/// `Sleep` instances as their deadlines have been reached. For all higher
/// levels, all entries will be redistributed across the wheel at the next level
/// down. Eventually, as time progresses, entries with [`Sleep`][sleep] instances will
/// either be canceled (dropped) or their associated entries will reach level
/// zero and be notified.
///
/// [paper]: http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf
/// [sleep]: crate::time::Sleep
/// [timeout]: crate::time::Timeout
/// [interval]: crate::time::Interval
#[derive(Debug)]
pub(crate) struct Driver {
    /// Parker to delegate to.
    park: IoStack,
}

enum Inner {
    Traditional {
        // The state is split like this so `Handle` can access `is_shutdown` without locking the mutex
        state: Mutex<InnerState>,

        /// True if the driver is being shutdown.
        is_shutdown: AtomicBool,

        // When `true`, a call to `park_timeout` should immediately return and time
        // should not advance. One reason for this to be `true` is if the task
        // passed to `Runtime::block_on` called `task::yield_now()`.
        //
        // While it may look racy, it only has any effect when the clock is paused
        // and pausing the clock is restricted to a single-threaded runtime.
        #[cfg(feature = "test-util")]
        did_wake: AtomicBool,
    },

    #[cfg(all(tokio_unstable, feature = "rt-multi-thread"))]
    Alternative {
        /// True if the driver is being shutdown.
        is_shutdown: AtomicBool,

        // When `true`, a call to `park_timeout` should immediately return and time
        // should not advance. One reason for this to be `true` is if the task
        // passed to `Runtime::block_on` called `task::yield_now()`.
        //
        // While it may look racy, it only has any effect when the clock is paused
        // and pausing the clock is restricted to a single-threaded runtime.
        #[cfg(feature = "test-util")]
        did_wake: AtomicBool,
    },
}

/// Time state shared which must be protected by a `Mutex`
struct InnerState {
    /// The earliest time at which we promise to wake up without unparking.
    next_wake: Option<NonZeroU64>,

    /// The earliest deadline (ns since driver start) at which we promise to
    /// wake up without being unparked, taking the minimum over the exact
    /// store and the wheel (wheel ticks converted at 1 tick = `1e6` ns).
    /// Used by exact-store registrations to decide whether to unpark, the
    /// same role `next_wake` plays for wheel registrations.
    #[cfg(feature = "test-util")]
    next_wake_ns: Option<NonZeroU64>,

    /// Timer wheel.
    wheel: wheel::Wheel,

    /// Exact-deadline timers registered while the clock is paused. Timers in
    /// here fire at exact nanosecond deadlines; timers in `wheel` fire at
    /// ms-tick granularity. The driver's next-wake decisions take the minimum
    /// over both.
    ///
    /// Only the Traditional driver services this store. That is sound: a
    /// paused clock requires the `current_thread` runtime, which always uses
    /// the Traditional flavor; the Alternative (`time_alt`) path can never see
    /// a paused clock.
    #[cfg(feature = "test-util")]
    exact: ExactStore,

    /// Registered quiesce waiters (test-util). Protected by the same mutex as the
    /// wheel so the resolution decision (compare bounds against the earliest
    /// pending deadline across the exact store and the wheel) is atomic.
    ///
    /// At most one entry is unresolved at any time -- registration refuses a
    /// second in-progress step -- so resolution moves the clock to a single,
    /// well-defined bound. Resolved entries are inert mailboxes awaiting
    /// collection by their futures' next poll.
    #[cfg(feature = "test-util")]
    quiesce_waiters: Vec<QuiesceWaiter>,

    /// Monotonic id source for quiesce waiter registrations.
    #[cfg(feature = "test-util")]
    next_quiesce_waiter_id: u64,
}

cfg_test_util! {
    /// A registered quiesce waiter: a task waiting for the runtime to become
    /// quiescent at or below a virtual-time bound.
    struct QuiesceWaiter {
        /// Registration id (handed back to the `Quiesce` future).
        id: u64,

        /// Inclusive bound in nanoseconds since driver start
        /// (`instant_to_nanos`; exact, no round-up), or `None` for an
        /// unbounded waiter (resolves only when the exact store and the wheel
        /// are both empty).
        bound: Option<u64>,

        /// Waker of the waiting task (or root future).
        waker: std::task::Waker,

        /// Filled at resolution; collected by the future's next poll.
        result: Option<crate::time::QuiescedState>,
    }

    /// Outcome of polling a registered quiesce waiter.
    pub(crate) enum QuiescePoll {
        /// The waiter resolved; it has been removed from the registry.
        Ready(crate::time::QuiescedState),

        /// The waiter is registered but has not yet resolved.
        Pending,

        /// The waiter is no longer in the registry. The registry is only ever
        /// drained wholesale by `Driver::shutdown`, so this means the driver shut
        /// down after the waiter registered.
        Missing,
    }

    /// Outcome of registering a quiesce waiter.
    pub(crate) enum QuiesceRegister {
        /// Registered; the id is handed back to the `Quiesce` future.
        Registered(u64),

        /// Another step is already in progress (an unresolved waiter is
        /// registered). Only one step may be in progress at a time: resolving a
        /// step moves the clock to that step's bound, and the clock can only land
        /// on one bound.
        Busy,

        /// The driver is shutting down; registration refused.
        Shutdown,
    }
}

/// Where and at what deadline a timer (re)registration should land.
pub(super) enum RegisterWhen {
    /// Wheel registration at a ms tick (`deadline_to_tick`).
    Wheel(u64),
    /// Exact-store registration while the clock is paused.
    #[cfg(feature = "test-util")]
    Exact {
        /// Deadline, in ns since driver start.
        when_ns: u64,
        /// The paused clock's position (ns since driver start) sampled at
        /// route time, used for the synchronous already-elapsed fire that
        /// mirrors the wheel's `Elapsed` insert result. The paused clock
        /// only moves forward, so a stale sample can only under-classify a
        /// deadline as still-future -- the entry then lands in the store and
        /// the driver's next park-bound recomputation picks it up.
        now_ns: u64,
    },
}

cfg_test_util! {
    /// Merged ns-domain wake bound: min(store first deadline, wheel tick * `1e6`),
    /// with the same 0 -> 1 clamp convention `next_wake` uses.
    fn merged_next_wake_ns(exact: Option<u64>, wheel_tick: Option<u64>) -> Option<NonZeroU64> {
        let wheel_ns = wheel_tick.map(|t| t.saturating_mul(1_000_000));
        let min_ns = match (exact, wheel_ns) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        min_ns.map(|v| NonZeroU64::new(v).unwrap_or_else(|| NonZeroU64::new(1).unwrap()))
    }

    /// Earliest pending deadline across the exact store and the wheel, in ns.
    /// The wheel's contribution is its tick lower bound (slot start for upper
    /// levels), so the merged value is a sound lower bound; the store's
    /// contribution is exact.
    fn next_pending_ns(lock: &InnerState) -> Option<u64> {
        match (
            lock.exact.next_deadline(),
            lock.wheel.next_expiration_time().map(|t| t.saturating_mul(1_000_000)),
        ) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// True if some registered, not-yet-resolved quiesce waiter would resolve
    /// at the runtime's current virtual-time position (its bound lies below
    /// every pending deadline). Such a waiter is owed a resolution by the
    /// drain-park hook before the clock moves again; an auto-advance now
    /// would cross its bound.
    fn has_resolvable_quiesce_waiter(lock: &InnerState) -> bool {
        if lock.quiesce_waiters.is_empty() {
            return false;
        }

        let next_pending = next_pending_ns(lock);
        lock.quiesce_waiters.iter().any(|waiter| {
            waiter.result.is_none()
                && match (waiter.bound, next_pending) {
                    (_, None) => true,
                    (None, Some(_)) => false,
                    (Some(bound), Some(next)) => bound < next,
                }
        })
    }
}

// ===== impl Driver =====

impl Driver {
    /// Creates a new `Driver` instance that uses `park` to block the current
    /// thread and `time_source` to get the current time and convert to ticks.
    ///
    /// Specifying the source of time is useful when testing.
    pub(crate) fn new(park: IoStack, clock: &Clock) -> (Driver, Handle) {
        let time_source = TimeSource::new(clock);

        let handle = Handle {
            time_source,
            inner: Inner::Traditional {
                state: Mutex::new(InnerState {
                    next_wake: None,

                    #[cfg(feature = "test-util")]
                    next_wake_ns: None,

                    wheel: wheel::Wheel::new(),

                    #[cfg(feature = "test-util")]
                    exact: ExactStore::new(),

                    #[cfg(feature = "test-util")]
                    quiesce_waiters: Vec::new(),

                    #[cfg(feature = "test-util")]
                    next_quiesce_waiter_id: 0,
                }),
                is_shutdown: AtomicBool::new(false),

                #[cfg(feature = "test-util")]
                did_wake: AtomicBool::new(false),
            },

            #[cfg(feature = "test-util")]
            quiesce_waiter_count: crate::loom::sync::atomic::AtomicUsize::new(0),
        };

        let driver = Driver { park };

        (driver, handle)
    }

    #[cfg(all(tokio_unstable, feature = "rt-multi-thread"))]
    pub(crate) fn new_alt(clock: &Clock) -> Handle {
        let time_source = TimeSource::new(clock);

        Handle {
            time_source,
            inner: Inner::Alternative {
                is_shutdown: AtomicBool::new(false),
                #[cfg(feature = "test-util")]
                did_wake: AtomicBool::new(false),
            },

            #[cfg(feature = "test-util")]
            quiesce_waiter_count: crate::loom::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(crate) fn park(&mut self, handle: &driver::Handle) {
        self.park_internal(handle, None);
    }

    pub(crate) fn park_timeout(&mut self, handle: &driver::Handle, duration: Duration) {
        self.park_internal(handle, Some(duration));
    }

    pub(crate) fn shutdown(&mut self, rt_handle: &driver::Handle) {
        let handle = rt_handle.time();

        if handle.is_shutdown() {
            return;
        }

        match &handle.inner {
            Inner::Traditional { is_shutdown, .. } => {
                is_shutdown.store(true, Ordering::SeqCst);
            }
            #[cfg(all(tokio_unstable, feature = "rt-multi-thread"))]
            Inner::Alternative { is_shutdown, .. } => {
                is_shutdown.store(true, Ordering::SeqCst);
            }
        }

        // Advance time forward to the end of time.

        handle.process_at_time(u64::MAX);

        // Wake any registered quiesce waiters so they can observe the shutdown.
        #[cfg(feature = "test-util")]
        {
            let mut lock = handle.inner.lock();
            let waiters = std::mem::take(&mut lock.quiesce_waiters);
            // The count mirrors the registry and is only mutated while holding the
            // registry lock; reset it together with the drain. Orphaned `Quiesce`
            // futures cannot do this themselves (their entries are already gone, so
            // their deregistration is a no-op), and a stale count would make
            // `resume()`/`advance()` through a still-live `Handle` report a phantom
            // in-progress quiesce.
            handle.quiesce_waiter_count.store(0, Ordering::Relaxed);
            drop(lock);
            for waiter in waiters {
                waiter.waker.wake();
            }
        }

        self.park.shutdown(rt_handle);
    }

    fn park_internal(&mut self, rt_handle: &driver::Handle, limit: Option<Duration>) {
        let handle = rt_handle.time();
        let mut lock = handle.inner.lock();

        assert!(!handle.is_shutdown());

        let next_wake_tick = lock.wheel.next_expiration_time();
        lock.next_wake = next_wake_tick
            .map(|t| NonZeroU64::new(t).unwrap_or_else(|| NonZeroU64::new(1).unwrap()));

        #[cfg(feature = "test-util")]
        let next_exact = lock.exact.next_deadline();
        #[cfg(feature = "test-util")]
        {
            lock.next_wake_ns = merged_next_wake_ns(next_exact, next_wake_tick);
        }

        drop(lock);

        // Park-duration candidates. With test-util both are computed in the
        // nanosecond domain from one clock read: the wheel candidate's target
        // is the tick boundary itself, and measuring the distance from the
        // clock's exact position makes auto-advance land exactly on that
        // boundary. A hop computed in truncated whole ticks from a
        // fractional-millisecond position would overshoot the boundary by the
        // fraction -- carrying a quiesce step past its bound. (Real parks
        // never see a sub-ms wheel duration anyway: park_thread_timeout
        // floors them to 1ms.)
        #[cfg(feature = "test-util")]
        let (wheel_dur, exact_dur) = {
            let now_ns = if next_wake_tick.is_some() || next_exact.is_some() {
                handle.time_source.instant_to_nanos(rt_handle.clock().now())
            } else {
                0
            };
            (
                next_wake_tick.map(|when| {
                    Duration::from_nanos(when.saturating_mul(1_000_000).saturating_sub(now_ns))
                }),
                next_exact.map(|when_ns| Duration::from_nanos(when_ns.saturating_sub(now_ns))),
            )
        };

        #[cfg(not(feature = "test-util"))]
        let wheel_dur = next_wake_tick.map(|when| {
            let now = handle.time_source.now(rt_handle.clock());
            // Note that we effectively round up to 1ms here - this avoids
            // very short-duration microsecond-resolution sleeps that the OS
            // might treat as zero-length.
            handle
                .time_source
                .tick_to_duration(when.saturating_sub(now))
        });
        #[cfg(not(feature = "test-util"))]
        let exact_dur: Option<Duration> = None;

        let next_dur = match (wheel_dur, exact_dur) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };

        match next_dur {
            Some(mut duration) => {
                if duration > Duration::from_millis(0) {
                    if let Some(limit) = limit {
                        duration = std::cmp::min(limit, duration);
                    }

                    self.park_thread_timeout(rt_handle, duration);
                } else {
                    self.park.park_timeout(rt_handle, Duration::from_secs(0));
                }
            }
            None => {
                if let Some(duration) = limit {
                    self.park_thread_timeout(rt_handle, duration);
                } else {
                    self.park.park(rt_handle);
                }
            }
        }

        // Process pending timers after waking up
        handle.process(rt_handle.clock());
    }

    cfg_test_util! {
        fn park_thread_timeout(&mut self, rt_handle: &driver::Handle, duration: Duration) {
            let handle = rt_handle.time();
            let clock = rt_handle.clock();

            if clock.can_auto_advance() {
                self.park.park_timeout(rt_handle, Duration::from_secs(0));

                // If the time driver was woken, then the park completed
                // before the "duration" elapsed (usually caused by a
                // yield in `Runtime::block_on`). In this case, we don't
                // advance the clock.
                //
                // This veto also makes the over-advance race with a
                // concurrently released blocking-task inhibit benign: the
                // release unparks the driver, which sets `did_wake`, so the
                // advance below is skipped and the wake-up time is recomputed
                // (the `quiesce_blocking_release_vs_park` loom model covers
                // this ordering).
                if !handle.did_wake() {
                    // Re-validate before moving the clock; both checks must be
                    // atomic with the advance itself:
                    //
                    // - A quiesce waiter registered since this park's bound
                    //   was computed must not have its bound crossed. Holding
                    //   the registry lock across the advance means a
                    //   concurrent registration either lands before this
                    //   check (and vetoes the advance) or after the advance
                    //   has fully completed -- never in between. The
                    //   `quiesce_register_vs_auto_advance` loom model pins
                    //   this.
                    // - An inhibit taken (a `spawn_blocking` spawned from
                    //   another thread) or a `resume()` landing since
                    //   `can_auto_advance()` must veto the advance;
                    //   `try_auto_advance` re-checks under the clock lock.
                    //
                    // A vetoed advance is not lost: the scheduler loop parks
                    // again, and the next pass recomputes the wake-up time or
                    // the drain-park hook resolves the waiter.
                    let lock = handle.inner.lock();
                    if !has_resolvable_quiesce_waiter(&lock) {
                        clock.try_auto_advance(duration);
                    }
                    drop(lock);
                }
            } else {
                // A sub-ms timeout can only come from the exact store (wheel
                // durations are whole ms). Under a running clock, clamp it up
                // to 1ms: very short OS sleeps may be treated as zero-length
                // (same rationale as the wheel's tick rounding), and firing
                // up to 1ms late matches the lateness envelope sub-ms timers
                // have always had. Never clamp the auto-advance branch above:
                // that amount becomes virtual-time movement and must stay
                // exact.
                self.park
                    .park_timeout(rt_handle, duration.max(Duration::from_millis(1)));
            }
        }
    }

    cfg_not_test_util! {
        fn park_thread_timeout(&mut self, rt_handle: &driver::Handle, duration: Duration) {
            self.park.park_timeout(rt_handle, duration);
        }
    }
}

impl Handle {
    pub(self) fn process(&self, clock: &Clock) {
        // Read the clock once and derive both units from the same instant, so
        // the tick and ns positions can never disagree about "now".
        let now_instant = clock.now();
        let now = self.time_source().instant_to_tick(now_instant);
        #[cfg(feature = "test-util")]
        let now_ns = self.time_source().instant_to_nanos(now_instant);

        self.process_at(
            now,
            #[cfg(feature = "test-util")]
            now_ns,
        );
    }

    pub(self) fn process_at_time(&self, now: u64) {
        // Raw-tick callers (tests, shutdown) get the tick boundary as their
        // ns position; saturating_mul keeps u64::MAX meaning "end of time".
        self.process_at(
            now,
            #[cfg(feature = "test-util")]
            now.saturating_mul(1_000_000),
        );
    }

    fn process_at(&self, mut now: u64, #[cfg(feature = "test-util")] now_ns: u64) {
        let mut waker_list = WakeList::new();

        let mut lock = self.inner.lock();

        if now < lock.wheel.elapsed() {
            // Time went backwards! This normally shouldn't happen as the Rust language
            // guarantees that an Instant is monotonic, but can happen when running
            // Linux in a VM on a Windows host due to std incorrectly trusting the
            // hardware clock to be monotonic.
            //
            // See <https://github.com/tokio-rs/tokio/issues/3619> for more information.
            now = lock.wheel.elapsed();
        }

        // Fire due exact-store entries first. Each structure is compared only
        // against its own unit. Re-acquiring the lock mid-loop is sound
        // because `pop_due` re-reads the map's first entry each iteration --
        // registrations or cancellations during the unlock window are
        // observed.
        #[cfg(feature = "test-util")]
        // SAFETY: lock held; pop_due hands back entries committed to fire and
        // removed from every structure.
        while let Some(entry) = unsafe { lock.exact.pop_due(now_ns) } {
            if let Some(waker) = unsafe { entry.fire(Ok(())) } {
                waker_list.push(waker);

                if !waker_list.can_push() {
                    // Wake a batch of wakers. To avoid deadlock, we must do this with the lock temporarily dropped.
                    drop(lock);

                    waker_list.wake_all();

                    lock = self.inner.lock();
                }
            }
        }

        while let Some(entry) = lock.wheel.poll(now) {
            debug_assert!(unsafe { entry.is_pending() });

            // SAFETY: We hold the driver lock, and just removed the entry from any linked lists.
            if let Some(waker) = unsafe { entry.fire(Ok(())) } {
                waker_list.push(waker);

                if !waker_list.can_push() {
                    // Wake a batch of wakers. To avoid deadlock, we must do this with the lock temporarily dropped.
                    drop(lock);

                    waker_list.wake_all();

                    lock = self.inner.lock();
                }
            }
        }

        lock.next_wake = lock
            .wheel
            .poll_at()
            .map(|t| NonZeroU64::new(t).unwrap_or_else(|| NonZeroU64::new(1).unwrap()));

        #[cfg(feature = "test-util")]
        {
            lock.next_wake_ns =
                merged_next_wake_ns(lock.exact.next_deadline(), lock.wheel.poll_at());
        }

        drop(lock);

        waker_list.wake_all();
    }

    #[cfg(all(tokio_unstable, feature = "rt-multi-thread"))]
    pub(crate) fn process_at_time_alt(
        &self,
        wheel: &mut time_alt::Wheel,
        mut now: u64,
        wake_queue: &mut time_alt::WakeQueue,
    ) {
        if now < wheel.elapsed() {
            // Time went backwards! This normally shouldn't happen as the Rust language
            // guarantees that an Instant is monotonic, but can happen when running
            // Linux in a VM on a Windows host due to std incorrectly trusting the
            // hardware clock to be monotonic.
            //
            // See <https://github.com/tokio-rs/tokio/issues/3619> for more information.
            now = wheel.elapsed();
        }

        wheel.take_expired(now, wake_queue);
    }

    #[cfg(all(tokio_unstable, feature = "rt-multi-thread"))]
    pub(crate) fn shutdown_alt(&self, wheel: &mut time_alt::Wheel) {
        // self.is_shutdown.store(true, Ordering::SeqCst);
        // Advance time forward to the end of time.
        // This will ensure that all timers are fired.
        let max_tick = u64::MAX;
        let mut wake_queue = time_alt::WakeQueue::new();
        self.process_at_time_alt(wheel, max_tick, &mut wake_queue);
        wake_queue.wake_all();
    }

    /// Removes the entry from whichever structure currently holds it, if any.
    ///
    /// SAFETY: caller holds the driver lock; the entry must be registered with
    /// this driver or unregistered.
    unsafe fn remove_from_structures(lock: &mut InnerState, entry: NonNull<TimerShared>) {
        #[cfg(feature = "test-util")]
        if unsafe { entry.as_ref().exact_key() }.is_some() {
            unsafe { lock.exact.remove(entry) };
            return;
        }
        if unsafe { entry.as_ref().might_be_registered() } {
            unsafe { lock.wheel.remove(entry) };
        }
    }

    /// Removes a registered timer from the driver.
    ///
    /// The timer will be moved to the cancelled state. Wakers will _not_ be
    /// invoked. If the timer is already completed, this function is a no-op.
    ///
    /// This function always acquires the driver lock, even if the entry does
    /// not appear to be registered: that lock acquisition is the `acq/rel`
    /// fence `TimerEntry::cancel` relies on for cross-thread drops.
    ///
    /// SAFETY: The timer must not be registered with some other driver, and
    /// `add_entry` must not be called concurrently.
    pub(self) unsafe fn clear_entry(&self, entry: NonNull<TimerShared>) {
        unsafe {
            let mut lock = self.inner.lock();

            Self::remove_from_structures(&mut lock, entry);

            entry.as_ref().handle().fire(Ok(()));
        }
    }

    /// Removes and re-adds an entry to the driver.
    ///
    /// SAFETY: The timer must be either unregistered, or registered with this
    /// driver. No other threads are allowed to concurrently manipulate the
    /// timer at all (the current thread should hold an exclusive reference to
    /// the `TimerEntry`)
    pub(self) unsafe fn reregister(
        &self,
        unpark: &IoHandle,
        new_when: RegisterWhen,
        entry: NonNull<TimerShared>,
    ) {
        let waker = unsafe {
            let mut lock = self.inner.lock();

            // We may have raced with a firing/deregistration, so check before
            // deregistering.
            Self::remove_from_structures(&mut lock, entry);

            // Now that we have exclusive control of this entry, mint a handle to reinsert it.
            let entry = entry.as_ref().handle();

            if self.is_shutdown() {
                unsafe { entry.fire(Err(crate::time::error::Error::shutdown())) }
            } else {
                match new_when {
                    RegisterWhen::Wheel(tick) => {
                        entry.set_expiration(tick);

                        // Note: We don't have to worry about racing with some other resetting
                        // thread, because add_entry and reregister require exclusive control of
                        // the timer entry.
                        match unsafe { lock.wheel.insert(entry) } {
                            Ok(when) => {
                                if lock
                                    .next_wake
                                    .map(|next_wake| when < next_wake.get())
                                    .unwrap_or(true)
                                {
                                    unpark.unpark();
                                }

                                None
                            }
                            Err((entry, crate::time::error::InsertError::Elapsed)) => unsafe {
                                entry.fire(Ok(()))
                            },
                        }
                    }
                    #[cfg(feature = "test-util")]
                    RegisterWhen::Exact { when_ns, now_ns } => {
                        entry.set_expiration(when_ns);

                        if when_ns <= now_ns {
                            // Already-reached deadline: fire synchronously,
                            // mirroring the wheel's `Elapsed` insert result, so
                            // a first poll after registration observes Ready
                            // exactly as it would on the wheel path.
                            unsafe { entry.fire(Ok(())) }
                        } else {
                            // SAFETY: lock held; removal above guarantees the
                            // entry is in no structure.
                            unsafe { lock.exact.insert(entry) };
                            // Unpark whenever the new deadline beats the
                            // published wake bound, so a parked driver
                            // re-evaluates its wake-up time -- the same role
                            // the wheel arm's next_wake comparison plays.
                            if lock
                                .next_wake_ns
                                .map(|nw| when_ns < nw.get())
                                .unwrap_or(true)
                            {
                                unpark.unpark();
                            }
                            None
                        }
                    }
                }
            }

            // Must release lock before invoking waker to avoid the risk of deadlock.
        };

        // The timer was fired synchronously as a result of the reregistration.
        // Wake the waker; this is needed because we might reset _after_ a poll,
        // and otherwise the task won't be awoken to poll again.
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    cfg_test_util! {
        pub(super) fn did_wake(&self) -> bool {
            match &self.inner {
                Inner::Traditional { did_wake, .. } => did_wake.swap(false, Ordering::SeqCst),
                #[cfg(all(tokio_unstable, feature = "rt-multi-thread"))]
                Inner::Alternative { did_wake, .. } => did_wake.swap(false, Ordering::SeqCst),
            }
        }

        /// Fast-path check for the scheduler's drain-park hook: are any quiesce
        /// waiters registered?
        ///
        /// A single relaxed load; when this returns `false` the hook does nothing
        /// else.
        pub(crate) fn has_quiesce_waiters(&self) -> bool {
            self.quiesce_waiter_count.load(Ordering::Relaxed) > 0
        }

        /// Registers a quiesce waiter with an optional inclusive bound (as an
        /// `Instant`; converted to exact nanoseconds since driver start, no
        /// round-up).
        ///
        /// Refuses the registration when another step is in progress (an
        /// unresolved waiter is already registered): resolving a step moves the
        /// clock to that step's bound, so only one step may be in progress at a
        /// time. A resolved-but-uncollected waiter does not block registration --
        /// its step is over; the entry is only a mailbox its future has yet to
        /// drain.
        ///
        /// The shutdown check happens under the registry lock: `Driver::shutdown`
        /// stores the shutdown flag before taking this same lock to drain the
        /// registry, so a registration that observes the flag unset is guaranteed
        /// to land before the drain (and be woken by it), while one that observes
        /// it set must not land at all -- a waiter registered after the drain would
        /// never be woken.
        ///
        /// The caller is responsible for unparking the target runtime's driver
        /// afterwards so a parked runtime notices the new waiter; this handle alone
        /// cannot do that (it can only set the time driver's `did_wake` flag, not
        /// wake the runtime thread).
        pub(crate) fn register_quiesce_waiter(
            &self,
            bound: Option<crate::time::Instant>,
            waker: &std::task::Waker,
        ) -> QuiesceRegister {
            let bound_ns = bound.map(|b| self.time_source.instant_to_nanos(b));

            let mut lock = self.inner.lock();

            if self.is_shutdown() {
                return QuiesceRegister::Shutdown;
            }

            if lock.quiesce_waiters.iter().any(|w| w.result.is_none()) {
                return QuiesceRegister::Busy;
            }

            let id = lock.next_quiesce_waiter_id;
            lock.next_quiesce_waiter_id += 1;
            lock.quiesce_waiters.push(QuiesceWaiter {
                id,
                bound: bound_ns,
                waker: waker.clone(),
                result: None,
            });
            // Increment under the lock so the scheduler's (lock-free) fast path can
            // never observe count > 0 without the registry entry being visible once
            // it takes the lock.
            self.quiesce_waiter_count.fetch_add(1, Ordering::Relaxed);
            drop(lock);

            QuiesceRegister::Registered(id)
        }

        /// Polls a registered waiter: if it has resolved, removes it and returns the
        /// report; otherwise refreshes its waker.
        ///
        /// Returns [`QuiescePoll::Missing`] if the waiter is not in the registry,
        /// which happens when the driver shut down (and drained the registry)
        /// concurrently with this poll. The caller decides how to surface that.
        pub(crate) fn poll_quiesce_waiter(
            &self,
            id: u64,
            waker: &std::task::Waker,
        ) -> QuiescePoll {
            let mut lock = self.inner.lock();
            let idx = match lock.quiesce_waiters.iter().position(|w| w.id == id) {
                Some(idx) => idx,
                None => return QuiescePoll::Missing,
            };

            match lock.quiesce_waiters[idx].result {
                Some(result) => {
                    lock.quiesce_waiters.swap_remove(idx);
                    self.quiesce_waiter_count.fetch_sub(1, Ordering::Relaxed);
                    QuiescePoll::Ready(result)
                }
                None => {
                    if !lock.quiesce_waiters[idx].waker.will_wake(waker) {
                        lock.quiesce_waiters[idx].waker = waker.clone();
                    }
                    QuiescePoll::Pending
                }
            }
        }

        /// Removes a registered waiter (called when a `Quiesce` future is dropped
        /// before collecting its result). Idempotent.
        pub(crate) fn deregister_quiesce_waiter(&self, id: u64) {
            let mut lock = self.inner.lock();
            if let Some(idx) = lock.quiesce_waiters.iter().position(|w| w.id == id) {
                lock.quiesce_waiters.swap_remove(idx);
                self.quiesce_waiter_count.fetch_sub(1, Ordering::Relaxed);
            }
        }

        /// Resolution pass run by the `current_thread` scheduler's drain-park hook.
        ///
        /// Caller contract (enforced by the hook, not re-checked here): nothing is
        /// runnable, no blocking task is outstanding, and a zero-timeout driver poll
        /// has just completed (so all timers due at the current virtual time have
        /// fired).
        ///
        /// At most one unresolved waiter can be registered (registration refuses a
        /// second in-progress step). It resolves when its bound lies strictly below
        /// the earliest pending deadline across the exact store and the wheel — or
        /// unconditionally when both are empty. On resolution the clock is advanced
        /// to land exactly on the bound (when the bound lies ahead of the clock):
        /// the resolution condition proves every pending deadline lies strictly
        /// beyond the bound, so the move crosses no timer and fires nothing — the
        /// runtime is exactly as quiescent after the move as before it. An
        /// unbounded waiter, or a bound at or before the clock's position, leaves
        /// the clock untouched.
        ///
        /// Returns true if the waiter was resolved (the caller must then SKIP the
        /// park).
        ///
        /// The clock advance happens under the registry lock, in the same
        /// inner-then-clock nesting order `park_thread_timeout` uses for
        /// `try_auto_advance`. The waker is invoked only after the lock is dropped
        /// (the same lock-safety rule `process_at_time` follows).
        pub(crate) fn resolve_quiesce_waiter(&self, clock: &Clock) -> bool {
            let mut lock = self.inner.lock();

            let next_pending_ns = next_pending_ns(&lock);
            let wheel_next = lock.wheel.next_expiration_time();
            let store_next = lock.exact.next_deadline();

            let Some(waiter) = lock
                .quiesce_waiters
                .iter_mut()
                .find(|w| w.result.is_none())
            else {
                return false;
            };

            let resolves = match (waiter.bound, next_pending_ns) {
                // No pending timers in either structure: the waiter (bounded or
                // not) resolves.
                (_, None) => true,
                // Unbounded waiter, timers pending: keep waiting.
                (None, Some(_)) => false,
                // Bounded waiter: resolves iff every pending timer lies strictly
                // beyond the bound (both in ns; the strict `<` is what makes the
                // caller-facing bound inclusive).
                (Some(bound), Some(next)) => bound < next,
            };

            if !resolves {
                return false;
            }

            // Land the clock exactly on the bound. The clock is necessarily still
            // paused: `resume()` panics while a waiter is registered.
            if let Some(bound_ns) = waiter.bound {
                let now_ns = self.time_source.instant_to_nanos(clock.now());
                if bound_ns > now_ns {
                    clock
                        .advance(Duration::from_nanos(bound_ns - now_ns))
                        .expect("clock must be paused while a quiesce step is registered");
                }
            }
            let now = clock.now();

            // next_timer report: exact Instant when the minimum comes from
            // the store, tick-aligned lower bound when it comes from the
            // wheel (unchanged contract for wheel residents). Computed from the
            // pre-advance wheel state, which the clock move cannot change: no
            // deadline at or before the bound exists.
            let next_timer = match (store_next, wheel_next) {
                (Some(s), Some(w)) if s <= w.saturating_mul(1_000_000) => {
                    Some(self.time_source.nanos_to_instant(s))
                }
                (Some(s), None) => Some(self.time_source.nanos_to_instant(s)),
                (_, Some(w)) => Some(self.time_source.tick_to_instant(w)),
                (None, None) => None,
            };

            waiter.result = Some(crate::time::QuiescedState { now, next_timer });
            let waker = waiter.waker.clone();

            drop(lock);

            // Wake outside the lock: a waker may run arbitrary code (task scheduling),
            // and waking under the registry/wheel lock risks lock-order inversions.
            waker.wake();

            true
        }
    }
}

// ===== impl Inner =====

impl Inner {
    /// Locks the driver's inner structure
    pub(super) fn lock(&self) -> crate::loom::sync::MutexGuard<'_, InnerState> {
        match self {
            Inner::Traditional { state, .. } => state.lock(),
            #[cfg(all(tokio_unstable, feature = "rt-multi-thread"))]
            Inner::Alternative { .. } => unreachable!("unreachable in alternative timer"),
        }
    }

    // Check whether the driver has been shutdown
    pub(super) fn is_shutdown(&self) -> bool {
        match self {
            Inner::Traditional { is_shutdown, .. } => is_shutdown.load(Ordering::SeqCst),
            #[cfg(all(tokio_unstable, feature = "rt-multi-thread"))]
            Inner::Alternative { is_shutdown, .. } => is_shutdown.load(Ordering::SeqCst),
        }
    }
}

impl fmt::Debug for Inner {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Inner").finish()
    }
}

#[cfg(test)]
mod tests;
