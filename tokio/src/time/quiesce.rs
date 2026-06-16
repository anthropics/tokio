//! Run a paused runtime until it is quiescent.
//!
//! See [`quiesce`] and [`quiesce_until`] for details.

use crate::runtime::scheduler;
use crate::runtime::time::{QuiescePoll, QuiesceRegister};
use crate::time::Instant;

use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};

/// Report produced when a [`Quiesce`] future resolves.
///
/// Constructed by the runtime; there is no public constructor.
///
/// [`Quiesce`]: struct@Quiesce
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct QuiescedState {
    /// The virtual instant at which the runtime quiesced.
    ///
    /// For [`quiesce_until`], the later of the call's `deadline` and the clock's
    /// position when the step began: resolution lands the clock exactly on the
    /// deadline, and a deadline at or before the clock's position leaves the
    /// clock unchanged. For unbounded [`quiesce`], the clock rests where the last
    /// fired timer left it (there is no deadline to land on).
    ///
    /// Always equal to `Instant::now()` observed from within the runtime's
    /// context — inside a task, or under [`Runtime::enter`] — immediately after
    /// the resolving call returns. (Outside the runtime context, `Instant::now()`
    /// reads the real system clock, not the paused virtual clock.)
    ///
    /// [`Runtime::enter`]: crate::runtime::Runtime::enter
    /// [`quiesce`]: crate::time::quiesce()
    pub now: Instant,

    /// The earliest pending timer deadline strictly after `now`, or `None` if no
    /// timers remain registered with the runtime.
    ///
    /// Reported at nanosecond precision. A timer that has been extended via
    /// [`Sleep::reset`] to a later deadline, and whose wheel slot the driver has
    /// not yet revisited, is reported at its pre-extend (registered) deadline — a
    /// lower bound on its actual fire time; the next step that reaches that slot
    /// re-keys it. Treat the value as a snapshot taken at resolution time: the
    /// reported timer may be cancelled or extended afterward.
    ///
    /// [`Sleep::reset`]: crate::time::Sleep::reset
    pub next_timer: Option<Instant>,
}

pin_project! {
    /// Future returned by [`quiesce`] and [`quiesce_until`].
    ///
    /// Resolves with a [`QuiescedState`] report when the paused `current_thread`
    /// runtime that polls it has nothing left to do at or before the requested
    /// virtual-time bound.
    ///
    /// The future is bound to the runtime that first polls it. Validation and
    /// registration happen at that first poll — not at construction — so the future
    /// can be created outside a runtime context, for example as the argument to
    /// [`Runtime::block_on`].
    ///
    /// Dropping a `Quiesce` that has not yet resolved deregisters it from that
    /// runtime, ending the step (a new step may then begin); the runtime's
    /// subsequent auto-advance behavior is unchanged.
    ///
    /// [`Runtime::block_on`]: crate::runtime::Runtime::block_on
    #[project(!Unpin)]
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Quiesce {
        // Inclusive virtual-time bound; `None` means unbounded (resolve only when
        // no timers remain).
        bound: Option<Instant>,

        // Registration state. All validation happens on first poll so that
        // `rt.block_on(time::quiesce_until(..))` works (the future is constructed
        // before `block_on` establishes the runtime context).
        state: State,
    }

    impl PinnedDrop for Quiesce {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let State::Registered { id, ref handle } = *this.state {
                // Deregister from the originating runtime's time driver. The driver
                // handle was captured at registration, so this targets the right
                // runtime regardless of the ambient context (idempotent if the
                // waiter was already collected or the driver shut down).
                if let Some(time_handle) = handle.driver().time.as_ref() {
                    time_handle.deregister_quiesce_waiter(id);
                }
            }
        }
    }
}

#[derive(Debug)]
enum State {
    /// Created; not yet polled.
    Init,
    /// Registered with the time driver of the runtime that first polled this future.
    Registered { id: u64, handle: scheduler::Handle },
    /// Resolved; the report has been collected and returned.
    Done,
}

/// Waits until the paused runtime has no work left to do and no pending timers
/// remain.
///
/// Equivalent to [`quiesce_until`] with no bound: the future resolves only once the
/// runtime's timer wheel is empty, nothing is runnable, and no [`spawn_blocking`]
/// task spawned on this runtime is outstanding. The resolution contract, supported
/// calling patterns, and determinism caveats documented on [`quiesce_until`] all
/// apply here, with "at or before the bound" read as "at any point in the future".
///
/// With no bound there is no deadline for the clock to land on: the clock rests
/// wherever the last fired timer left it ([`quiesce_until`], by contrast, lands
/// the clock exactly on its deadline).
///
/// **Important:** a workload with a recurring timer — an [`interval`], or any task
/// that re-arms a [`sleep`] each time it fires — never empties the timer wheel, so
/// an unbounded `quiesce()` never resolves on it. `quiesce()` is intended for
/// workloads that finish on their own; stepped simulations and workloads with
/// periodic timers should use [`quiesce_until`].
///
/// # Panics
///
/// The returned future panics when polled if:
/// - first polled outside a Tokio runtime context (validation and registration
///   happen at the first poll; later polls target the registered runtime
///   regardless of the ambient context),
/// - the runtime is not the `current_thread` flavor,
/// - the runtime's clock is not paused,
/// - the runtime has no time driver (`enable_time()`/`enable_all()` not called),
/// - the runtime has shut down,
/// - another quiesce step is already in progress on this runtime (only one step
///   may be in progress at a time; a step ends when its future resolves or is
///   dropped), or
/// - polled again after it has resolved.
///
/// While the future is registered (polled at least once and not yet resolved),
/// calling [`resume`] or [`advance`] on the runtime panics: an explicit clock change
/// during a quiesce step would move the clock out from under the step.
///
/// # Examples
///
/// ```
/// use tokio::time::{self, Duration};
///
/// #[tokio::main(flavor = "current_thread", start_paused = true)]
/// async fn main() {
///     tokio::spawn(async {
///         time::sleep(Duration::from_millis(10)).await;
///     });
///
///     // Runs the spawned task (firing its timer); resolves once no pending
///     // timers remain and nothing is runnable.
///     let state = time::quiesce().await;
///     assert!(state.next_timer.is_none());
/// }
/// ```
///
/// [`interval`]: crate::time::interval()
/// [`sleep`]: crate::time::sleep()
/// [`spawn_blocking`]: crate::task::spawn_blocking
/// [`resume`]: crate::time::resume
/// [`advance`]: crate::time::advance
pub fn quiesce() -> Quiesce {
    Quiesce {
        bound: None,
        state: State::Init,
    }
}

/// Waits until the paused runtime has run everything that can possibly happen at or
/// before `deadline` (inclusive), lands the virtual clock exactly on `deadline`,
/// and reports when the next pending timer is due.
///
/// When the returned [`Quiesce`] future resolves, all of the following held at the
/// moment of resolution: nothing was runnable, no [`spawn_blocking`] task spawned on
/// this runtime was outstanding, and every timer with a deadline at or before
/// `deadline` had fired. This is a statement about the past, not a promise
/// about the future: a wake delivered from outside the runtime after resolution (a
/// message sent by another thread, for example) is the caller's to manage —
/// typically by stepping again.
///
/// The bound is inclusive: a timer with a deadline exactly equal to `deadline` fires
/// within the call. Callers building windowing protocols can layer their own
/// half-open convention on top of this.
///
/// A `deadline` at or before the current virtual time resolves as soon as nothing is
/// runnable, with the clock unchanged — a cheap "drain without advancing". (The
/// clock never moves backward.)
///
/// # Where the clock ends up
///
/// When the future resolves, the clock reads exactly `deadline` (or its prior
/// position, for a `deadline` already at or before it). During the step the clock
/// visits intermediate positions only to fire pending timers — each at its exact
/// nanosecond deadline — and then takes one final hop to `deadline`. That last
/// hop is safe by construction: the future only resolves once every pending timer
/// lies strictly beyond `deadline`, so the hop crosses no timer and fires nothing.
///
/// [`QuiescedState::now`] reports the final position and always equals
/// [`Instant::now()`] observed from within the runtime's context (inside a task,
/// or under [`Runtime::enter`]) after the call returns — outside the runtime
/// context, `Instant::now()` reads the real system clock instead of the paused
/// virtual clock.
///
/// [`QuiescedState::next_timer`] is the earliest pending timer's registered
/// deadline strictly after `now` (see the field's doc for the one inexact
/// case), and is `None` exactly when no timers remain.
///
/// # Supported calling patterns
///
/// Step the runtime either by passing this future to [`Runtime::block_on`] on the
/// thread that drives the runtime — the typical shape for a stepping loop driven
/// from synchronous test code — or by awaiting it inside a task spawned on the
/// runtime.
///
/// **Important:** [`Handle::block_on`] does not drive a `current_thread` runtime's
/// scheduler or its IO and timer drivers. A `Quiesce` awaited there does not resolve
/// unless another thread is concurrently driving the runtime with
/// [`Runtime::block_on`]. See [`Handle::block_on`] for details.
///
/// # Determinism caveats
///
/// Quiescence stepping makes the runtime's virtual-time scheduling observable and
/// repeatable; it does not by itself make a workload deterministic:
///
/// - [`spawn_blocking`] work runs on real operating-system threads. A step waits for
///   outstanding blocking tasks so their effects are not lost, but how long that
///   takes — and how blocking work interleaves with other threads — is real-world
///   timing, not virtual time.
/// - A task that re-wakes itself in a loop (for example by calling [`yield_now`]
///   repeatedly) never lets the runtime quiesce, so a step never resolves. Tasks
///   must wait on real wakeups: timers, [`Notify`], channels, or IO.
/// - When several [`select!`] branches are ready at once, the winning branch is
///   randomized. Seed the runtime's random number generator (`Builder::rng_seed`, a
///   `tokio_unstable` API) to make that choice reproducible.
///
/// # Panics
///
/// The returned future panics when polled if:
/// - first polled outside a Tokio runtime context (validation and registration
///   happen at the first poll; later polls target the registered runtime
///   regardless of the ambient context),
/// - the runtime is not the `current_thread` flavor,
/// - the runtime's clock is not paused,
/// - the runtime has no time driver (`enable_time()`/`enable_all()` not called),
/// - the runtime has shut down,
/// - another quiesce step is already in progress on this runtime (resolution
///   lands the clock on the step's deadline, and the clock can only land on one;
///   a step ends when its future resolves or is dropped), or
/// - polled again after it has resolved.
///
/// While the future is registered (polled at least once and not yet resolved),
/// calling [`resume`] or [`advance`] on the runtime panics: an explicit clock change
/// during a quiesce step would move the clock past the step's bound.
///
/// # Examples
///
/// Stepping a paused runtime one window at a time from outside the runtime:
///
/// ```
/// use tokio::time::{self, Duration, Instant};
///
/// let rt = tokio::runtime::Builder::new_current_thread()
///     .enable_time()
///     .start_paused(true)
///     .build()
///     .unwrap();
///
/// let start = {
///     let _enter = rt.enter();
///     Instant::now()
/// };
///
/// rt.spawn(async move {
///     time::sleep_until(start + Duration::from_millis(15)).await;
///     // ... work that happens at t = 15ms ...
/// });
///
/// // Window 1: nothing is due at or before 10ms; the clock lands on the bound.
/// let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(10)));
/// assert_eq!(state.now, start + Duration::from_millis(10));
/// assert_eq!(state.next_timer, Some(start + Duration::from_millis(15)));
///
/// // Window 2: the 15ms timer fires on the way; the clock lands on the bound.
/// let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(20)));
/// assert_eq!(state.now, start + Duration::from_millis(20));
/// assert_eq!(state.next_timer, None);
/// ```
///
/// Awaiting inside the runtime:
///
/// ```
/// use tokio::time::{self, Duration, Instant};
///
/// #[tokio::main(flavor = "current_thread", start_paused = true)]
/// async fn main() {
///     let start = Instant::now();
///
///     tokio::spawn(async {
///         time::sleep(Duration::from_millis(10)).await;
///     });
///
///     // Run everything due in the first 50ms of virtual time.
///     let state = time::quiesce_until(start + Duration::from_millis(50)).await;
///
///     // The 10ms timer fired on the way; the clock landed on the bound.
///     assert_eq!(state.now, start + Duration::from_millis(50));
///     assert_eq!(Instant::now(), start + Duration::from_millis(50));
/// }
/// ```
///
/// [`spawn_blocking`]: crate::task::spawn_blocking
/// [`yield_now`]: crate::task::yield_now()
/// [`Notify`]: crate::sync::Notify
/// [`select!`]: crate::select
/// [`Runtime::block_on`]: crate::runtime::Runtime::block_on
/// [`Runtime::enter`]: crate::runtime::Runtime::enter
/// [`Handle::block_on`]: crate::runtime::Handle::block_on
/// [`Instant::now()`]: crate::time::Instant::now
/// [`resume`]: crate::time::resume
/// [`advance`]: crate::time::advance
pub fn quiesce_until(deadline: Instant) -> Quiesce {
    Quiesce {
        bound: Some(deadline),
        state: State::Init,
    }
}

impl Quiesce {
    /// First-poll validation and registration. Returns the registration id and the
    /// scheduler handle.
    #[track_caller]
    fn register(bound: Option<Instant>, waker: &task::Waker) -> (u64, scheduler::Handle) {
        // Name the entry point the user actually called in panic messages.
        let api = if bound.is_some() {
            "time::quiesce_until()"
        } else {
            "time::quiesce()"
        };

        // Panics with CONTEXT_MISSING_ERROR outside a runtime context.
        let handle = scheduler::Handle::current();

        // Flavor check: quiesce only exists for the current_thread flavor.
        // LocalRuntime also uses the CurrentThread scheduler handle, so it passes.
        match &handle {
            scheduler::Handle::CurrentThread(_) => {}
            #[cfg(feature = "rt-multi-thread")]
            scheduler::Handle::MultiThread(_) => panic!(
                "`{api}` requires the `current_thread` Tokio runtime. \
                 This is the default Runtime used by `#[tokio::test]`."
            ),
        }

        // Time-driver presence check: panics with the existing
        // "timers are disabled" message.
        let time_handle = handle.driver().time();

        // Paused-clock check.
        let clock = handle.driver().clock();
        if !clock.is_paused() {
            panic!(
                "`{api}` requires the runtime's clock to be paused \
                 (see `tokio::time::pause()`)"
            );
        }

        // Registration is refused while another step is in progress (resolution
        // moves the clock to the step's bound, and the clock can only land on one
        // bound) and once the driver is shutting down. Both checks run under the
        // waiter-registry lock; the shutdown check therefore cannot race the
        // shutdown drain: a waiter that landed after the drain would never be
        // woken, hanging the caller forever. Mirror TimerEntry: a quiesce on a
        // shut-down runtime is a bug.
        let id = match time_handle.register_quiesce_waiter(bound, waker) {
            QuiesceRegister::Registered(id) => id,
            QuiesceRegister::Busy => panic!(
                "`{api}` cannot be called while another quiesce step is in progress \
                 on this runtime (only one step may be in progress at a time)"
            ),
            QuiesceRegister::Shutdown => {
                panic!("{}", crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR)
            }
        };

        // This poll may be running on a thread that merely holds a `Handle::enter`
        // guard while the target runtime is parked on its own thread. Nothing else
        // would cause a parked runtime to re-run its drain-park hook and notice the
        // new waiter, so unpark its driver explicitly. (The full driver unpark is
        // needed -- the time handle alone only sets `did_wake` and cannot wake the
        // runtime thread.)
        handle.driver().unpark();

        (id, handle.clone())
    }
}

impl Future for Quiesce {
    type Output = QuiescedState;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<QuiescedState> {
        let this = self.project();

        match this.state {
            State::Init => {
                let (id, handle) = Quiesce::register(*this.bound, cx.waker());
                *this.state = State::Registered { id, handle };
                Poll::Pending
            }
            State::Registered { id, handle } => {
                let time_handle = handle.driver().time();

                // Mirror TimerEntry: a quiesce outliving its runtime is a bug.
                if time_handle.is_shutdown() {
                    panic!("{}", crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR);
                }

                match time_handle.poll_quiesce_waiter(*id, cx.waker()) {
                    QuiescePoll::Ready(report) => {
                        *this.state = State::Done;
                        Poll::Ready(report)
                    }
                    QuiescePoll::Pending => Poll::Pending,
                    QuiescePoll::Missing => {
                        // The registry is only drained wholesale by the driver's
                        // shutdown, which sets the shutdown flag first; the shutdown
                        // raced with the `is_shutdown` check above.
                        debug_assert!(time_handle.is_shutdown());
                        panic!("{}", crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR);
                    }
                }
            }
            State::Done => panic!("`Quiesce` polled after completion"),
        }
    }
}
