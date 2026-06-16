mod yield_now;

use crate::loom::sync::atomic::{AtomicUsize, Ordering};
use crate::loom::sync::Arc;
use crate::loom::thread;
use crate::runtime::{Builder, Runtime};
use crate::sync::oneshot::{self, Receiver};
use crate::task;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Duration;

fn assert_at_most_num_polls(rt: Arc<Runtime>, at_most_polls: usize) {
    let (tx, rx) = oneshot::channel();
    let num_polls = Arc::new(AtomicUsize::new(0));
    rt.spawn(async move {
        for _ in 0..12 {
            task::yield_now().await;
        }
        tx.send(()).unwrap();
    });

    rt.block_on(async {
        BlockedFuture {
            rx,
            num_polls: num_polls.clone(),
        }
        .await;
    });

    let polls = num_polls.load(Acquire);
    assert!(polls <= at_most_polls);
}

#[test]
fn block_on_num_polls() {
    loom::model(|| {
        // we expect at most 4 number of polls because there are three points at
        // which we poll the future and an opportunity for a false-positive.. At
        // any of these points it can be ready:
        //
        // - when we fail to steal the parker and we block on a notification
        //   that it is available.
        //
        // - when we steal the parker and we schedule the future
        //
        // - when the future is woken up and we have ran the max number of tasks
        //   for the current tick or there are no more tasks to run.
        //
        // - a thread is notified that the parker is available but a third
        //   thread acquires it before the notified thread can.
        //
        let at_most = 4;

        let rt1 = Arc::new(Builder::new_current_thread().build().unwrap());
        let rt2 = rt1.clone();
        let rt3 = rt1.clone();

        let th1 = thread::spawn(move || assert_at_most_num_polls(rt1, at_most));
        let th2 = thread::spawn(move || assert_at_most_num_polls(rt2, at_most));
        let th3 = thread::spawn(move || assert_at_most_num_polls(rt3, at_most));

        th1.join().unwrap();
        th2.join().unwrap();
        th3.join().unwrap();
    });
}

#[test]
fn assert_no_unnecessary_polls() {
    loom::model(|| {
        // // After we poll outer future, woken should reset to false
        let rt = Builder::new_current_thread().build().unwrap();
        let (tx, rx) = oneshot::channel();
        let pending_cnt = Arc::new(AtomicUsize::new(0));

        rt.spawn(async move {
            for _ in 0..24 {
                task::yield_now().await;
            }
            tx.send(()).unwrap();
        });

        let pending_cnt_clone = pending_cnt.clone();
        rt.block_on(async move {
            // use task::yield_now() to ensure woken set to true
            // ResetFuture will be polled at most once
            // Here comes two cases
            // 1. recv no message from channel, ResetFuture will be polled
            //    but get Pending and we record ResetFuture.pending_cnt ++.
            //    Then when message arrive, ResetFuture returns Ready. So we
            //    expect ResetFuture.pending_cnt = 1
            // 2. recv message from channel, ResetFuture returns Ready immediately.
            //    We expect ResetFuture.pending_cnt = 0
            task::yield_now().await;
            ResetFuture {
                rx,
                pending_cnt: pending_cnt_clone,
            }
            .await;
        });

        let pending_cnt = pending_cnt.load(Acquire);
        assert!(pending_cnt <= 1);
    });
}

#[test]
fn drop_jh_during_schedule() {
    unsafe fn waker_clone(ptr: *const ()) -> RawWaker {
        let atomic = unsafe { &*(ptr as *const AtomicUsize) };
        atomic.fetch_add(1, Ordering::Relaxed);
        RawWaker::new(ptr, &VTABLE)
    }
    unsafe fn waker_drop(ptr: *const ()) {
        let atomic = unsafe { &*(ptr as *const AtomicUsize) };
        atomic.fetch_sub(1, Ordering::Relaxed);
    }
    unsafe fn waker_nop(_ptr: *const ()) {}

    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(waker_clone, waker_drop, waker_nop, waker_drop);

    loom::model(|| {
        let rt = Builder::new_current_thread().build().unwrap();

        let mut jh = rt.spawn(async {});
        // Using AbortHandle to increment task refcount. This ensures that the waker is not
        // destroyed due to the refcount hitting zero.
        let task_refcnt = jh.abort_handle();

        let waker_refcnt = AtomicUsize::new(1);
        {
            // Set up the join waker.
            use std::future::Future;
            use std::pin::Pin;

            // SAFETY: Before `waker_refcnt` goes out of scope, this test asserts that the refcnt
            // has dropped to zero.
            let join_waker = unsafe {
                Waker::from_raw(RawWaker::new(
                    (&waker_refcnt) as *const AtomicUsize as *const (),
                    &VTABLE,
                ))
            };

            assert!(Pin::new(&mut jh)
                .poll(&mut Context::from_waker(&join_waker))
                .is_pending());
        }
        assert_eq!(waker_refcnt.load(Ordering::Relaxed), 1);

        let bg_thread = loom::thread::spawn(move || drop(jh));
        rt.block_on(crate::task::yield_now());
        bg_thread.join().unwrap();

        assert_eq!(waker_refcnt.load(Ordering::Relaxed), 0);
        drop(task_refcnt);
    });
}

struct BlockedFuture {
    rx: Receiver<()>,
    num_polls: Arc<AtomicUsize>,
}

impl Future for BlockedFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.num_polls.fetch_add(1, Release);

        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Pending => Poll::Pending,
            _ => Poll::Ready(()),
        }
    }
}

struct ResetFuture {
    rx: Receiver<()>,
    pending_cnt: Arc<AtomicUsize>,
}

impl Future for ResetFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Pending => {
                self.pending_cnt.fetch_add(1, Release);
                Poll::Pending
            }
            _ => Poll::Ready(()),
        }
    }
}

#[test]
fn quiesce_vs_cross_thread_schedule() {
    use crate::runtime::tests::loom_oneshot;

    loom::model(|| {
        let rt = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        let handle = rt.handle().clone();

        let (tx, rx) = loom_oneshot::channel();

        // Remote thread: spawns a task onto the runtime (inject queue + unpark),
        // racing with the drain-park's resolution decision below.
        let th = loom::thread::spawn(move || {
            handle.spawn(async move {
                tx.send(());
            });
        });

        // Step 1: races with the remote spawn. May resolve before or after the
        // injected task lands; either is correct.
        let _ = rt.block_on(crate::time::quiesce());

        // After join, the spawn has definitely been pushed.
        th.join().unwrap();

        // Step 2: the injected task (if it has not already run) must run before this
        // unbounded quiesce resolves: the scheduler drains the inject queue before
        // parking, and the drain-park hook re-checks it before resolving waiters.
        let _ = rt.block_on(crate::time::quiesce());

        // Proves the task ran in some step (was never dropped/lost). If the runtime
        // lost the cross-thread spawn entirely, this recv never completes and loom
        // reports a deadlock.
        let () = rx.recv();
    });
}

#[test]
fn quiesce_blocking_release_vs_park() {
    loom::model(|| {
        let rt = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();

        let bound = {
            let _enter = rt.handle().enter();
            crate::time::Instant::now() + Duration::from_millis(1)
        };

        // A timer exactly at the bound: firing it requires auto-advance, which the
        // outstanding blocking task inhibits until it completes.
        {
            let _enter = rt.handle().enter();
            rt.handle().spawn(async {
                crate::time::sleep(Duration::from_millis(1)).await;
            });
        }

        // The inhibit is taken synchronously at spawn; the blocking-pool thread's
        // release (decrement count -> unpark) races the runtime's park below.
        let blocking_jh = {
            let _enter = rt.handle().enter();
            crate::task::spawn_blocking(|| {})
        };

        // Must complete under every interleaving (a missed unpark or a resolution
        // that ignores the pending timer shows up as a loom deadlock or a panic).
        let state = rt.block_on(crate::time::quiesce_until(bound));

        // The timer fired before resolution.
        assert!(state.next_timer.is_none());

        rt.block_on(blocking_jh).unwrap();
    });
}

#[test]
fn quiesce_both_waiter_shapes() {
    loom::model(|| {
        let rt = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();

        // An outstanding blocking task gates both waiters' resolution on a
        // cross-thread event: it inhibits the auto-advance the 1ms timer below
        // needs, and the hook's blocking branch refuses to resolve waiters
        // while it is outstanding.
        let bound = {
            let _enter = rt.handle().enter();
            crate::time::Instant::now() + Duration::from_millis(2)
        };

        // A timer at 1ms (inside both bounds).
        {
            let _enter = rt.handle().enter();
            rt.handle().spawn(async {
                crate::time::sleep(Duration::from_millis(1)).await;
            });
        }

        // Spawned-task waiter.
        let waiter_jh = {
            let _enter = rt.handle().enter();
            rt.handle()
                .spawn(async move { crate::time::quiesce_until(bound).await })
        };

        // The blocking-pool thread's release (inhibit decrement + unpark) races
        // with the runtime registering the waiters, parking, and auto-advancing.
        let blocking_jh = {
            let _enter = rt.handle().enter();
            crate::task::spawn_blocking(|| {})
        };

        // Root-future waiter: block_on the spawned waiter's JoinHandle; the root
        // future itself then awaits a quiesce as well. Neither shape can resolve
        // before the blocking task's release (a missed release/unpark shows up as
        // a loom deadlock).
        let spawned_report = rt.block_on(waiter_jh).unwrap();
        let root_report = rt.block_on(crate::time::quiesce_until(bound));

        // Both shapes saw the timer fire and the same final clock position.
        assert_eq!(spawned_report.now, root_report.now);
        assert!(spawned_report.next_timer.is_none());
        assert!(root_report.next_timer.is_none());

        rt.block_on(blocking_jh).unwrap();
    });
}

/// Asserts that a caught panic payload carries the standard runtime-shutting-down
/// message (and not, for example, a tripped `debug_assert`).
fn assert_runtime_shutting_down_panic(payload: Box<dyn std::any::Any + Send>) {
    let msg = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&'static str>().copied())
        .unwrap_or("<non-string panic payload>");
    assert!(
        msg.contains(crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR),
        "unexpected panic message: {msg}"
    );
}

/// Races runtime shutdown against polling a registered `Quiesce` waiter from
/// another thread.
///
/// Verifies the cross-thread ordering claim behind `QuiescePoll::Missing`: the
/// registry is only drained wholesale by driver shutdown, which stores the
/// shutdown flag (`SeqCst`) before taking the registry lock to drain it, so a poll
/// that finds its waiter missing must also observe `is_shutdown() == true` (the
/// `debug_assert` in `Quiesce::poll`'s `Missing` arm). The poll panicking with
/// the standard runtime-shutting-down message is the expected outcome whenever
/// it observes the shutdown; tripping the `debug_assert` is the bug this model
/// exists to catch.
#[test]
fn quiesce_poll_vs_shutdown() {
    use futures::task::noop_waker_ref;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    loom::model(|| {
        let rt = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();

        // Register a waiter. A noop waker suffices: this model never relies on the
        // wake being delivered, only on the poll outcomes below.
        let mut quiesce = Box::pin(crate::time::quiesce());
        {
            let _enter = rt.handle().enter();
            let mut cx = Context::from_waker(noop_waker_ref());
            assert!(quiesce.as_mut().poll(&mut cx).is_pending());
        }

        // Shut the runtime down from another thread, racing the poll below.
        let th = loom::thread::spawn(move || {
            drop(rt);
        });

        // The poll either runs before the shutdown becomes observable (Pending) or
        // observes it and panics with RUNTIME_SHUTTING_DOWN_ERROR. It can never
        // resolve (the waiter registry is only resolved at a drain park, which the
        // shutdown path does not reach), and it must never panic with anything
        // else -- in particular not the `debug_assert` in the `Missing` arm.
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut cx = Context::from_waker(noop_waker_ref());
            quiesce.as_mut().poll(&mut cx)
        }));

        match result {
            Ok(Poll::Pending) => {}
            Ok(Poll::Ready(_)) => panic!("quiesce waiter resolved during shutdown"),
            Err(payload) => assert_runtime_shutting_down_panic(payload),
        }

        th.join().unwrap();
    });
}

/// Races runtime shutdown against the FIRST poll (registration) of a `Quiesce`
/// future from another thread.
///
/// Registration checks the shutdown flag under the waiter-registry lock, and
/// driver shutdown stores that flag (`SeqCst`) before taking the same lock to
/// drain the registry. Exactly two outcomes are therefore possible:
///
/// - registration observes the shutdown and the first poll panics with the
///   standard runtime-shutting-down message, or
/// - registration lands before the drain; the drain then removes the waiter and
///   wakes it, and the next poll panics with that same message.
///
/// What must never happen: a `Pending` first poll whose waiter is never woken (an
/// awaiting caller would hang forever), a waiter that resolves with a report, or a
/// tripped `debug_assert` in `Quiesce::poll`'s `Missing` arm.
#[test]
fn quiesce_first_poll_vs_shutdown() {
    use futures::task::ArcWake;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Counts wake calls. Uses std (not loom) types throughout: `ArcWake` requires a
    // `std::sync::Arc`, and the count is only read after `th.join()`, whose
    // happens-before edge makes the value well defined without loom tracking it.
    struct CountingWaker {
        wakes: AtomicUsize,
    }

    impl ArcWake for CountingWaker {
        fn wake_by_ref(arc_self: &std::sync::Arc<Self>) {
            arc_self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    loom::model(|| {
        let rt = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        let handle = rt.handle().clone();

        // Shut the runtime down from another thread, racing the first poll below.
        let th = loom::thread::spawn(move || {
            drop(rt);
        });

        let counting_waker = std::sync::Arc::new(CountingWaker {
            wakes: AtomicUsize::new(0),
        });
        let waker = futures::task::waker(counting_waker.clone());

        let mut quiesce = Box::pin(crate::time::quiesce());

        // First poll: the registration races the shutdown.
        let first = catch_unwind(AssertUnwindSafe(|| {
            let _enter = handle.enter();
            let mut cx = Context::from_waker(&waker);
            quiesce.as_mut().poll(&mut cx)
        }));

        // After this join the shutdown -- including its registry drain -- is
        // complete.
        th.join().unwrap();

        match first {
            // Registration observed the shutdown and was refused.
            Err(payload) => assert_runtime_shutting_down_panic(payload),

            // Registration landed before the drain.
            Ok(Poll::Pending) => {
                // The drain woke the waiter; without this wake an awaiting caller
                // would never be polled again and would hang forever.
                assert!(
                    counting_waker.wakes.load(Ordering::SeqCst) > 0,
                    "waiter registered on a shutting-down driver was never woken"
                );

                // The next poll surfaces the shutdown; `Pending` (the hang), a
                // resolved report, and the `Missing`-arm `debug_assert` are all
                // bugs.
                let second = catch_unwind(AssertUnwindSafe(|| {
                    let mut cx = Context::from_waker(&waker);
                    quiesce.as_mut().poll(&mut cx)
                }));
                match second {
                    Err(payload) => assert_runtime_shutting_down_panic(payload),
                    Ok(poll) => {
                        panic!("poll after shutdown returned {poll:?} instead of panicking")
                    }
                }
            }

            Ok(Poll::Ready(_)) => panic!("quiesce waiter resolved during shutdown"),
        }
    });
}

/// Races a cross-thread `quiesce_until` registration against an in-flight
/// auto-advance.
///
/// The invariant under test: once the first poll (registration) has returned
/// and the registering thread has observed the clock at or below its bound,
/// the step must resolve at or below that bound — an advance decided before
/// the registration landed must not move the clock across it afterwards.
/// (When the advance wins the race outright, the post-registration
/// observation already exceeds the bound and the assertion is vacuous.)
#[test]
fn quiesce_register_vs_auto_advance() {
    use crate::runtime::tests::loom_oneshot;
    use futures::task::noop_waker_ref;

    loom::model(|| {
        let rt = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        let handle = rt.handle().clone();

        let (bound, sleep_jh) = {
            let _enter = rt.handle().enter();
            let bound = crate::time::Instant::now() + Duration::from_millis(1);
            // A timer strictly beyond the bound: the auto-advance target.
            let jh = rt.handle().spawn(async {
                crate::time::sleep(Duration::from_millis(5)).await;
            });
            (bound, jh)
        };

        let (tx, rx) = loom_oneshot::channel();
        let th = loom::thread::spawn(move || {
            // First poll registers the waiter, racing the runtime thread's
            // park/advance cycle below.
            let mut quiesce = Box::pin(crate::time::quiesce_until(bound));
            {
                let _enter = handle.enter();
                let mut cx = Context::from_waker(noop_waker_ref());
                assert!(quiesce.as_mut().poll(&mut cx).is_pending());
            }

            // Clock position observed after registration completed.
            let observed = {
                let _enter = handle.enter();
                crate::time::Instant::now()
            };

            tx.send((quiesce, observed));
        });

        // Drive the runtime through the 5ms sleep (fires via auto-advance; a
        // registered waiter must not block it forever — resolution unblocks).
        rt.block_on(async { sleep_jh.await.unwrap() });

        let (quiesce, observed) = rx.recv();
        th.join().unwrap();

        // Collect the report (the waiter resolved during the block_on above
        // or resolves here on an idle runtime).
        let report = rt.block_on(quiesce);

        if observed <= bound {
            assert!(
                report.now <= bound,
                "step crossed its bound after registration: observed {observed:?} <= bound \
                 {bound:?}, but resolved at {:?}",
                report.now,
            );
        }
    });
}
