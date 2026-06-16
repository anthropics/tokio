//! Benchmarks for externally-driven deterministic stepping of paused runtimes
//! (`tokio::time::quiesce_until`).
//!
//! Measures per-window stepping overhead and multi-core scaling of a balanced
//! synthetic island world: N paused current_thread runtimes, each with a 1 ms
//! periodic timer, a fixed CPU budget per tick, and a ring message to its neighbor,
//! stepped in lookahead-sized windows by a persistent pool of controller worker
//! threads.
//!
//! Requires the `test-util` feature:
//!
//!     cargo bench --features test-util --bench rt_quiesce
//!
//! Methodology notes:
//! - All configurations run with paused clocks, i.e. on the post-pause
//!   `Instant::now()` slow path (process-wide, one-way). This is representative of
//!   real simulation builds; configurations are mutually comparable.
//! - Island timers are 1 ms periodic regardless of the stepping lookahead, so
//!   sub-millisecond lookahead configurations measure stepping overhead (mostly
//!   empty windows), not extra timer work.
//! - The per-tick CPU budget is a fixed iteration count; its wall-clock cost is
//!   measured and printed at startup so results can be read as "this much useful
//!   work per island per 1 ms window".
//! - Controller worker threads form a persistent pool created once per benchmark
//!   iteration, outside the timed section (pool creation is setup, like building
//!   the runtimes). Workers persist across all windows of an iteration and
//!   synchronize with the dispatcher twice per window through spin-synced
//!   atomics (a published window sequence number and a completion count), so
//!   the timed window loop contains no thread spawning and no scheduler
//!   handoffs.

#[cfg(feature = "test-util")]
mod bench {
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use criterion::{black_box, criterion_group, BenchmarkId, Criterion};
    use tokio::runtime::Runtime;
    use tokio::sync::mpsc;
    use tokio::time::{self, Instant};

    /// Fixed CPU budget per island per 1 ms virtual tick: SPIN_ITERS rounds of SplitMix64.
    /// The wall-clock cost is measured and printed once at startup (see `measure_spin_cost`).
    const SPIN_ITERS: u64 = 10_000;

    /// Virtual time horizon simulated per benchmark iteration.
    const VIRTUAL_HORIZON: Duration = Duration::from_millis(20);

    /// Island timers are 1 ms periodic regardless of the stepping lookahead.
    const TICK: Duration = Duration::from_millis(1);

    fn spin(seed: u64) -> u64 {
        // SplitMix64 rounds; black_box prevents the loop from being optimized away.
        let mut x = seed;
        for _ in 0..SPIN_ITERS {
            x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            x = black_box(z ^ (z >> 31));
        }
        x
    }

    /// Measures and returns the wall-clock cost of one spin() call (printed at startup).
    fn measure_spin_cost() -> Duration {
        let start = std::time::Instant::now();
        let mut acc = 0u64;
        for i in 0..1000u64 {
            acc = acc.wrapping_add(spin(i));
        }
        black_box(acc);
        start.elapsed() / 1000
    }

    /// One island: a paused runtime with a periodic-tick task.
    struct Island {
        rt: Runtime,
        start: Instant,
        /// Controller -> island: ring messages due in the upcoming window.
        inbox_tx: mpsc::UnboundedSender<Duration>, // payload = delivery time
        /// Island -> controller: ring messages sent this window (their send times).
        outbox: Arc<Mutex<Vec<Duration>>>,
        /// Count of ring messages received (proves the ring is live; read at teardown).
        received: Arc<AtomicU64>,
    }

    /// Builds one island. `work` controls whether the island has the spin budget
    /// (false for the pure-overhead benchmark).
    fn build_island(id: usize, work: bool, horizon: Duration) -> Island {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();

        let start = {
            let _enter = rt.enter();
            Instant::now()
        };

        let (inbox_tx, mut inbox_rx) = mpsc::unbounded_channel::<Duration>();
        let outbox: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));
        let received = Arc::new(AtomicU64::new(0));

        {
            let _enter = rt.enter();

            // Mailbox: receive ring messages, schedule their observation at delivery time.
            let received_clone = received.clone();
            rt.spawn(async move {
                while let Some(delivery_time) = inbox_rx.recv().await {
                    let received = received_clone.clone();
                    tokio::spawn(async move {
                        time::sleep_until(start + delivery_time).await;
                        received.fetch_add(1, Relaxed);
                    });
                }
            });

            // The periodic worker: tick every 1 ms, spin, send a ring message.
            let outbox_clone = outbox.clone();
            rt.spawn(async move {
                let mut tick = TICK;
                while tick <= horizon {
                    time::sleep_until(start + tick).await;
                    if work {
                        black_box(spin(id as u64 ^ tick.as_millis() as u64));
                    }
                    outbox_clone.lock().unwrap().push(tick);
                    tick += TICK;
                }
            });
        }

        Island {
            rt,
            start,
            inbox_tx,
            outbox,
            received,
        }
    }

    /// State shared between the dispatcher and the persistent pool workers.
    ///
    /// Synchronization is a pair of spin-synced atomics rather than
    /// `std::sync::Barrier`: with tens of workers, the mutex/condvar handoff
    /// inside `Barrier` costs several microseconds per worker per crossing
    /// (every waiter serializes on one mutex and the wakeup goes through the
    /// scheduler), which at 32 workers adds up to hundreds of microseconds per
    /// window and dominates the windows being measured. The atomics keep
    /// per-window synchronization in the single-digit-microsecond range,
    /// independent of worker count.
    struct PoolShared {
        /// Number of pool workers reporting into `done_count`.
        n_workers: usize,
        /// Sequence number of the published window. The dispatcher increments it
        /// to release workers into the next window; workers spin on it.
        window_seq: AtomicU64,
        /// Virtual end of the published window, in nanoseconds since island
        /// start. Written by the dispatcher before it bumps `window_seq`.
        window_end_nanos: AtomicU64,
        /// Number of workers that have finished stepping the published window.
        /// The dispatcher spins on it reaching `n_workers`, then resets it.
        done_count: AtomicUsize,
        /// Set by the dispatcher before the final `window_seq` bump, telling
        /// workers to exit instead of stepping another window.
        shutdown: AtomicBool,
    }

    /// Spins until `ready` returns true. A bounded busy-spin phase keeps
    /// microsecond-scale waits off the scheduler; longer waits fall back to
    /// yielding so oversubscribed configurations (e.g. 64 workers plus the
    /// dispatcher on 64 logical CPUs) stay live.
    fn spin_wait(ready: impl Fn() -> bool) {
        let mut spins = 0u32;
        while !ready() {
            if spins < 1_000 {
                std::hint::spin_loop();
                spins += 1;
            } else {
                std::thread::yield_now();
            }
        }
    }

    /// Releases the pool workers when dropped: sets the shutdown flag and
    /// publishes one final pseudo-window so workers exit their spin loop.
    ///
    /// Shutdown lives in a `Drop` impl so it also runs if the dispatcher's
    /// closure panics; otherwise the enclosing `std::thread::scope` would try
    /// to join workers still spinning on `window_seq`, turning the panic into
    /// a hang.
    struct PoolShutdownGuard<'a> {
        shared: &'a PoolShared,
    }

    impl Drop for PoolShutdownGuard<'_> {
        fn drop(&mut self) {
            self.shared.shutdown.store(true, Relaxed);
            self.shared.window_seq.fetch_add(1, Release);
        }
    }

    /// Dispatcher-side handle to a running worker pool.
    struct PoolHandle<'a> {
        shared: &'a PoolShared,
    }

    impl PoolHandle<'_> {
        /// Steps every island through one window ending at `window_end` (virtual
        /// time since island start). Returns once all workers have finished.
        fn step_window(&self, window_end: Duration) {
            let shared = self.shared;
            shared
                .window_end_nanos
                .store(window_end.as_nanos() as u64, Relaxed);
            // Publish the window. The Release pairs with the workers' Acquire
            // loads of `window_seq`, making `window_end_nanos` and the ring
            // messages delivered by the dispatcher visible to them.
            shared.window_seq.fetch_add(1, Release);
            // Workers step their islands in parallel here. Their Release
            // increments of `done_count` pair with this Acquire load, making the
            // islands' outbox writes visible to the dispatcher.
            spin_wait(|| shared.done_count.load(Acquire) >= shared.n_workers);
            shared.done_count.store(0, Relaxed);
        }
    }

    /// Creates a persistent pool of `threads` workers (clamped to the island count)
    /// stepping `islands` in strided assignment (worker w steps islands w, w+k,
    /// w+2k, ...), runs `f` with a handle to the pool, then shuts the pool down.
    /// Pool creation and teardown happen outside `f`, so `f` can time a window
    /// loop without measuring either.
    fn with_worker_pool<R>(
        islands: &[Island],
        threads: usize,
        f: impl FnOnce(&PoolHandle<'_>) -> R,
    ) -> R {
        let n_workers = threads.min(islands.len()).max(1);
        let shared = PoolShared {
            n_workers,
            window_seq: AtomicU64::new(0),
            window_end_nanos: AtomicU64::new(0),
            done_count: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
        };

        std::thread::scope(|scope| {
            // Shutdown (untimed): when this guard drops, it publishes one final
            // pseudo-window with the shutdown flag set to release the workers;
            // the scope joins them on exit. The guard is created before the
            // spawn loop so it is armed on every exit path: whether `f`
            // returns, `f` panics, or a `scope.spawn` call below panics
            // partway through spawning, already-running workers are released
            // instead of leaving the scope join hanging on workers that never
            // stop spinning.
            let _shutdown = PoolShutdownGuard { shared: &shared };

            for worker in 0..n_workers {
                let shared = &shared;
                scope.spawn(move || {
                    let stepping = || {
                        let mut next_window = 1u64;
                        loop {
                            // Wait for the dispatcher to publish window
                            // `next_window`; shutdown is also signalled through a
                            // `window_seq` bump.
                            spin_wait(|| shared.window_seq.load(Acquire) >= next_window);
                            if shared.shutdown.load(Relaxed) {
                                break;
                            }
                            let window_end =
                                Duration::from_nanos(shared.window_end_nanos.load(Relaxed));
                            let mut idx = worker;
                            while idx < islands.len() {
                                let island = &islands[idx];
                                let deadline = island.start + window_end;
                                let _state = island.rt.block_on(time::quiesce_until(deadline));
                                idx += n_workers;
                            }
                            // Report completion of this window to the dispatcher.
                            shared.done_count.fetch_add(1, Release);
                            next_window += 1;
                        }
                    };
                    // A panicking worker would leave the dispatcher spinning on
                    // `done_count` forever; turn worker panics into a loud process
                    // abort instead of a silent benchmark hang.
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(stepping)).is_err() {
                        eprintln!("rt_quiesce: pool worker panicked; aborting");
                        std::process::abort();
                    }
                });
            }

            f(&PoolHandle { shared: &shared })
        })
    }

    /// Runs the windowed simulation, stepping islands through the persistent
    /// `pool`. Returns the number of windows stepped. ONLY this function is inside
    /// the timed section.
    fn run_world(
        islands: &[Island],
        pool: &PoolHandle<'_>,
        lookahead: Duration,
        horizon: Duration,
    ) -> usize {
        let mut window_end = Duration::ZERO;
        let mut windows = 0usize;
        // Ring messages in flight: (delivery_time, dst) pairs, kept sorted by insertion
        // discipline (delivery times are monotone per source).
        let mut in_flight: Vec<(Duration, usize)> = Vec::new();

        // The simulation must run a fixed amount past the horizon so messages sent in the
        // last tick still get delivered and observed (two lookaheads of slack: one window
        // for the message to come due, one more to step its destination past it).
        let end = horizon + lookahead + lookahead;

        while window_end < end {
            window_end += lookahead;
            windows += 1;

            // (a) Deliver due ring messages.
            in_flight.retain(|&(delivery_time, dst)| {
                if delivery_time <= window_end {
                    // Ignore send errors at teardown (island tasks may have finished).
                    let _ = islands[dst].inbox_tx.send(delivery_time);
                    false
                } else {
                    true
                }
            });

            // (b) Step all islands in parallel (strided assignment across the pool).
            pool.step_window(window_end);

            // (c) Collect outboxes (island-index order) and queue ring messages with
            //     latency exactly == lookahead.
            for (i, island) in islands.iter().enumerate() {
                let sent: Vec<Duration> = island.outbox.lock().unwrap().drain(..).collect();
                let dst = (i + 1) % islands.len();
                for sent_at in sent {
                    in_flight.push((sent_at + lookahead, dst));
                }
            }
        }

        windows
    }

    /// Controller thread counts swept by the benchmarks, clamped to the machine.
    fn parallelism_levels() -> Vec<usize> {
        let max_parallelism = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        [1, 8, 32, 64]
            .into_iter()
            .filter(|&n| n <= max_parallelism)
            .collect()
    }

    /// Total ring messages received across all islands. Read at teardown to verify
    /// the ring was live (or, for the overhead benchmark, that it stayed silent).
    fn total_received(islands: &[Island]) -> u64 {
        islands
            .iter()
            .map(|island| island.received.load(Relaxed))
            .sum()
    }

    /// Measures `iters` iterations of one working-world configuration; the
    /// `iter_custom` body shared by the scaling and lookahead-sensitivity
    /// groups. Each iteration builds a fresh world and worker pool (untimed),
    /// times only the windowed simulation, and asserts at teardown that the
    /// ring was live.
    fn measure_working_world(
        islands: usize,
        threads: usize,
        lookahead: Duration,
        iters: u64,
    ) -> Duration {
        let mut total = Duration::ZERO;
        for _ in 0..iters {
            // World construction: NOT timed.
            let world: Vec<Island> = (0..islands)
                .map(|id| build_island(id, true, VIRTUAL_HORIZON))
                .collect();

            // Pool creation/teardown: NOT timed (happens outside the closure
            // passed to the pool).
            total += with_worker_pool(&world, threads, |pool| {
                // The simulation: timed.
                let t0 = std::time::Instant::now();
                let windows = run_world(&world, pool, lookahead, VIRTUAL_HORIZON);
                let elapsed = t0.elapsed();
                black_box(windows);
                elapsed
            });

            // Teardown sanity (not timed): the ring was live.
            assert!(
                total_received(&world) > 0,
                "no ring messages were delivered"
            );
            drop(world);
        }
        total
    }

    fn bench_scaling(c: &mut Criterion) {
        let spin_cost = measure_spin_cost();
        println!(
            "rt_quiesce: spin budget = {SPIN_ITERS} iters = {spin_cost:?} per island per 1ms tick"
        );

        let mut group = c.benchmark_group("rt_quiesce_scaling");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(15));
        group.warm_up_time(Duration::from_secs(3));

        for &islands in &[8usize, 32, 128] {
            for &lookahead_us in &[1000u64, 250, 100] {
                let lookahead = Duration::from_micros(lookahead_us);
                for threads in parallelism_levels() {
                    group.bench_with_input(
                        BenchmarkId::new(format!("islands{islands}_la{lookahead_us}us"), threads),
                        &threads,
                        |b, &threads| {
                            b.iter_custom(|iters| {
                                measure_working_world(islands, threads, lookahead, iters)
                            });
                        },
                    );
                }
            }
        }
        group.finish();
    }

    fn bench_lookahead_sensitivity(c: &mut Criterion) {
        // Same measurement as bench_scaling but restricted to islands=32 and threads in
        // {1, 8, 32}; exists as a separate, smaller group so the lookahead sensitivity
        // curve can be re-run quickly without the full matrix.
        let mut group = c.benchmark_group("rt_quiesce_lookahead");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(15));
        group.warm_up_time(Duration::from_secs(3));

        let islands = 32usize;
        let threads_levels: Vec<usize> = parallelism_levels()
            .into_iter()
            .filter(|&t| t == 1 || t == 8 || t == 32)
            .collect();

        for &lookahead_us in &[1000u64, 250, 100] {
            let lookahead = Duration::from_micros(lookahead_us);
            for &threads in &threads_levels {
                group.bench_with_input(
                    BenchmarkId::new(format!("la{lookahead_us}us"), threads),
                    &threads,
                    |b, &threads| {
                        b.iter_custom(|iters| {
                            measure_working_world(islands, threads, lookahead, iters)
                        });
                    },
                );
            }
        }
        group.finish();
    }

    fn bench_per_window_overhead(c: &mut Criterion) {
        let mut group = c.benchmark_group("rt_quiesce_overhead");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        for &islands in &[8usize, 32] {
            group.bench_with_input(
                BenchmarkId::new("empty_windows", islands),
                &islands,
                |b, &islands| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            // World construction: NOT timed. work = false and
                            // horizon = ZERO mean no spin budget and no timers at all,
                            // so every window is empty: stepping a fixed 100 windows
                            // over this world measures pure per-window overhead.
                            let world: Vec<Island> = (0..islands)
                                .map(|id| build_island(id, false, Duration::ZERO))
                                .collect();

                            // Pool creation/teardown: NOT timed.
                            total += with_worker_pool(&world, 1, |pool| {
                                // The simulation: timed.
                                let t0 = std::time::Instant::now();
                                let windows = run_world(
                                    &world,
                                    pool,
                                    Duration::from_millis(1),
                                    Duration::from_millis(100),
                                );
                                let elapsed = t0.elapsed();
                                black_box(windows);
                                elapsed
                            });

                            // Teardown sanity (not timed): pure overhead means the
                            // ring stayed silent.
                            assert_eq!(total_received(&world), 0);
                            drop(world);
                        }
                        total
                    });
                },
            );
        }
        group.finish();
    }

    criterion_group!(
        rt_quiesce,
        bench_scaling,
        bench_lookahead_sensitivity,
        bench_per_window_overhead
    );
}

#[cfg(feature = "test-util")]
criterion::criterion_main!(bench::rt_quiesce);

#[cfg(not(feature = "test-util"))]
fn main() {
    eprintln!("rt_quiesce benchmark requires the `test-util` feature:");
    eprintln!("    cargo bench --features test-util --bench rt_quiesce");
}
