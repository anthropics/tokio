#![warn(rust_2018_idioms)]
#![cfg(all(
    feature = "stall-detection",
    feature = "rt-multi-thread",
    feature = "macros",
    feature = "sync",
    not(target_os = "wasi"),
    target_has_atomic = "64"
))]

use tokio::runtime::Runtime;

#[test]
fn multi_thread_generation_starts_even() {
    let rt = multi_thread();
    let metrics = rt.metrics();
    let gen = metrics.worker_poll_generation(0);
    assert_eq!(gen % 2, 0, "generation should be even when idle");
}

#[test]
fn multi_thread_generation_even_after_work() {
    let rt = multi_thread();
    let metrics = rt.metrics();

    // Spawn actual tasks so the worker's run_task path increments the generation.
    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..10 {
            handles.push(tokio::spawn(async {
                tokio::task::yield_now().await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });

    let gen_after = metrics.worker_poll_generation(0);
    assert_eq!(
        gen_after % 2,
        0,
        "generation should be even when idle after work"
    );
}

#[test]
fn multi_thread_generation_advances() {
    let rt = multi_thread();
    let metrics = rt.metrics();

    let gen_before = metrics.worker_poll_generation(0);

    // Spawn actual tasks so the worker's run_task path increments the generation.
    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..50 {
            handles.push(tokio::spawn(async {
                tokio::task::yield_now().await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });

    let gen_after = metrics.worker_poll_generation(0);
    assert_eq!(gen_after % 2, 0, "generation should be even when idle");
    assert!(
        gen_after > gen_before,
        "generation should have advanced: before={gen_before}, after={gen_after}"
    );
}

#[test]
fn current_thread_generation_starts_even() {
    let rt = current_thread();
    let metrics = rt.metrics();
    let gen = metrics.worker_poll_generation(0);
    assert_eq!(gen % 2, 0, "generation should be even when idle");
}

#[test]
fn current_thread_generation_even_after_work() {
    let rt = current_thread();
    let metrics = rt.metrics();

    // Spawn actual tasks so the scheduler's run_task path increments the generation.
    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..10 {
            handles.push(tokio::spawn(async {
                tokio::task::yield_now().await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });

    let gen_after = metrics.worker_poll_generation(0);
    assert_eq!(
        gen_after % 2,
        0,
        "generation should be even when idle after work"
    );
}

#[test]
fn current_thread_generation_advances() {
    let rt = current_thread();
    let metrics = rt.metrics();

    let gen_before = metrics.worker_poll_generation(0);

    // Spawn actual tasks so the scheduler's run_task path increments the generation.
    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..50 {
            handles.push(tokio::spawn(async {
                tokio::task::yield_now().await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });

    let gen_after = metrics.worker_poll_generation(0);
    assert_eq!(gen_after % 2, 0, "generation should be even when idle");
    assert!(
        gen_after > gen_before,
        "generation should have advanced: before={gen_before}, after={gen_after}"
    );
}

fn multi_thread() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

fn current_thread() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn multi_thread_enable_stall_detection_builds() {
    // Verify that enable_stall_detection() doesn't panic or fail.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_stall_detection()
        .enable_all()
        .build()
        .unwrap();

    // The runtime should work normally with stall detection enabled.
    rt.block_on(async {
        tokio::spawn(async {
            tokio::task::yield_now().await;
        })
        .await
        .unwrap();
    });
}

#[test]
fn current_thread_enable_stall_detection_builds() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_stall_detection()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        tokio::spawn(async {
            tokio::task::yield_now().await;
        })
        .await
        .unwrap();
    });
}

#[test]
fn monitor_shuts_down_on_runtime_drop() {
    // Build a runtime with stall detection, do some work, then drop it.
    // The monitor thread should join cleanly (no hang or panic).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_stall_detection()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..20 {
            handles.push(tokio::spawn(async {
                tokio::task::yield_now().await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });

    // Drop the runtime - the monitor should stop.
    drop(rt);
}

/// Test that the monitor detects a stall when a task blocks the worker.
/// We use std::thread::sleep inside a spawned task to simulate a blocking call.
/// The test verifies the runtime survives the stall detection cycle without panicking.
#[test]
fn monitor_detects_stall() {
    use std::sync::{Arc, Barrier};

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_stall_detection()
        .enable_all()
        .build()
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = barrier.clone();

    // Spawn a task that blocks the worker for 300ms (enough for the 100ms
    // poll interval to detect the stall).
    rt.block_on(async {
        let handle = tokio::spawn(async move {
            std::thread::sleep(std::time::Duration::from_millis(300));
            barrier2.wait();
        });

        // Wait for the blocking task to finish from a non-worker thread.
        barrier.wait();
        handle.await.unwrap();
    });

    // Give the monitor a moment to process the resolved stall.
    std::thread::sleep(std::time::Duration::from_millis(200));

    // The monitor should have detected and reported the stall via tracing.
    // We verify the runtime survived the stall detection cycle without panicking.
    drop(rt);
}

/// Test that stall detection works with a custom short poll interval.
/// Uses a 50ms interval and a 500ms blocking sleep so the monitor has
/// multiple chances to observe the stall.
#[test]
fn detects_blocking_stall_with_short_interval() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_stall_detection()
        .stall_detection_poll_interval(std::time::Duration::from_millis(50))
        .stall_detection_escalation_threshold(std::time::Duration::from_secs(60))
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        tokio::spawn(async {
            // Simulate a stall - blocking the worker thread for 500ms.
            // With a 50ms poll interval the monitor should detect the stall
            // after ~100ms (two consecutive polls seeing the same odd generation).
            std::thread::sleep(std::time::Duration::from_millis(500));
        })
        .await
        .unwrap();
    });

    // Give the monitor time to notice the stall resolved and emit the message.
    std::thread::sleep(std::time::Duration::from_millis(200));

    // The stall detection output is emitted via tracing. We verify the runtime
    // didn't crash and that the generation counter reflects the completed work.
    let metrics = rt.metrics();
    let gen = metrics.worker_poll_generation(0);
    assert_eq!(gen % 2, 0, "generation should be even after stall resolves");
    assert!(gen > 0, "generation should have advanced");

    drop(rt);
}

/// Verify that the runtime shuts down promptly when stall detection is enabled.
/// The monitor thread should exit within one poll interval after shutdown is requested.
#[test]
fn runtime_shuts_down_promptly_with_stall_detection() {
    let start = std::time::Instant::now();
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_stall_detection()
            .stall_detection_poll_interval(std::time::Duration::from_millis(100))
            .build()
            .unwrap();

        rt.block_on(async {
            tokio::task::yield_now().await;
        });

        // rt is dropped here
    }
    let elapsed = start.elapsed();
    // Should shut down in well under 1 second
    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "Runtime took {:?} to shut down",
        elapsed
    );
}

/// Verify that the runtime shuts down cleanly even when a worker is actively
/// stalled (blocking in std::thread::sleep). The monitor is in its detection loop
/// while shutdown is requested. We use shutdown_timeout to avoid waiting for the
/// blocking task to finish (which is expected tokio behavior for normal drop).
#[test]
fn runtime_shuts_down_during_active_stall() {
    let start = std::time::Instant::now();
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_stall_detection()
            .stall_detection_poll_interval(std::time::Duration::from_millis(50))
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            // Spawn a task that blocks a worker thread for a long time.
            tokio::spawn(async {
                std::thread::sleep(std::time::Duration::from_secs(30));
            });
            // Give the monitor time to detect the stall.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        // Use shutdown_timeout so we don't wait for the blocking task.
        // The stall monitor should still be stopped promptly.
        rt.shutdown_timeout(std::time::Duration::from_millis(100));
    }
    let elapsed = start.elapsed();
    // block_on takes ~200ms, shutdown_timeout allows 100ms for blocking pool,
    // plus the monitor needs at most one poll_interval (50ms) to notice shutdown.
    // Total should be well under 2 seconds.
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "Runtime took {:?} to shut down during active stall",
        elapsed
    );
}

/// Verify that shutdown_timeout() also properly stops the monitor thread.
#[test]
fn shutdown_timeout_stops_monitor() {
    let start = std::time::Instant::now();
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_stall_detection()
            .stall_detection_poll_interval(std::time::Duration::from_millis(50))
            .build()
            .unwrap();

        rt.block_on(async {
            tokio::task::yield_now().await;
        });

        rt.shutdown_timeout(std::time::Duration::from_millis(100));
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "shutdown_timeout took {:?}",
        elapsed
    );
}

/// Verify that the on_stall callback receives a populated `StallInfo`,
/// including the worker thread name configured via `Builder::thread_name`.
///
/// This in particular guards the multi-thread `fn run()` startup path that
/// records `WorkerMetrics::thread_name`: if a worker reaches its first stall
/// without that path having run, `thread_name` would still be unset.
#[test]
fn on_stall_callback_receives_thread_name() {
    use std::sync::{Arc, Mutex};

    let events: Arc<Mutex<Vec<tokio::runtime::StallInfo>>> = Arc::new(Mutex::new(Vec::new()));
    let events_cb = events.clone();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("stall-test-worker")
        .enable_stall_detection()
        .stall_detection_poll_interval(std::time::Duration::from_millis(50))
        .stall_detection_escalation_threshold(std::time::Duration::from_secs(60))
        .on_stall(move |info| {
            events_cb.lock().unwrap().push(info);
        })
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        tokio::spawn(async {
            // Block the worker long enough for the 50ms poller to see two
            // consecutive identical odd generations.
            std::thread::sleep(std::time::Duration::from_millis(500));
        })
        .await
        .unwrap();
    });

    // Give the monitor a moment to observe the resolved stall and run the callback.
    std::thread::sleep(std::time::Duration::from_millis(300));
    drop(rt);

    let events = events.lock().unwrap();
    assert!(
        !events.is_empty(),
        "expected at least one stall event, got none"
    );

    let info = &events[0];
    assert_eq!(info.worker, 0);
    assert!(info.duration >= std::time::Duration::from_millis(100));
    assert_eq!(
        info.thread_name.as_deref(),
        Some("stall-test-worker"),
        "thread_name should reflect Builder::thread_name(...)"
    );
    // backtrace_frames and symbolicated_frames must be the same length so
    // consumers can zip them.
    assert_eq!(info.backtrace_frames.len(), info.symbolicated_frames.len());
}
