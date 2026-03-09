//! Demonstrates tokio's stall detection feature.
//!
//! The stall detection monitor runs a background thread that periodically checks
//! whether any worker thread has been stuck in a single `poll()` call for too
//! long. When a stall is detected, it captures a stack trace (on Linux) and
//! reports the event via tracing and/or a user-provided callback.
//!
//! This example spawns:
//! - A "good" task that cooperates by yielding and using async sleep.
//! - A "bad" task that blocks the worker with `std::thread::sleep`.
//! - A "busy" task that hogs the CPU with a tight loop.
//!
//! Run with:
//!
//!     cargo run --example stall_detection
//!
//! Expected output: the tracing subscriber shows WARN-level messages from the
//! stall detection monitor, including user-space and kernel stack traces. The
//! `on_stall` callback silently counts stall events for programmatic use.

#[cfg(target_os = "linux")]
fn main() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    // Set up a tracing subscriber so the monitor's tracing::warn! output is visible.
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();

    // Track how many stall events the callback receives.
    let stall_count = Arc::new(AtomicUsize::new(0));
    let stall_count_cb = stall_count.clone();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        // Enable stall detection with a short poll interval for this demo.
        // In production you would typically use the default (100ms) or tune
        // it to your latency requirements.
        .stall_detection_poll_interval(Duration::from_millis(50))
        // Register a callback that fires whenever a stall is detected or resolved.
        // The tracing output from the monitor is sufficient for display; the callback
        // just counts stalls here to demonstrate programmatic access to stall events.
        .on_stall(move |_info| {
            stall_count_cb.fetch_add(1, Ordering::Relaxed);
        })
        .build()
        .expect("failed to build runtime");

    rt.block_on(async {
        println!("=== Stall Detection Demo ===\n");

        // --- Good task: cooperates with the runtime ---
        let good = tokio::spawn(async {
            println!("[good task] starting -- does async work and yields properly");
            for i in 0..5 {
                // Async sleep yields control back to the runtime.
                tokio::time::sleep(Duration::from_millis(100)).await;
                println!("[good task] iteration {}", i + 1);
            }
            println!("[good task] done");
        });

        // Give the good task a moment to start.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // --- Bad task: blocks the worker with std::thread::sleep ---
        let bad_blocking = tokio::spawn(async {
            println!("[bad task] starting -- about to block the worker for 500ms!");
            // THIS IS THE BUG: using std::thread::sleep inside an async task
            // blocks the entire worker thread. The stall detector will catch this.
            std::thread::sleep(Duration::from_millis(500));
            println!("[bad task] done blocking");
        });

        // Give the bad task a moment to start and get detected.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // --- Busy task: hogs the CPU with a tight loop ---
        let bad_busy = tokio::spawn(async {
            println!("[busy task] starting -- about to spin the CPU for 300ms!");
            // Another common mistake: a CPU-intensive loop that never yields.
            let start = std::time::Instant::now();
            let mut sum: u64 = 0;
            while start.elapsed() < Duration::from_millis(300) {
                sum = sum.wrapping_add(1);
            }
            // Use `sum` to prevent the loop from being optimized away.
            println!("[busy task] done spinning (sum = {})", sum);
        });

        // Wait for all tasks to finish.
        let _ = good.await;
        let _ = bad_blocking.await;
        let _ = bad_busy.await;

        // Let the monitor have a final poll cycle to report resolved stalls.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let total = stall_count.load(Ordering::Relaxed);
        println!("\n=== Demo Complete ===");
        println!("Total stall events reported: {}", total);
        if total > 0 {
            println!(
                "The stall detector successfully caught the blocking operations!\n\
                 In production, use tokio::task::spawn_blocking() for blocking work."
            );
        } else {
            println!(
                "No stalls detected. This can happen if the OS scheduled things \
                 favorably. Try running again or increasing the blocking durations."
            );
        }
    });
}

#[cfg(not(target_os = "linux"))]
fn main() {
    println!(
        "Stall detection is currently supported on Linux only.\n\
         Run this example on a Linux system to see it in action."
    );
}
