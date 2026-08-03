//! Stall detection monitor thread with signal-based stack trace capture.
//!
//! When enabled, a background monitor thread periodically polls the per-worker
//! `poll_generation` counters. If a worker's generation counter remains at the
//! same odd value between two successive checks, the worker is likely stalled
//! in a long-running synchronous call within `Future::poll`.
//!
//! On Linux, the monitor can capture a stack trace from the stalled worker
//! thread by sending a realtime signal and collecting instruction
//! pointers from within the signal handler.
//!
//! The signal handler uses an async-signal-safe frame-pointer walker instead of
//! `backtrace::trace_unsynchronized` (which internally calls `_Unwind_Backtrace`
//! -> `dl_iterate_phdr` -> glibc loader lock, making it NOT async-signal-safe).
//! Address validation is performed by attempting a `write()` to a pre-opened pipe,
//! which returns EFAULT for unreadable memory. Both raw pointer reads and `write()`
//! are POSIX async-signal-safe.

use crate::runtime::blocking::BlockingPoolSnapshot;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

/// Maximum number of stack frames to capture.
const MAX_FRAMES: usize = 64;

// --- Pipe-based pointer validation ---

/// File descriptor for the read end of the validation pipe.
static VALIDATE_PIPE_READ: AtomicI32 = AtomicI32::new(-1);
/// File descriptor for the write end of the validation pipe.
static VALIDATE_PIPE_WRITE: AtomicI32 = AtomicI32::new(-1);

/// Initialize the validation pipe. Call once at monitor startup.
///
/// The pipe is used by the signal handler to validate pointer readability
/// via `write()`, which is async-signal-safe and returns EFAULT for bad addresses.
#[cfg(target_os = "linux")]
fn init_validate_pipe() {
    let mut fds = [0i32; 2];
    // SAFETY: pipe2() with valid pointer and flags is safe. We store the resulting
    // fds in atomics for use in the signal handler.
    unsafe {
        if libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) == 0 {
            VALIDATE_PIPE_READ.store(fds[0], Ordering::Release);
            VALIDATE_PIPE_WRITE.store(fds[1], Ordering::Release);
        }
    }
}

/// Drain accumulated data from the validation pipe's read end.
///
/// Called on the monitor thread before each capture to prevent the pipe buffer
/// from filling up (which would cause `write()` to return EAGAIN in the handler).
#[cfg(target_os = "linux")]
fn drain_validate_pipe() {
    let read_fd = VALIDATE_PIPE_READ.load(Ordering::Relaxed);
    if read_fd >= 0 {
        let mut buf = [0u8; 4096];
        // SAFETY: read_fd is a valid fd from pipe2(), buf is a valid local buffer.
        unsafe { while libc::read(read_fd, buf.as_mut_ptr() as *mut _, buf.len()) > 0 {} }
    }
}

/// Close and clean up the validation pipe file descriptors.
#[cfg(target_os = "linux")]
fn cleanup_validate_pipe() {
    let read_fd = VALIDATE_PIPE_READ.swap(-1, Ordering::Relaxed);
    let write_fd = VALIDATE_PIPE_WRITE.swap(-1, Ordering::Relaxed);
    // SAFETY: We only close fds that were successfully opened by pipe2().
    // The swap(-1) ensures each fd is closed at most once.
    unsafe {
        if read_fd >= 0 {
            libc::close(read_fd);
        }
        if write_fd >= 0 {
            libc::close(write_fd);
        }
    }
}

/// Check if a pointer is readable. Async-signal-safe.
///
/// Returns `true` if the memory at `ptr` for `len` bytes is readable.
/// Uses `write()` to a pipe: the kernel returns EFAULT if the source address
/// is not readable, and the pipe has O_NONBLOCK so it never blocks.
#[cfg(target_os = "linux")]
unsafe fn is_readable(ptr: *const u8, len: usize) -> bool {
    let write_fd = VALIDATE_PIPE_WRITE.load(Ordering::Relaxed);
    if write_fd < 0 {
        return false;
    }

    // write() returns EFAULT if the source address is not readable.
    // If the pipe buffer is full (EAGAIN), we conservatively treat the
    // pointer as valid -- the monitor thread drains before each capture.
    // SAFETY: write_fd is a valid file descriptor opened by init_validate_pipe(),
    // and ptr/len are provided by the caller who ensures they describe a memory
    // region we want to probe. If ptr is invalid, write() returns EFAULT rather
    // than causing undefined behavior.
    let ret = unsafe { libc::write(write_fd, ptr as *const _, len) };
    // ret > 0: wrote successfully, memory is readable
    // ret == -1 && errno == EAGAIN: pipe full, assume valid (conservative)
    // ret == -1 && errno == EFAULT: memory not readable
    if ret > 0 {
        return true;
    }
    if ret == -1 {
        // SAFETY: __errno_location() returns a valid pointer to the thread-local
        // errno value. This is async-signal-safe.
        let errno = unsafe { *libc::__errno_location() };
        if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
            return true; // conservative: assume readable
        }
    }
    false
}

// --- Frame-pointer walker ---

/// Frame pointer layout on the stack.
///
/// Each frame contains a pointer to the previous frame and the return address.
/// This matches the standard x86_64 and aarch64 frame pointer convention.
#[cfg(target_os = "linux")]
#[repr(C)]
struct StackFrame {
    next: *const StackFrame,
    return_address: usize,
}

/// Walk the frame pointer chain starting from the ucontext.
///
/// Fully async-signal-safe: only raw pointer reads and pipe-based address
/// validation (`write()` syscall). No locks, no heap allocation, no `dl_iterate_phdr`.
///
/// Requires code to be compiled with frame pointers (`-C force-frame-pointers=yes`),
/// which is the default on x86_64-linux since Rust 1.72.
///
/// # Safety
///
/// - `ucontext` must be a valid pointer to a `libc::ucontext_t` (as provided
///   by the kernel to SA_SIGINFO signal handlers).
/// - `frames_ptr` must point to a buffer of at least `max_frames` elements.
#[cfg(target_os = "linux")]
unsafe fn walk_frame_pointers(
    ucontext: *mut libc::c_void,
    frames_ptr: *mut usize,
    max_frames: usize,
) -> usize {
    let uc = ucontext as *mut libc::ucontext_t;
    if uc.is_null() {
        return 0;
    }

    let mut idx = 0;

    // First frame: use the instruction pointer from the ucontext.
    // This is the IP at the point the signal interrupted the thread.
    #[cfg(target_arch = "x86_64")]
    // SAFETY: uc is non-null (checked above) and was provided by the kernel
    // as a valid ucontext_t pointer to the SA_SIGINFO signal handler.
    let (ip, fp) = unsafe {
        let mctx = &(*uc).uc_mcontext;
        (
            mctx.gregs[libc::REG_RIP as usize] as usize,
            mctx.gregs[libc::REG_RBP as usize] as usize,
        )
    };

    #[cfg(target_arch = "aarch64")]
    // SAFETY: uc is non-null (checked above) and was provided by the kernel
    // as a valid ucontext_t pointer to the SA_SIGINFO signal handler.
    let (ip, fp) = unsafe {
        let mctx = &(*uc).uc_mcontext;
        (
            mctx.pc as usize,
            mctx.regs[29] as usize, // x29 = frame pointer on aarch64
        )
    };

    // On unsupported architectures, bail out.
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = (uc, frames_ptr, max_frames, idx);
        return 0;
    }

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        // Store the interrupted instruction pointer as frame 0.
        if idx < max_frames {
            // SAFETY: frames_ptr has at least max_frames elements (caller invariant),
            // and idx < max_frames.
            unsafe { *frames_ptr.add(idx) = ip };
            idx += 1;
        }

        // Walk the frame pointer chain.
        let mut current_fp = fp as *const StackFrame;

        while idx < max_frames {
            // Validate the frame pointer: must be non-null, aligned, and readable.
            if current_fp.is_null() {
                break;
            }
            if (current_fp as usize) % std::mem::align_of::<*const u8>() != 0 {
                break;
            }
            // SAFETY: is_readable probes the memory via write() to a pipe,
            // which is async-signal-safe and will not crash on bad addresses.
            if unsafe { !is_readable(current_fp as *const u8, std::mem::size_of::<StackFrame>()) } {
                break;
            }

            // SAFETY: is_readable confirmed the StackFrame at current_fp is readable.
            let frame = unsafe { &*current_fp };
            let ret_addr = frame.return_address;

            // A return address of 0 means we've reached the bottom of the stack.
            if ret_addr == 0 {
                break;
            }

            // SAFETY: frames_ptr has at least max_frames elements, and idx < max_frames.
            unsafe { *frames_ptr.add(idx) = ret_addr };
            idx += 1;

            // The next frame pointer should be higher on the stack (stack grows down).
            // This prevents infinite loops from corrupted frame pointers.
            let next_fp = frame.next;
            if next_fp <= current_fp {
                break;
            }

            current_fp = next_fp;
        }

        idx
    }
}

/// State for a single in-flight trace capture.
///
/// Only one capture can be in flight at a time (enforced by the monitor thread,
/// which is the sole caller of `capture_trace`).
///
/// # Safety protocol
///
/// - Only the monitor thread writes `ready` (set before sending signal,
///   cleared after reading the result or on timeout).
/// - Only the signal handler writes `frames`, `len`, and `done`.
/// - The monitor reads `frames` and `len` only after observing `done == true`,
///   which establishes a happens-before relationship via Acquire/Release on
///   `done`.
struct CaptureState {
    /// Set by monitor before sending signal, cleared after reading result.
    ready: AtomicBool,
    /// Set by signal handler after capture is complete.
    done: AtomicBool,
    /// Captured instruction pointers.
    frames: std::cell::UnsafeCell<[usize; MAX_FRAMES]>,
    /// Number of frames captured.
    len: std::cell::UnsafeCell<usize>,
}

// SAFETY: Access is synchronized via the ready/done atomic protocol.
// Only the monitor writes `ready`; only the handler writes `frames`, `len`, `done`.
// The monitor reads `frames` and `len` only after observing `done == true`.
unsafe impl Sync for CaptureState {}
unsafe impl Send for CaptureState {}

static CAPTURE: CaptureState = CaptureState {
    ready: AtomicBool::new(false),
    done: AtomicBool::new(false),
    frames: std::cell::UnsafeCell::new([0usize; MAX_FRAMES]),
    len: std::cell::UnsafeCell::new(0),
};

/// Global lock held during stack trace capture to prevent multiple monitor
/// threads from interfering with each other's signal handler communication.
/// This is NOT held by the signal handler itself (which would be unsafe),
/// only by the monitor thread around the ready/signal/wait-for-done sequence.
static CAPTURE_LOCK: Mutex<()> = Mutex::new(());

// --- Signal handler infrastructure (Linux only) ---

#[cfg(target_os = "linux")]
mod signal_impl {
    use super::*;
    use std::sync::Once;

    /// Dynamically chosen realtime signal number, or -1 if no free signal was found.
    static STALL_SIGNAL: AtomicI32 = AtomicI32::new(-1);
    static SIGNAL_INIT: Once = Once::new();

    /// Signal handler invoked on the target worker thread.
    ///
    /// # Safety
    ///
    /// This is a signal handler. It is fully async-signal-safe:
    /// - Frame pointer walking uses only raw pointer reads
    /// - Address validation uses `write()` to a pipe (POSIX async-signal-safe)
    /// - No locks, no heap allocation, no `dl_iterate_phdr`
    /// - Requires code to be compiled with frame pointers
    ///   (`-C force-frame-pointers=yes`), which is the default on
    ///   x86_64-linux since Rust 1.72
    unsafe extern "C" fn stall_signal_handler(
        _sig: libc::c_int,
        _info: *mut libc::siginfo_t,
        ucontext: *mut libc::c_void,
    ) {
        if !CAPTURE.ready.load(Ordering::Acquire) {
            return;
        }

        // Save errno before any syscalls. The interrupted thread may be in the
        // middle of inspecting errno from a prior syscall; the write()/read()
        // calls below would otherwise clobber it.
        // SAFETY: __errno_location() returns the thread-local errno pointer and
        // is async-signal-safe.
        let saved_errno = unsafe { *libc::__errno_location() };

        let frames_ptr = CAPTURE.frames.get();
        let len_ptr = CAPTURE.len.get();

        // SAFETY: walk_frame_pointers uses only raw pointer reads and
        // pipe-based address validation (write() syscall), both of which
        // are async-signal-safe. No locks, no heap allocation, no dl_iterate_phdr.
        //
        // The raw pointer dereferences of `frames_ptr` and `len_ptr` are safe because
        // the atomic protocol guarantees exclusive access: only the signal handler
        // writes to these fields, and only one handler runs at a time.
        unsafe {
            let count = walk_frame_pointers(ucontext, (*frames_ptr).as_mut_ptr(), MAX_FRAMES);
            *len_ptr = count;
        }
        CAPTURE.done.store(true, Ordering::Release);

        // SAFETY: same as the load above.
        unsafe { *libc::__errno_location() = saved_errno };
    }

    /// Installs the stall signal handler. Safe to call multiple times;
    /// the handler is only installed once.
    ///
    /// Probes realtime signals from `SIGRTMIN+1` through `SIGRTMAX` and installs
    /// our handler on the first signal that has a default or ignored disposition.
    /// This avoids overwriting handlers installed by other profiling tools.
    pub(super) fn install_signal_handler() {
        SIGNAL_INIT.call_once(|| {
            // SAFETY: We call sigaction to probe and then install a handler.
            // All arguments are valid: zeroed sigaction structs, valid signal numbers.
            unsafe {
                // Probe for a free realtime signal.
                // Skip SIGRTMIN+0 which is often reserved by glibc/pthreads.
                let min = libc::SIGRTMIN() + 1;
                let max = libc::SIGRTMAX();

                for sig in min..=max {
                    let mut old: libc::sigaction = std::mem::zeroed();
                    if libc::sigaction(sig, std::ptr::null(), &mut old) != 0 {
                        continue;
                    }

                    // Check if the signal is free (default or ignored disposition).
                    let handler = old.sa_sigaction;
                    if handler == libc::SIG_DFL || handler == libc::SIG_IGN {
                        // Found a free signal -- install our handler.
                        let mut sa: libc::sigaction = std::mem::zeroed();
                        sa.sa_sigaction =
                            stall_signal_handler as unsafe extern "C" fn(_, _, _) as usize;
                        sa.sa_flags = libc::SA_SIGINFO | libc::SA_RESTART;
                        libc::sigemptyset(&mut sa.sa_mask);

                        if libc::sigaction(sig, &sa, std::ptr::null_mut()) == 0 {
                            STALL_SIGNAL.store(sig, Ordering::Release);
                            return;
                        }
                    }
                }

                // No free signal found.
                tracing::warn!(
                    "tokio stall detection: no free realtime signal available, \
                     stack traces will be unavailable"
                );
            }
        });
    }

    /// Captures a stack trace from the thread with the given OS thread ID.
    ///
    /// Returns instruction pointer addresses, or an empty vec on timeout
    /// or if no realtime signal was available.
    pub(super) fn capture_trace(os_tid: u64) -> Vec<usize> {
        let sig = STALL_SIGNAL.load(Ordering::Acquire);
        if sig < 0 {
            return vec![]; // no signal available
        }

        // Hold the capture lock to prevent concurrent monitor threads from
        // interfering with each other's signal handler communication.
        let _guard = CAPTURE_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        // Drain the validation pipe before capture to ensure the signal handler's
        // write()-based pointer validation won't hit a full pipe buffer.
        drain_validate_pipe();

        // Reset state
        CAPTURE.done.store(false, Ordering::Release);
        CAPTURE.ready.store(true, Ordering::Release);

        // SAFETY: tgkill with our own pid and a valid tid sends a signal to
        // the specified thread. os_tid was obtained from gettid() on the worker.
        unsafe {
            libc::syscall(
                libc::SYS_tgkill,
                libc::getpid() as libc::c_long,
                os_tid as libc::c_long,
                sig as libc::c_long,
            );
        }

        // Wait for completion with timeout
        let start = std::time::Instant::now();
        while !CAPTURE.done.load(Ordering::Acquire) {
            if start.elapsed() > std::time::Duration::from_millis(500) {
                CAPTURE.ready.store(false, Ordering::Release);
                return vec![];
            }
            std::thread::yield_now();
        }

        CAPTURE.ready.store(false, Ordering::Release);

        // SAFETY: We observed `done == true` with Acquire ordering, which
        // synchronizes with the handler's Release store, guaranteeing that
        // `frames` and `len` are fully written and visible to us.
        unsafe {
            let len = *CAPTURE.len.get();
            let frames = &*CAPTURE.frames.get();
            frames[..len].to_vec()
        }
    }
}

/// Symbolicate a captured trace: resolve instruction pointers to human-readable
/// symbol names, file paths, and line numbers.
#[cfg(target_os = "linux")]
fn symbolicate_trace(ips: &[usize]) -> Vec<String> {
    use std::ffi::c_void;

    let mut result = Vec::new();
    for &ip in ips {
        let mut resolved = String::new();
        backtrace::resolve(ip as *mut c_void, |symbol| {
            if let Some(name) = symbol.name() {
                resolved = format!("{name}");
                if let (Some(file), Some(line)) = (symbol.filename(), symbol.lineno()) {
                    resolved = format!("{name} at {}:{line}", file.display());
                }
            } else {
                resolved = format!("{ip:#x}");
            }
        });
        if resolved.is_empty() {
            resolved = format!("{ip:#x}");
        }
        result.push(resolved);
    }
    result
}

/// Stub for non-Linux platforms. Capture is currently Linux-only so this is
/// usually called with an empty slice; if any IPs are passed (e.g. from a
/// future non-Linux capture path), format them as hex so the callback still
/// gets a usable per-frame entry.
#[cfg(not(target_os = "linux"))]
fn symbolicate_trace(ips: &[usize]) -> Vec<String> {
    ips.iter().map(|ip| format!("{ip:#x}")).collect()
}

/// Reads the kernel stack trace of a thread from procfs.
///
/// This provides information about what the thread is doing in kernel space
/// (e.g., waiting in a futex, blocked on I/O, etc.), which complements the
/// user-space backtrace.
#[cfg(target_os = "linux")]
fn capture_kernel_stack(os_tid: u64) -> Option<String> {
    let pid = std::process::id();
    let path = format!("/proc/{pid}/task/{os_tid}/stack");
    std::fs::read_to_string(&path).ok()
}


// --- Per-worker monitor state ---

#[derive(Clone, Copy, PartialEq)]
enum WorkerState {
    Idle,
    WaitingForResolution {
        stall_start: std::time::Instant,
        /// The (odd) generation the worker is stalled at.
        stalled_gen: u64,
        /// Latched true once the worker's `blocked_in_place_generation` is
        /// observed equal to `stalled_gen`. Latched (re-checked every tick)
        /// rather than read once at detection or only at emit: the stalled
        /// poll may call `block_in_place` after detection or escalation (the
        /// resolution event must still carry the flag), and after resolution
        /// a later poll's `block_in_place` can overwrite the single tag slot
        /// before the emit tick (the flag must not be lost).
        blocked: bool,
        trace: usize, // index into stored_traces, stored_kernel_stacks, stored_thread_names, stored_blocking
        escalated: bool,
    },
}

/// Information about a detected stall event.
#[derive(Debug)]
pub struct StallInfo {
    /// The worker index that stalled.
    pub worker: usize,
    /// How long the stall lasted (or has lasted so far for intermediate events).
    pub duration: std::time::Duration,
    /// Whether this is the final report (stall resolved) or an intermediate warning.
    pub resolved: bool,
    /// User-space stack trace frames (instruction pointers), if captured.
    /// Use `backtrace::resolve` to symbolicate.
    pub backtrace_frames: Vec<usize>,
    /// Symbolicated form of `backtrace_frames`, one entry per IP.
    ///
    /// Populated on Linux when stack traces are available; empty otherwise
    /// (non-Linux platforms, or when capture failed). Provided so consumers
    /// such as Sentry forwarders can avoid re-symbolicating the same frames
    /// the monitor thread already resolved for the log line.
    pub symbolicated_frames: Vec<String>,
    /// Kernel stack trace, if available.
    pub kernel_stack: Option<String>,
    /// Name of the stalled worker thread, captured at startup from
    /// `std::thread::current().name()`.
    ///
    /// This is the value configured via `Builder::thread_name(...)` /
    /// `Builder::thread_name_fn(...)` (default: `"tokio-runtime-worker"`),
    /// not the kernel-truncated `comm`. `None` if the worker had no thread
    /// name set.
    pub thread_name: Option<String>,
    /// Whether the stalled poll handed its core off via
    /// [`block_in_place`](crate::task::block_in_place).
    ///
    /// `true` means the "stall" is the gap between the core handoff and
    /// another thread claiming the core (e.g. blocking pool saturation) -- the
    /// worker was released, not held hostage by the poll. `false` means the
    /// poll kept the worker occupied for the full stall duration.
    pub blocked_in_place: bool,
}

/// Callback type for stall events.
pub(crate) type StallCallback = Arc<dyn Fn(StallInfo) + Send + Sync>;

// --- Monitor configuration ---

/// Configuration for the stall detection monitor thread.
pub(crate) struct StallDetectionConfig {
    /// How often to poll generation counters.
    pub(crate) poll_interval: std::time::Duration,
    /// How long a stall must persist before emitting an intermediate "still stalled" warning.
    pub(crate) escalation_threshold: std::time::Duration,
    /// Optional callback for stall events. Stall events are always reported via
    /// `tracing` regardless of whether a callback is set.
    pub(crate) on_stall: Option<StallCallback>,
}

impl Clone for StallDetectionConfig {
    fn clone(&self) -> Self {
        Self {
            poll_interval: self.poll_interval,
            escalation_threshold: self.escalation_threshold,
            on_stall: self.on_stall.clone(),
        }
    }
}

impl std::fmt::Debug for StallDetectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StallDetectionConfig")
            .field("poll_interval", &self.poll_interval)
            .field("escalation_threshold", &self.escalation_threshold)
            .field("on_stall", &self.on_stall.as_ref().map(|_| "<callback>"))
            .finish()
    }
}

impl Default for StallDetectionConfig {
    fn default() -> Self {
        Self {
            poll_interval: std::time::Duration::from_millis(100),
            escalation_threshold: std::time::Duration::from_secs(10),
            on_stall: None,
        }
    }
}

// --- Monitor handle for lifecycle management ---

/// Handle to the stall detection monitor thread.
///
/// When dropped, signals the monitor to shut down and joins the thread.
pub(crate) struct StallMonitorHandle {
    shutdown: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl std::fmt::Debug for StallMonitorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StallMonitorHandle")
            .field("shutdown", &self.shutdown.load(Ordering::Relaxed))
            .field("thread", &self.thread.as_ref().map(|t| t.thread().name()))
            .finish()
    }
}

impl Drop for StallMonitorHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Starts the stall detection monitor thread.
///
/// Returns a `StallMonitorHandle` that will shut down the monitor when dropped.
pub(crate) fn start_monitor(
    handle: crate::runtime::Handle,
    config: StallDetectionConfig,
) -> StallMonitorHandle {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    let thread = std::thread::Builder::new()
        .name("tokio-stall-monitor".to_string())
        .spawn(move || {
            run_monitor(handle, config, shutdown_clone);
        })
        .expect("failed to spawn stall detection monitor thread");

    StallMonitorHandle {
        shutdown,
        thread: Some(thread),
    }
}

/// Main loop of the stall detection monitor thread.
fn run_monitor(
    handle: crate::runtime::Handle,
    config: StallDetectionConfig,
    shutdown: Arc<AtomicBool>,
) {
    #[cfg(target_os = "linux")]
    {
        init_validate_pipe();
        signal_impl::install_signal_handler();
    }

    let num_workers = handle.metrics().num_workers();
    let mut prev_gen = vec![0u64; num_workers];
    let mut worker_states = vec![WorkerState::Idle; num_workers];
    let mut stored_traces: Vec<Vec<usize>> = Vec::new();
    let mut stored_kernel_stacks: Vec<Option<String>> = Vec::new();
    let mut stored_blocking: Vec<BlockingPoolSnapshot> = Vec::new();
    let mut stored_thread_names: Vec<Option<String>> = Vec::new();

    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(config.poll_interval);

        let metrics = handle.metrics();

        for i in 0..num_workers {
            let gen = metrics.worker_poll_generation(i);

            match worker_states[i] {
                WorkerState::Idle => {
                    if gen == prev_gen[i] && gen % 2 == 1 {
                        // Stall detected! Capture trace immediately.
                        let trace = capture_worker_trace(&metrics, i);
                        let kernel_stack = capture_worker_kernel_stack(&metrics, i);
                        let thread_name = capture_worker_thread_name_for(&metrics, i);
                        let blocking =
                            handle.inner.blocking_spawner().stall_detection_snapshot();
                        let trace_idx = stored_traces.len();
                        stored_traces.push(trace);
                        stored_kernel_stacks.push(kernel_stack);
                        stored_thread_names.push(thread_name);
                        stored_blocking.push(blocking);
                        worker_states[i] = WorkerState::WaitingForResolution {
                            stall_start: std::time::Instant::now(),
                            stalled_gen: gen,
                            blocked: metrics.worker_blocked_in_place_generation(i) == gen,
                            trace: trace_idx,
                            escalated: false,
                        };
                    }
                }
                WorkerState::WaitingForResolution {
                    stall_start,
                    stalled_gen,
                    ref mut blocked,
                    trace,
                    ref mut escalated,
                } => {
                    // See the `blocked` field docs for why this latches on
                    // every tick.
                    *blocked =
                        *blocked || metrics.worker_blocked_in_place_generation(i) == stalled_gen;
                    let blocked = *blocked;
                    if gen != prev_gen[i] {
                        // Stall resolved
                        let duration = stall_start.elapsed();
                        emit_resolved(
                            &config.on_stall,
                            i,
                            duration,
                            &stored_traces[trace],
                            &stored_kernel_stacks[trace],
                            &stored_thread_names[trace],
                            stored_blocking[trace],
                            blocked,
                        );
                        worker_states[i] = WorkerState::Idle;
                    } else if !*escalated && stall_start.elapsed() > config.escalation_threshold {
                        // Stall is really bad - emit intermediate warning
                        let duration = stall_start.elapsed();
                        emit_escalation(
                            &config.on_stall,
                            i,
                            duration,
                            &stored_traces[trace],
                            &stored_kernel_stacks[trace],
                            &stored_thread_names[trace],
                            stored_blocking[trace],
                            blocked,
                        );
                        *escalated = true;
                    }
                }
            }

            prev_gen[i] = gen;
        }
    }

    #[cfg(target_os = "linux")]
    cleanup_validate_pipe();
}

/// Attempt to capture a stack trace from the given worker.
///
/// On Linux, this sends a realtime signal to the worker thread and collects the
/// backtrace from the signal handler. On other platforms, returns an empty vec.
fn capture_worker_trace(metrics: &crate::runtime::RuntimeMetrics, worker: usize) -> Vec<usize> {
    #[cfg(target_os = "linux")]
    {
        let os_tid = metrics.worker_os_thread_id(worker);
        if os_tid != 0 {
            signal_impl::capture_trace(os_tid)
        } else {
            vec![]
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (metrics, worker);
        vec![]
    }
}

/// Attempt to capture the kernel stack trace from the given worker.
///
/// On Linux, reads from /proc/<pid>/task/<tid>/stack. On other platforms,
/// returns None.
fn capture_worker_kernel_stack(
    metrics: &crate::runtime::RuntimeMetrics,
    worker: usize,
) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let os_tid = metrics.worker_os_thread_id(worker);
        if os_tid != 0 {
            capture_kernel_stack(os_tid)
        } else {
            None
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (metrics, worker);
        None
    }
}

/// Look up the worker's thread name as recorded by the worker at startup.
fn capture_worker_thread_name_for(
    metrics: &crate::runtime::RuntimeMetrics,
    worker: usize,
) -> Option<String> {
    metrics.worker_thread_name(worker).map(str::to_owned)
}

/// Format a symbolicated stack trace and optional kernel stack into a string.
fn format_trace(
    trace_ips: &[usize],
    symbolicated: &[String],
    kernel_stack: &Option<String>,
) -> String {
    use std::fmt::Write;
    let mut output = String::new();

    if trace_ips.is_empty() {
        writeln!(output, "  (no stack trace available)").unwrap();
    } else {
        #[cfg(target_os = "linux")]
        {
            writeln!(output, "User-space stack trace at time of detection:").unwrap();
            for (j, frame) in symbolicated.iter().enumerate() {
                writeln!(output, "  #{j}: {frame}").unwrap();
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = symbolicated;
            writeln!(output, "  (stack traces not supported on this platform)").unwrap();
        }
    }

    match kernel_stack {
        Some(ref ks) if !ks.trim().is_empty() => {
            writeln!(output, "Kernel stack at time of detection:").unwrap();
            for line in ks.lines() {
                if !line.is_empty() {
                    writeln!(output, "  {line}").unwrap();
                }
            }
        }
        _ => {
            writeln!(
                output,
                "Kernel stack: (not available, may require CAP_SYS_PTRACE or root)"
            )
            .unwrap();
        }
    }

    output
}

/// Emit a stall-resolved event via tracing and optionally via callback.
fn emit_resolved(
    on_stall: &Option<StallCallback>,
    worker: usize,
    duration: std::time::Duration,
    trace_ips: &[usize],
    kernel_stack: &Option<String>,
    thread_name: &Option<String>,
    blocking: BlockingPoolSnapshot,
    blocked_in_place: bool,
) {
    // Always emit via tracing for observability.
    let symbolicated = symbolicate_trace(trace_ips);
    let trace_str = format_trace(trace_ips, &symbolicated, kernel_stack);
    tracing::warn!(
        worker = worker,
        thread_name = thread_name.as_deref(),
        duration_ms = duration.as_millis() as u64,
        blocking_threads = blocking.num_threads,
        blocking_thread_cap = blocking.thread_cap,
        blocking_idle = blocking.num_idle_threads,
        blocking_queued = blocking.queue_depth,
        blocked_in_place = blocked_in_place,
        "Scheduler stall on worker {} resolved after {:.1}ms\n{}",
        worker,
        duration.as_secs_f64() * 1000.0,
        trace_str,
    );

    // Additionally call the on_stall callback if one is configured.
    if let Some(ref cb) = *on_stall {
        cb(StallInfo {
            worker,
            duration,
            resolved: true,
            backtrace_frames: trace_ips.to_vec(),
            symbolicated_frames: symbolicated,
            kernel_stack: kernel_stack.clone(),
            thread_name: thread_name.clone(),
            blocked_in_place,
        });
    }
}

/// Emit an escalation warning via tracing and optionally via callback.
fn emit_escalation(
    on_stall: &Option<StallCallback>,
    worker: usize,
    duration: std::time::Duration,
    trace_ips: &[usize],
    kernel_stack: &Option<String>,
    thread_name: &Option<String>,
    blocking: BlockingPoolSnapshot,
    blocked_in_place: bool,
) {
    // Always emit via tracing for observability.
    let symbolicated = symbolicate_trace(trace_ips);
    let trace_str = format_trace(trace_ips, &symbolicated, kernel_stack);
    tracing::error!(
        worker = worker,
        thread_name = thread_name.as_deref(),
        duration_s = duration.as_secs(),
        blocking_threads = blocking.num_threads,
        blocking_thread_cap = blocking.thread_cap,
        blocking_idle = blocking.num_idle_threads,
        blocking_queued = blocking.queue_depth,
        blocked_in_place = blocked_in_place,
        "Worker {} has been stalled for {:.1}s and counting!\n{}",
        worker,
        duration.as_secs_f64(),
        trace_str,
    );

    // Additionally call the on_stall callback if one is configured.
    if let Some(ref cb) = *on_stall {
        cb(StallInfo {
            worker,
            duration,
            resolved: false,
            backtrace_frames: trace_ips.to_vec(),
            symbolicated_frames: symbolicated,
            kernel_stack: kernel_stack.clone(),
            thread_name: thread_name.clone(),
            blocked_in_place,
        });
    }
}
