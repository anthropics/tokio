// Signal handling
cfg_signal_internal_and_unix! {
    mod signal;
}
cfg_io_uring! {
    mod uring;
    use uring::UringContext;
    use crate::sync::OnceCell;
}

use crate::io::interest::Interest;
use crate::io::ready::Ready;
use crate::loom::sync::Mutex;
use crate::runtime::driver;
use crate::runtime::io::registration_set;
use crate::runtime::io::{IoDriverMetrics, RegistrationSet, ScheduledIo};

use mio::event::Source;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::time::Duration;

/// I/O driver, backed by Mio. With `io_shards > 1` there is one `Driver` per
/// shard, each with its own epoll instance; they share a single [`Handle`].
pub(crate) struct Driver {
    /// Which shard of the handle this driver polls.
    shard: usize,

    /// True when an event with the signal token is received
    signal_ready: bool,

    /// Reuse the `mio::Events` value across calls to poll.
    events: mio::Events,

    /// The system event queue.
    poll: mio::Poll,
}

/// One epoll instance and the registrations that live on it.
pub(crate) struct Shard {
    /// Registers I/O resources.
    registry: mio::Registry,

    /// Tracks all registrations
    registrations: RegistrationSet,

    /// State that should be synchronized
    synced: Mutex<registration_set::Synced>,

    /// Used to wake up the reactor from a call to `turn`.
    /// Not supported on `Wasi` due to lack of threading support.
    #[cfg(not(target_os = "wasi"))]
    waker: mio::Waker,

    /// `TOKIO_IO_POLL_DEBUG` counters (reset on each snapshot).
    dbg: ShardDebug,
}

/// Debug-only counters; plain `usize` atomics so they exist on every target
/// (times are in microseconds).
#[derive(Default)]
struct ShardDebug {
    last_poll_us: std::sync::atomic::AtomicUsize,
    max_gap_us: std::sync::atomic::AtomicUsize,
    polls: std::sync::atomic::AtomicUsize,
    blocking_polls: std::sync::atomic::AtomicUsize,
    events: std::sync::atomic::AtomicUsize,
    in_wait: std::sync::atomic::AtomicUsize,
}

/// A reference to an I/O driver (all shards).
pub(crate) struct Handle {
    shards: Box<[Shard]>,

    /// `cpu_to_shard[cpu]` = shard serving sockets whose `SO_INCOMING_CPU` is
    /// `cpu`. Empty when there is a single shard or the topology is unknown.
    cpu_to_shard: Box<[u16]>,

    /// Whether to consult `SO_INCOMING_CPU` at all.
    use_incoming_cpu: bool,

    /// Round-robin cursor for sources without a CPU hint.
    next_shard: std::sync::atomic::AtomicUsize,

    pub(crate) metrics: IoDriverMetrics,

    #[cfg(all(
        tokio_unstable,
        feature = "io-uring",
        feature = "rt",
        feature = "fs",
        target_os = "linux",
    ))]
    pub(crate) uring_context: Mutex<UringContext>,

    #[cfg(all(
        tokio_unstable,
        feature = "io-uring",
        feature = "rt",
        feature = "fs",
        target_os = "linux",
    ))]
    pub(crate) uring_probe: OnceCell<Option<io_uring::Probe>>,
}

#[derive(Debug)]
pub(crate) struct ReadyEvent {
    pub(super) tick: u8,
    pub(crate) ready: Ready,
    pub(super) is_shutdown: bool,
}

cfg_net_unix!(
    impl ReadyEvent {
        pub(crate) fn with_ready(&self, ready: Ready) -> Self {
            Self {
                ready,
                tick: self.tick,
                is_shutdown: self.is_shutdown,
            }
        }
    }
);

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub(super) enum Direction {
    Read,
    Write,
}

pub(super) enum Tick {
    Set,
    Clear(u8),
}

const TOKEN_WAKEUP: mio::Token = mio::Token(0);
const TOKEN_SIGNAL: mio::Token = mio::Token(1);

fn _assert_kinds() {
    fn _assert<T: Send + Sync>() {}

    _assert::<Handle>();
}

// ===== impl Driver =====

impl Driver {
    #[cfg(test)]
    pub(crate) fn new(nevents: usize) -> io::Result<(Driver, Handle)> {
        let (mut drivers, handle) = Self::new_sharded(nevents, 1)?;
        Ok((drivers.pop().unwrap(), handle))
    }

    /// Creates `num_shards` event loops (one epoll instance each) sharing one
    /// [`Handle`]. `drivers[i]` polls shard `i`.
    pub(crate) fn new_sharded(
        nevents: usize,
        num_shards: usize,
    ) -> io::Result<(Vec<Driver>, Handle)> {
        let num_shards = num_shards.max(1);
        let mut drivers = Vec::with_capacity(num_shards);
        let mut shards = Vec::with_capacity(num_shards);

        for shard in 0..num_shards {
            let poll = mio::Poll::new()?;
            #[cfg(not(target_os = "wasi"))]
            let waker = mio::Waker::new(poll.registry(), TOKEN_WAKEUP)?;
            let registry = poll.registry().try_clone()?;
            let (registrations, synced) = RegistrationSet::new();

            drivers.push(Driver {
                shard,
                signal_ready: false,
                events: mio::Events::with_capacity(nevents),
                poll,
            });
            shards.push(Shard {
                registry,
                registrations,
                synced: Mutex::new(synced),
                #[cfg(not(target_os = "wasi"))]
                waker,
                dbg: ShardDebug::default(),
            });
        }

        let use_incoming_cpu = num_shards > 1 && incoming_cpu_placement_enabled();

        let handle = Handle {
            cpu_to_shard: if num_shards > 1 {
                cpu_to_shard_table(num_shards)
            } else {
                Box::new([])
            },
            shards: shards.into_boxed_slice(),
            use_incoming_cpu,
            next_shard: std::sync::atomic::AtomicUsize::new(0),
            metrics: IoDriverMetrics::default(),
            #[cfg(all(
                tokio_unstable,
                feature = "io-uring",
                feature = "rt",
                feature = "fs",
                target_os = "linux",
            ))]
            uring_context: Mutex::new(UringContext::new()),
            #[cfg(all(
                tokio_unstable,
                feature = "io-uring",
                feature = "rt",
                feature = "fs",
                target_os = "linux",
            ))]
            uring_probe: OnceCell::new(),
        };

        if poll_debug_enabled() && num_shards > 1 {
            for s in 0..num_shards {
                let cpus: Vec<usize> = (0..handle.cpu_to_shard.len())
                    .filter(|&c| handle.cpu_to_shard[c] as usize == s)
                    .collect();
                eprintln!("[tokio-io-debug] shard {s} <- cpus {cpus:?}");
            }
        }

        Ok((drivers, handle))
    }

    pub(crate) fn park(&mut self, rt_handle: &driver::Handle) {
        let handle = rt_handle.io();
        self.turn(handle, None);
    }

    pub(crate) fn park_timeout(&mut self, rt_handle: &driver::Handle, duration: Duration) {
        let handle = rt_handle.io();
        self.turn(handle, Some(duration));
    }

    pub(crate) fn shutdown(&mut self, rt_handle: &driver::Handle) {
        let shard = &rt_handle.io().shards[self.shard];
        let ios = shard.registrations.shutdown(&mut shard.synced.lock());

        // `shutdown()` must be called without holding the lock.
        for io in ios {
            io.shutdown();
        }
    }

    fn turn(&mut self, handle: &Handle, max_wait: Option<Duration>) {
        let shard = &handle.shards[self.shard];
        debug_assert!(!shard.registrations.is_shutdown(&shard.synced.lock()));

        shard.release_pending_registrations();

        let events = &mut self.events;

        // Block waiting for an event to happen, peeling out how many events
        // happened.
        let dbg_blocking = poll_debug_enabled() && max_wait != Some(Duration::ZERO);
        if dbg_blocking {
            shard
                .dbg
                .in_wait
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let res = self.poll.poll(events, max_wait);
        if dbg_blocking {
            shard
                .dbg
                .in_wait
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        shard.dbg_polled(max_wait != Some(Duration::ZERO));
        match res {
            Ok(()) => {}
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            #[cfg(target_os = "wasi")]
            Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
                // In case of wasm32_wasi this error happens, when trying to poll without subscriptions
                // just return from the park, as there would be nothing, which wakes us up.
            }
            Err(e) => panic!("unexpected error when polling the I/O driver: {e:?}"),
        }

        // Process all the events that came in, dispatching appropriately
        let mut ready_count = 0;
        for event in events.iter() {
            let token = event.token();

            if token == TOKEN_WAKEUP {
                // Nothing to do, the event is used to unblock the I/O driver
            } else if token == TOKEN_SIGNAL {
                self.signal_ready = true;
            } else {
                let ready = Ready::from_mio(event);
                let ptr = super::EXPOSE_IO.from_exposed_addr(token.0);

                // Safety: we ensure that the pointers used as tokens are not freed
                // until they are both deregistered from mio **and** we know the I/O
                // driver is not concurrently polling. The I/O driver holds ownership of
                // an `Arc<ScheduledIo>` so we can safely cast this to a ref.
                let io: &ScheduledIo = unsafe { &*ptr };

                io.set_readiness(Tick::Set, |curr| curr | ready);
                io.wake(ready);

                ready_count += 1;
            }
        }

        #[cfg(all(
            tokio_unstable,
            feature = "io-uring",
            feature = "rt",
            feature = "fs",
            target_os = "linux",
        ))]
        if self.shard == 0 {
            let mut guard = handle.get_uring().lock();
            let ctx = &mut *guard;
            ctx.dispatch_completions();
        }

        handle.metrics.incr_ready_count_by(ready_count);
        if poll_debug_enabled() {
            shard
                .dbg
                .events
                .fetch_add(ready_count as usize, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

impl fmt::Debug for Driver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Driver")
    }
}

impl Handle {
    /// Forces a reactor blocked in a call to `turn` to wakeup, or otherwise
    /// makes the next call to `turn` return immediately.
    ///
    /// This method is intended to be used in situations where a notification
    /// needs to otherwise be sent to the main reactor. If the reactor is
    /// currently blocked inside of `turn` then it will wake up and soon return
    /// after this method has been called. If the reactor is not currently
    /// blocked in `turn`, then the next call to `turn` will not block and
    /// return immediately.
    /// Wakes every shard's poller. Used when the caller does not know which
    /// shard is parked (timer re-registration, generic unpark).
    pub(crate) fn unpark(&self) {
        #[cfg(not(target_os = "wasi"))]
        for shard in self.shards.iter() {
            shard.waker.wake().expect("failed to wake I/O driver");
        }
    }

    /// Wakes the poller of one shard.
    pub(crate) fn unpark_shard(&self, shard: usize) {
        #[cfg(not(target_os = "wasi"))]
        self.shards[shard]
            .waker
            .wake()
            .expect("failed to wake I/O driver");
        #[cfg(target_os = "wasi")]
        let _ = shard;
    }

    /// `TOKIO_IO_POLL_DEBUG`: per shard `(threads in epoll_wait, max gap between
    /// polls ms, polls, blocking polls, events, ms since last poll)`; resets the
    /// counters.
    pub(crate) fn debug_poll_snapshot(&self) -> Vec<(usize, f64, usize, usize, usize, f64)> {
        use std::sync::atomic::Ordering::Relaxed;
        let now = dbg_epoch().elapsed().as_micros() as usize;
        self.shards
            .iter()
            .map(|s| {
                let d = &s.dbg;
                let last = d.last_poll_us.load(Relaxed);
                let since = if last == 0 {
                    -1.0
                } else {
                    now.saturating_sub(last) as f64 / 1e3
                };
                (
                    d.in_wait.load(Relaxed),
                    d.max_gap_us.swap(0, Relaxed) as f64 / 1e3,
                    d.polls.swap(0, Relaxed),
                    d.blocking_polls.swap(0, Relaxed),
                    d.events.swap(0, Relaxed),
                    since,
                )
            })
            .collect()
    }

    /// Registry of shard 0; used for the signal pipe and io_uring eventfd.
    #[allow(dead_code)]
    pub(super) fn registry(&self) -> &mio::Registry {
        &self.shards[0].registry
    }

    /// Picks the shard for a new source. `fd`, when given, is consulted for
    /// `SO_INCOMING_CPU`; otherwise (or when the CPU is unknown) shards are
    /// assigned round-robin.
    pub(crate) fn pick_shard(&self, fd: Option<std::os::raw::c_int>) -> usize {
        let n = self.shards.len();
        if n == 1 {
            return 0;
        }
        #[cfg(target_os = "linux")]
        if let (true, Some(fd)) = (self.use_incoming_cpu, fd) {
            if let Some(cpu) = incoming_cpu(fd) {
                if let Some(&s) = self.cpu_to_shard.get(cpu) {
                    return s as usize;
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = fd;
        self.next_shard
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % n
    }

    /// Registers an I/O resource with the reactor for a given `mio::Ready`
    /// state. `fd`, when given, is used only to choose the shard.
    pub(super) fn add_source(
        &self,
        source: &mut impl mio::event::Source,
        interest: Interest,
        fd: Option<std::os::raw::c_int>,
    ) -> io::Result<Arc<ScheduledIo>> {
        let shard_idx = self.pick_shard(fd);
        let shard = &self.shards[shard_idx];
        let scheduled_io = shard
            .registrations
            .allocate(&mut shard.synced.lock(), shard_idx)?;
        let token = scheduled_io.token();

        // we should remove the `scheduled_io` from the `registrations` set if registering
        // the `source` with the OS fails. Otherwise it will leak the `scheduled_io`.
        if let Err(e) = shard.registry.register(source, token, interest.to_mio()) {
            // safety: `scheduled_io` is part of the `registrations` set.
            unsafe {
                shard
                    .registrations
                    .remove(&mut shard.synced.lock(), &scheduled_io)
            };

            return Err(e);
        }

        // TODO: move this logic to `RegistrationSet` and use a `CountedLinkedList`
        self.metrics.incr_fd_count();

        Ok(scheduled_io)
    }

    /// Deregisters an I/O resource from the reactor.
    pub(super) fn deregister_source(
        &self,
        registration: &Arc<ScheduledIo>,
        source: &mut impl Source,
    ) -> io::Result<()> {
        let shard_idx = registration.shard();
        let shard = &self.shards[shard_idx];

        // Deregister the source with the OS poller **first**
        // Cleanup ALWAYS happens
        let os_result = shard.registry.deregister(source);

        if shard
            .registrations
            .deregister(&mut shard.synced.lock(), registration)
        {
            self.unpark_shard(shard_idx);
        }

        self.metrics.dec_fd_count();

        os_result // Return error after cleanup
    }
}

fn dbg_epoch() -> &'static std::time::Instant {
    static T: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    T.get_or_init(std::time::Instant::now)
}

pub(crate) fn poll_debug_enabled() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| std::env::var_os("TOKIO_IO_POLL_DEBUG").is_some())
}

impl Shard {
    fn dbg_polled(&self, blocking: bool) {
        use std::sync::atomic::Ordering::Relaxed;
        if !poll_debug_enabled() {
            return;
        }
        let now = (dbg_epoch().elapsed().as_micros() as usize).max(1);
        let last = self.dbg.last_poll_us.swap(now, Relaxed);
        if last != 0 && now > last {
            self.dbg.max_gap_us.fetch_max(now - last, Relaxed);
        }
        self.dbg.polls.fetch_add(1, Relaxed);
        if blocking {
            self.dbg.blocking_polls.fetch_add(1, Relaxed);
        }
    }

    fn release_pending_registrations(&self) {
        if self.registrations.needs_release() {
            self.registrations.release(&mut self.synced.lock());
        }
    }
}

/// `getsockopt(SO_INCOMING_CPU)`; `None` if unsupported or not yet known (-1).
#[cfg(target_os = "linux")]
fn incoming_cpu(fd: std::os::raw::c_int) -> Option<usize> {
    let mut cpu: libc::c_int = -1;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: valid pointers to a c_int and its length; fd is caller-owned.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_INCOMING_CPU,
            &mut cpu as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    (rc == 0 && cpu >= 0).then_some(cpu as usize)
}

fn incoming_cpu_placement_enabled() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        !matches!(
            std::env::var("TOKIO_IO_SHARD_KEY").as_deref(),
            Ok("rr" | "round-robin")
        )
    })
}

/// `(ncpu, Some((llc_key per cpu, sorted distinct keys)))` from sysfs, read
/// once per process. `llc_key[cpu]` is the lowest CPU sharing `cpu`'s
/// last-level cache, or `usize::MAX` if unknown (e.g. offline).
#[allow(clippy::type_complexity)]
fn cpu_topology() -> &'static (usize, Option<(Vec<usize>, Vec<usize>)>) {
    static V: std::sync::OnceLock<(usize, Option<(Vec<usize>, Vec<usize>)>)> =
        std::sync::OnceLock::new();
    V.get_or_init(read_cpu_topology)
}

fn read_cpu_topology() -> (usize, Option<(Vec<usize>, Vec<usize>)>) {
    let ncpu = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    #[cfg(target_os = "linux")]
    {
        let max_cpu = std::fs::read_to_string("/sys/devices/system/cpu/possible")
            .ok()
            .and_then(|s| {
                s.trim()
                    .rsplit(['-', ','])
                    .next()
                    .and_then(|n| n.parse::<usize>().ok())
            })
            .map_or(ncpu, |m| m + 1);
        // group key = lowest CPU sharing this CPU's last-level cache
        let mut key = vec![usize::MAX; max_cpu];
        for (cpu, k) in key.iter_mut().enumerate() {
            for idx in (0..8).rev() {
                let path =
                    format!("/sys/devices/system/cpu/cpu{cpu}/cache/index{idx}/shared_cpu_list");
                if let Ok(list) = std::fs::read_to_string(&path) {
                    let first = list
                        .trim()
                        .split(['-', ','])
                        .next()
                        .and_then(|n| n.parse::<usize>().ok());
                    *k = first.unwrap_or(cpu);
                    break;
                }
            }
        }
        let mut uniq: Vec<usize> = key.iter().copied().filter(|&k| k != usize::MAX).collect();
        uniq.sort_unstable();
        uniq.dedup();
        (
            max_cpu,
            if uniq.is_empty() {
                None
            } else {
                Some((key, uniq))
            },
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        (ncpu, None)
    }
}

/// `cpu -> shard`: CPUs sharing a last-level cache map to the same shard when
/// there are at least as many cache groups as shards; otherwise CPUs ordered
/// by (group, cpu) are cut into equal chunks. CPUs with unknown topology (and
/// every CPU when sysfs is unavailable) get `cpu % shards`.
fn cpu_to_shard_table(num_shards: usize) -> Box<[u16]> {
    let (ncpu, groups) = cpu_topology();
    let ncpu = *ncpu;
    let mut table: Vec<u16> = (0..ncpu).map(|cpu| (cpu % num_shards) as u16).collect();
    match groups {
        Some((key, uniq)) if uniq.len() >= num_shards => {
            for (cpu, k) in key.iter().enumerate() {
                if let Ok(g) = uniq.binary_search(k) {
                    table[cpu] = (g * num_shards / uniq.len()) as u16;
                }
            }
        }
        Some((key, uniq)) => {
            let mut order: Vec<usize> = (0..ncpu).filter(|&c| key[c] != usize::MAX).collect();
            order.sort_by_key(|&c| (uniq.binary_search(&key[c]).unwrap(), c));
            let n = order.len().max(1);
            for (pos, cpu) in order.into_iter().enumerate() {
                table[cpu] = (pos * num_shards / n) as u16;
            }
        }
        None => {}
    }
    table.into_boxed_slice()
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Handle")
    }
}

impl Direction {
    pub(super) fn mask(self) -> Ready {
        match self {
            Direction::Read => Ready::READABLE | Ready::READ_CLOSED,
            Direction::Write => Ready::WRITABLE | Ready::WRITE_CLOSED,
        }
    }
}
