//! Exact-deadline timer store for the paused clock.
//!
//! While the clock is paused, timer registrations bypass the wheel and land
//! here, keyed by (nanoseconds since driver start, registration sequence
//! number) so that virtual time can move to exact deadlines and same-instant
//! timers fire in registration order. Lives inside the driver's
//! mutex-protected `InnerState`, next to the wheel; every method requires the
//! driver lock.
//!
//! Entries here are the same pinned `TimerShared` allocations the wheel uses,
//! and follow the same `StateCell` protocol; only the unit in the state cell
//! differs (nanoseconds instead of ms ticks).

use crate::runtime::time::{TimerHandle, TimerShared};

use std::collections::BTreeMap;
use std::ptr::NonNull;

/// Ordered set of exact-deadline timers. See the module docs.
#[derive(Debug)]
pub(super) struct ExactStore {
    /// Entries keyed by (deadline in ns since driver start, seq).
    entries: BTreeMap<(u64, u64), TimerHandle>,

    /// Next registration sequence number. Monotonic per driver; ties between
    /// equal deadlines resolve in registration order through this value.
    next_seq: u64,
}

// SAFETY: The store owns `TimerHandle`s -- raw pointers to pinned
// `TimerShared` allocations, which are themselves `Send + Sync`. Every access
// happens behind the driver lock, the same discipline that makes the wheel's
// `EntryList` (`LinkedList<TimerShared, TimerShared>`) `Send + Sync`.
unsafe impl Send for ExactStore {}
unsafe impl Sync for ExactStore {}

impl ExactStore {
    pub(super) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            next_seq: 0,
        }
    }

    /// Inserts an entry, reading its true deadline via `sync_when()` (the
    /// entry's state cell must already hold its ns deadline — callers do
    /// `set_expiration(when_ns)` first, mirroring `Handle::reregister`'s
    /// wheel order), assigning the next sequence number and recording the
    /// (ns, seq) key on the entry.
    ///
    /// SAFETY: The driver lock must be held; the entry must be in no driver
    /// structure and must remain pinned while in the store.
    pub(super) unsafe fn insert(&mut self, item: TimerHandle) {
        // Same call as Wheel::insert: read the true deadline and refresh the
        // registered position. SAFETY: per this fn's contract.
        let when = unsafe { item.sync_when() };

        let seq = self.next_seq;
        self.next_seq += 1;

        // SAFETY: driver lock held, entry in no structure (per contract).
        unsafe { item.set_exact_key(when, seq) };

        let prev = self.entries.insert((when, seq), item);
        debug_assert!(prev.is_none(), "exact-store key collision");
    }

    /// Removes an entry by its recorded key; no-op if the entry is not in
    /// the store.
    ///
    /// SAFETY: The driver lock must be held.
    pub(super) unsafe fn remove(&mut self, item: NonNull<TimerShared>) {
        // SAFETY: driver lock held (per contract); the reference does not
        // outlive this call.
        let key = unsafe { item.as_ref().exact_key() };
        if let Some(key) = key {
            let removed = self.entries.remove(&key);
            debug_assert!(removed.is_some(), "exact-store entry missing");
            // SAFETY: just removed from the map under the driver lock.
            unsafe { item.as_ref().clear_exact_key() };
        }
    }

    /// Returns the earliest deadline (ns since driver start) in the store.
    pub(super) fn next_deadline(&self) -> Option<u64> {
        self.entries.first_key_value().map(|((when, _), _)| *when)
    }

    /// Pops the next entry due at or before `now_ns`, committing it via
    /// `mark_pending`. An entry whose deadline was concurrently extended is
    /// reinserted at its true deadline keeping its original seq, and scanning
    /// continues.
    ///
    /// SAFETY: The driver lock must be held; returned handles must be fired
    /// before the lock is released.
    pub(super) unsafe fn pop_due(&mut self, now_ns: u64) -> Option<TimerHandle> {
        loop {
            let (&(when, seq), _) = self.entries.first_key_value()?;
            if when > now_ns {
                return None;
            }

            let item = self.entries.remove(&(when, seq)).unwrap();

            // SAFETY: driver lock held; entry just removed from the map.
            match unsafe { item.mark_pending(now_ns) } {
                Ok(()) => {
                    // Committed to fire. mark_pending set registered_when to
                    // STATE_DEREGISTERED (the wheel's pending-list marker);
                    // harmless -- store entries never enter wheel lists.
                    unsafe { item.clear_exact_key() };
                    return Some(item);
                }
                Err(true_when) => {
                    // A lock-free extend moved the deadline later. Reinsert at
                    // the true deadline, keeping the original seq so the
                    // entry's tie-break stays its original registration order.
                    debug_assert!(true_when > now_ns);
                    unsafe { item.set_exact_key(true_when, seq) };
                    self.entries.insert((true_when, seq), item);
                }
            }
        }
    }

    /// Returns the number of entries in the store.
    #[cfg_attr(not(test), allow(dead_code))] // only test code consumes this
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::pin::Pin;

    /// A pinned `TimerShared` with its state cell primed to `when_ns`,
    /// mirroring what registration does before a store insert.
    fn entry(when_ns: u64) -> Pin<Box<TimerShared>> {
        let shared = Box::pin(TimerShared::new());
        // SAFETY: single-threaded test; entry is in no driver structure.
        unsafe { shared.handle().set_expiration(when_ns) };
        shared
    }

    #[test]
    fn orders_by_deadline() {
        let a = entry(300);
        let b = entry(100);
        let c = entry(200);

        let mut store = ExactStore::new();
        // SAFETY: single-threaded test; entries are pinned, in no other
        // structure, and outlive the store.
        unsafe {
            store.insert(a.handle());
            store.insert(b.handle());
            store.insert(c.handle());

            assert!(store.pop_due(1_000).unwrap().ptr_eq(NonNull::from(&*b)));
            assert!(store.pop_due(1_000).unwrap().ptr_eq(NonNull::from(&*c)));
            assert!(store.pop_due(1_000).unwrap().ptr_eq(NonNull::from(&*a)));
            assert!(store.pop_due(1_000).is_none());
        }
    }

    #[test]
    fn same_deadline_pops_in_registration_order() {
        // Store-level contract: identical deadlines fire in registration
        // order. Also pins boundary inclusivity: entries due at exactly
        // `now_ns` pop.
        let a = entry(500);
        let b = entry(500);

        let mut store = ExactStore::new();
        // SAFETY: as in orders_by_deadline.
        unsafe {
            store.insert(a.handle());
            store.insert(b.handle());

            assert!(store.pop_due(500).unwrap().ptr_eq(NonNull::from(&*a)));
            assert!(store.pop_due(500).unwrap().ptr_eq(NonNull::from(&*b)));
            assert!(store.pop_due(500).is_none());
        }
    }

    #[test]
    fn pop_due_respects_now() {
        let a = entry(100);
        let b = entry(900);

        let mut store = ExactStore::new();
        // SAFETY: as in orders_by_deadline.
        unsafe {
            store.insert(a.handle());
            store.insert(b.handle());

            assert!(store.pop_due(500).unwrap().ptr_eq(NonNull::from(&*a)));
            assert!(store.pop_due(500).is_none());
        }

        assert_eq!(store.next_deadline(), Some(900));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn remove_unlinks_and_clears_key() {
        let a = entry(100);
        let ptr = NonNull::from(&*a);

        let mut store = ExactStore::new();
        // SAFETY: as in orders_by_deadline.
        unsafe {
            store.insert(a.handle());
            assert_eq!(store.len(), 1);
            assert!(a.exact_key().is_some());

            store.remove(ptr);
            assert_eq!(store.len(), 0);
            assert_eq!(a.exact_key(), None);

            // A second remove of the same entry is a no-op.
            store.remove(ptr);
            assert_eq!(store.len(), 0);
        }
    }

    #[test]
    fn lazily_extended_entry_reinserts_at_true_deadline() {
        let a = entry(100);

        let mut store = ExactStore::new();
        // SAFETY: as in orders_by_deadline.
        unsafe {
            store.insert(a.handle());

            // The lock-free reset fast path: move the true deadline later
            // directly on the shared state, without touching the store.
            a.extend_expiration(200).unwrap();

            assert!(store.pop_due(150).is_none());
            assert_eq!(store.len(), 1);
            assert_eq!(store.next_deadline(), Some(200));
            // Reinserted at the true deadline, keeping its original seq.
            assert_eq!(a.exact_key(), Some((200, 0)));

            assert!(store.pop_due(250).unwrap().ptr_eq(NonNull::from(&*a)));
            assert_eq!(store.len(), 0);
        }
    }

    #[test]
    fn drain_with_max_pops_everything_in_order() {
        let a = entry(300); // seq 0
        let b = entry(100); // seq 1
        let c = entry(100); // seq 2

        let mut store = ExactStore::new();
        // SAFETY: as in orders_by_deadline.
        unsafe {
            store.insert(a.handle());
            store.insert(b.handle());
            store.insert(c.handle());

            // (when, seq) order: (100, 1), (100, 2), (300, 0).
            assert!(store.pop_due(u64::MAX).unwrap().ptr_eq(NonNull::from(&*b)));
            assert!(store.pop_due(u64::MAX).unwrap().ptr_eq(NonNull::from(&*c)));
            assert!(store.pop_due(u64::MAX).unwrap().ptr_eq(NonNull::from(&*a)));
            assert!(store.pop_due(u64::MAX).is_none());
        }

        assert_eq!(store.len(), 0);
    }
}
