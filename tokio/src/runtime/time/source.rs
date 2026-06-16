use super::MAX_SAFE_MILLIS_DURATION;
use crate::time::{Clock, Duration, Instant};

/// A structure which handles conversion from Instants to `u64` timestamps.
#[derive(Debug)]
pub(crate) struct TimeSource {
    start_time: Instant,
}

impl TimeSource {
    pub(crate) fn new(clock: &Clock) -> Self {
        Self {
            start_time: clock.now(),
        }
    }

    #[cfg_attr(feature = "test-util", allow(dead_code))]
    pub(crate) fn deadline_to_tick(&self, t: Instant) -> u64 {
        // Round up to the end of a ms
        self.instant_to_tick(t + Duration::from_nanos(999_999))
    }

    pub(crate) fn instant_to_tick(&self, t: Instant) -> u64 {
        // round up
        let dur: Duration = t.saturating_duration_since(self.start_time);
        let ms = dur
            .as_millis()
            .try_into()
            .unwrap_or(MAX_SAFE_MILLIS_DURATION);
        ms.min(MAX_SAFE_MILLIS_DURATION)
    }

    // With test-util the traditional driver works in the ns domain; the ms
    // tick conversions remain for the not(test-util) build, the alternative
    // driver, and tests, which not every cfg combination compiles.
    #[cfg_attr(feature = "test-util", allow(dead_code))]
    pub(crate) fn tick_to_duration(&self, t: u64) -> Duration {
        Duration::from_millis(t)
    }

    /// Converts an `Instant` to whole nanoseconds since the driver's start
    /// time. Under `test-util` this is the wheel's tick unit.
    ///
    /// Saturates to `MAX_SAFE_MILLIS_DURATION` — the largest value below the
    /// timer state sentinels; the cap is a raw `u64` shared by both tick
    /// units. As nanoseconds it is roughly 584 years, far beyond
    /// `Instant::far_future()`.
    #[cfg(feature = "test-util")]
    pub(crate) fn instant_to_nanos(&self, t: Instant) -> u64 {
        let dur: Duration = t.saturating_duration_since(self.start_time);
        let ns = dur
            .as_nanos()
            .try_into()
            .unwrap_or(MAX_SAFE_MILLIS_DURATION);
        ns.min(MAX_SAFE_MILLIS_DURATION)
    }

    /// Converts nanoseconds since the driver's start time back to an
    /// `Instant`, saturating to `Instant::far_future()` (mirrors
    /// `tick_to_instant`).
    #[cfg(feature = "test-util")]
    pub(crate) fn nanos_to_instant(&self, ns: u64) -> Instant {
        self.start_time
            .checked_add(Duration::from_nanos(ns))
            .unwrap_or_else(Instant::far_future)
    }

    #[cfg_attr(feature = "test-util", allow(dead_code))]
    pub(crate) fn now(&self, clock: &Clock) -> u64 {
        self.instant_to_tick(clock.now())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) fn start_time(&self) -> Instant {
        self.start_time
    }
}
