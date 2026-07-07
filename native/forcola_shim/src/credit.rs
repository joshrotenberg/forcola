//! Read-budget accounting for demand-driven backpressure on the child's
//! stdout pump.
//!
//! When the BEAM opts into backpressure (a `window_bytes` field in the SPAWN
//! payload), the stdout pump reads the child only while it holds credit. The
//! BEAM grants credit with CREDIT frames as its consumer pulls lines; when the
//! credit reaches zero the pump stops reading, the OS pipe fills, and the
//! child's next write blocks. That block is the backpressure.
//!
//! The pump (the single credit consumer) and the supervisor (which grants
//! credit on CREDIT frames and uncorks on child exit) coordinate through a
//! mutex plus condvar, so a credit-starved pump parks instead of busy-spinning.

use std::sync::{Arc, Condvar, Mutex};

#[derive(Debug)]
struct State {
    /// Bytes the pump may still read from the child and forward.
    available: u64,
    /// Set once the child has exited or the run is tearing down: the pump
    /// stops gating and drains whatever remains in the pipe to EOF, so
    /// buffered output is not lost.
    uncorked: bool,
}

/// A shared read budget. Cloneable: the pump and the supervisor hold handles
/// to the same counter.
#[derive(Clone)]
pub struct Credit {
    inner: Arc<(Mutex<State>, Condvar)>,
}

/// What the pump may do on its next read.
pub enum Permit {
    /// Read at most this many bytes; credit was available.
    Read(usize),
    /// Ignore the budget and drain the pipe to EOF; the child is gone.
    Uncork,
}

impl Credit {
    /// A fresh budget starting at zero: the pump parks until the first grant.
    pub fn new() -> Self {
        Credit {
            inner: Arc::new((
                Mutex::new(State {
                    available: 0,
                    uncorked: false,
                }),
                Condvar::new(),
            )),
        }
    }

    /// Add `n` bytes of budget and wake a parked pump.
    pub fn grant(&self, n: u64) {
        let (lock, cvar) = &*self.inner;
        let mut st = lock.lock().unwrap();
        st.available = st.available.saturating_add(n);
        cvar.notify_all();
    }

    /// Stop gating: the pump drains to EOF regardless of budget. Called once
    /// the child has exited so buffered output already in the pipe is not
    /// lost, and so a parked pump wakes promptly for teardown.
    pub fn uncork(&self) {
        let (lock, cvar) = &*self.inner;
        let mut st = lock.lock().unwrap();
        st.uncorked = true;
        cvar.notify_all();
    }

    /// Block until there is budget to read or the budget has been uncorked.
    /// Returns how many bytes may be read (capped at `max`), or `Uncork`.
    ///
    /// The budget is peeked, not deducted: the caller reads up to the returned
    /// cap and then calls [`Credit::consume`] with the actual byte count, so a
    /// short read does not waste budget. The stdout pump is the only consumer,
    /// so the peek-then-consume gap cannot underflow.
    pub fn await_permit(&self, max: usize) -> Permit {
        let (lock, cvar) = &*self.inner;
        let mut st = lock.lock().unwrap();
        loop {
            if st.uncorked {
                return Permit::Uncork;
            }
            if st.available > 0 {
                let cap = st.available.min(max as u64) as usize;
                return Permit::Read(cap);
            }
            st = cvar.wait(st).unwrap();
        }
    }

    /// Deduct `n` bytes actually read and forwarded from the budget.
    pub fn consume(&self, n: usize) {
        let (lock, _) = &*self.inner;
        let mut st = lock.lock().unwrap();
        st.available = st.available.saturating_sub(n as u64);
    }
}

/// Parses an 8-byte big-endian credit grant from a CREDIT frame payload.
/// Returns `None` on a wrong-length payload rather than panicking on a
/// malformed frame.
pub fn parse_grant(payload: &[u8]) -> Option<u64> {
    let bytes: [u8; 8] = payload.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn parse_grant_reads_a_big_endian_u64() {
        assert_eq!(parse_grant(&4096u64.to_be_bytes()), Some(4096));
        assert_eq!(parse_grant(&0u64.to_be_bytes()), Some(0));
        assert_eq!(parse_grant(&u64::MAX.to_be_bytes()), Some(u64::MAX));
    }

    #[test]
    fn parse_grant_rejects_wrong_length() {
        assert_eq!(parse_grant(&[]), None);
        assert_eq!(parse_grant(&[0, 0, 0, 1]), None);
        assert_eq!(parse_grant(&[0; 9]), None);
    }

    #[test]
    fn permit_caps_at_available_then_at_max() {
        let c = Credit::new();
        c.grant(10);
        // Available (10) is below max (100): capped at available.
        match c.await_permit(100) {
            Permit::Read(n) => assert_eq!(n, 10),
            Permit::Uncork => panic!("unexpected uncork"),
        }
        // Max (4) is below available (10): capped at max.
        match c.await_permit(4) {
            Permit::Read(n) => assert_eq!(n, 4),
            Permit::Uncork => panic!("unexpected uncork"),
        }
    }

    #[test]
    fn consume_deducts_from_the_budget() {
        let c = Credit::new();
        c.grant(10);
        c.consume(7);
        match c.await_permit(100) {
            Permit::Read(n) => assert_eq!(n, 3),
            Permit::Uncork => panic!("unexpected uncork"),
        }
    }

    #[test]
    fn await_permit_parks_at_zero_then_wakes_on_grant() {
        let c = Credit::new();
        let c2 = c.clone();
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            // Parks here until the main thread grants credit.
            let permit = c2.await_permit(100);
            let _ = tx.send(());
            matches!(permit, Permit::Read(50))
        });

        // The pump must still be parked: nothing was granted yet.
        assert!(
            rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "await_permit returned before any credit was granted"
        );

        c.grant(50);
        rx.recv_timeout(Duration::from_secs(5))
            .expect("await_permit did not wake after a grant");
        assert!(handle.join().unwrap());
    }

    #[test]
    fn uncork_wakes_a_parked_pump() {
        let c = Credit::new();
        let c2 = c.clone();
        let handle = thread::spawn(move || matches!(c2.await_permit(100), Permit::Uncork));
        c.uncork();
        assert!(handle.join().unwrap(), "expected Uncork after uncork()");
    }
}
