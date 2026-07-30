use std::time::Duration;

#[cfg(unix)]
use nix::sys::signal::{kill, Signal};
#[cfg(unix)]
use nix::unistd::Pid;

/// Compute how long `pull_one` should block on the next event.
///
/// While a turn is in flight, wait the FULL remaining hang-watchdog budget so
/// slow-but-valid protocol events are not lost to an idle poll timeout. When
/// idle, poll on the short `poll_idle` interval.
pub fn next_wait(
    turn_in_flight: bool,
    elapsed: Duration,
    hang_watchdog: Duration,
    poll_idle: Duration,
) -> Duration {
    if turn_in_flight {
        hang_watchdog
            .saturating_sub(elapsed)
            .max(Duration::from_millis(1))
    } else {
        poll_idle
    }
}

#[cfg(unix)]
pub(crate) fn best_effort_signal(target: i32, sig: Signal) {
    let _ = kill(Pid::from_raw(target), sig);
}

pub(crate) async fn escalate(
    child: &mut tokio::process::Child,
    signal_target: Option<i32>,
    interrupt_ack: Duration,
    term_after: Duration,
    kill_after: Duration,
) -> Option<i32> {
    if let Some(target) = signal_target {
        best_effort_signal(target, Signal::SIGINT);
    }
    if let Some(code) = wait_for_exit(child, interrupt_ack).await {
        return code;
    }
    if let Some(target) = signal_target {
        best_effort_signal(target, Signal::SIGTERM);
    }
    if let Some(code) = wait_for_exit(child, term_after).await {
        return code;
    }
    if let Some(target) = signal_target {
        best_effort_signal(target, Signal::SIGKILL);
    }
    let _ = child.start_kill();
    if let Some(code) = wait_for_exit(child, kill_after).await {
        return code;
    }
    wait_for_exit(child, kill_after).await.flatten()
}

pub(crate) async fn wait_for_exit(
    child: &mut tokio::process::Child,
    bound: Duration,
) -> Option<Option<i32>> {
    match tokio::time::timeout(bound, child.wait()).await {
        Ok(Ok(status)) => Some(status.code()),
        Ok(Err(_)) => Some(None),
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WATCHDOG: Duration = Duration::from_secs(60);
    const IDLE: Duration = Duration::from_millis(500);

    #[test]
    fn next_wait_in_flight_uses_full_watchdog_not_poll_idle() {
        let w = next_wait(true, Duration::from_millis(10), WATCHDOG, IDLE);
        assert_eq!(w, WATCHDOG - Duration::from_millis(10));
        assert!(w > IDLE);
    }

    #[test]
    fn next_wait_in_flight_near_watchdog_returns_small_remainder() {
        let elapsed = Duration::from_millis(59_900);
        let w = next_wait(true, elapsed, WATCHDOG, IDLE);
        assert_eq!(w, Duration::from_millis(100));
    }

    #[test]
    fn next_wait_in_flight_past_watchdog_clamps_to_one_millisecond() {
        let w = next_wait(true, WATCHDOG + Duration::from_secs(5), WATCHDOG, IDLE);
        assert_eq!(w, Duration::from_millis(1));
    }

    #[test]
    fn next_wait_idle_is_exactly_poll_idle_regardless_of_elapsed() {
        assert_eq!(
            next_wait(false, Duration::from_millis(0), WATCHDOG, IDLE),
            IDLE
        );
        assert_eq!(
            next_wait(false, Duration::from_secs(120), WATCHDOG, IDLE),
            IDLE
        );
    }

    #[test]
    fn next_wait_policy_inflight_vs_idle() {
        assert_eq!(
            next_wait(true, Duration::from_millis(10), WATCHDOG, IDLE),
            WATCHDOG - Duration::from_millis(10)
        );
        assert_eq!(
            next_wait(true, WATCHDOG + Duration::from_secs(1), WATCHDOG, IDLE),
            Duration::from_millis(1)
        );
        assert_eq!(
            next_wait(false, Duration::from_secs(999), WATCHDOG, IDLE),
            IDLE
        );
    }
}
