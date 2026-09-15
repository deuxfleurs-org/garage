//! Module containing helper functions to manipulate time
use chrono::{LocalResult, SecondsFormat, TimeZone, Utc};
use std::convert::TryInto;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::Error;

/// Maximum timestamp (in milliseconds since UNIX epoch) that we are willing
/// to represent. chrono supports dates up to ~year 26214468 AD (~8.3e14 s)
/// and HTTP dates only up to year 9999, so this value (~year 2220 AD) is
/// safely below both limits. Any larger value is considered corrupted and
/// clamped.
pub const MAX_MSECS: u64 = 7_900_000_000_000;

/// Clamp a millisecond timestamp to the range that our date helpers can
/// represent.
///
/// Corrupted values (e.g. `u64::MAX` stored in the object table) are mapped
/// to the maximum representable date instead of causing panics in the
/// various date formatting code paths (RFC3339, HTTP dates, ...).
pub fn clamp_msec(msecs: u64) -> u64 {
	msecs.min(MAX_MSECS)
}

/// Returns milliseconds since UNIX Epoch
pub fn now_msec() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("Fix your clock :o")
		.as_millis() as u64
}

/// Increment logical clock
pub fn increment_logical_clock(prev: u64) -> u64 {
	std::cmp::max(prev + 1, now_msec())
}

/// Increment two logical clocks
pub fn increment_logical_clock_2(prev: u64, prev2: u64) -> u64 {
	std::cmp::max(prev2 + 1, std::cmp::max(prev + 1, now_msec()))
}

/// Convert a timestamp represented as milliseconds since UNIX Epoch to
/// its RFC3339 representation, such as "2021-01-01T12:30:00Z"
///
/// Timestamps that are out of the range representable by chrono (e.g.
/// corrupted values such as `u64::MAX` stored in the object table) are
/// clamped to the maximum representable date instead of panicking, so
/// that a single corrupt entry cannot crash the node.
pub fn msec_to_rfc3339(msecs: u64) -> String {
	let msecs = clamp_msec(msecs);
	let secs = (msecs / 1000) as i64;
	let nanos = ((msecs % 1000) * 1_000_000) as u32;
	let timestamp = match Utc.timestamp_opt(secs, nanos) {
		LocalResult::Single(ts) => ts,
		res => {
			warn!(
				"msec_to_rfc3339: timestamp {} ms is not representable ({:?}), clamping to epoch",
				msecs, res
			);
			// The epoch is always representable
			Utc.timestamp_opt(0, 0).unwrap()
		}
	};
	timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Parse a systemd-style duration using fundu
pub fn parse_duration(s: &str) -> Result<Duration, Error> {
	fundu_systemd::parse(s, Some(fundu::TimeUnit::Second), None)
		.map_err(|err| Error::Message(err.to_string()))
		.and_then(|dur| {
			dur.try_into()
				.map_err(|err: fundu::TryFromDurationError| Error::Message(err.to_string()))
		})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_clamp_msec() {
		assert_eq!(clamp_msec(0), 0);
		assert_eq!(clamp_msec(1641394898314), 1641394898314);
		assert_eq!(clamp_msec(MAX_MSECS), MAX_MSECS);
		assert_eq!(clamp_msec(MAX_MSECS + 1), MAX_MSECS);
		assert_eq!(clamp_msec(u64::MAX), MAX_MSECS);
	}

	#[test]
	fn test_msec_to_rfc3339() {
		assert_eq!(msec_to_rfc3339(0), "1970-01-01T00:00:00.000Z");
		assert_eq!(
			msec_to_rfc3339(1641394898314),
			Utc.timestamp_opt(1641394898, 314_000_000)
				.unwrap()
				.to_rfc3339_opts(SecondsFormat::Millis, true)
		);
	}

	#[test]
	fn test_msec_to_rfc3339_clamps_corrupted_values() {
		// Corrupted or out-of-range values must not panic and must be
		// clamped to the maximum representable date
		let clamped = msec_to_rfc3339(MAX_MSECS);
		let expected = Utc
			.timestamp_opt((MAX_MSECS / 1000) as i64, 0)
			.unwrap()
			.to_rfc3339_opts(SecondsFormat::Millis, true);
		assert_eq!(clamped, expected);
		// The clamped date must be far in the future, making corrupted
		// entries easy to spot in list results
		assert!(clamped.as_str() > "2100");

		assert_eq!(msec_to_rfc3339(u64::MAX), clamped);
		assert_eq!(msec_to_rfc3339(u64::MAX / 2), clamped);
		assert_eq!(msec_to_rfc3339(1u64 << 63), clamped);
		assert_eq!(msec_to_rfc3339(MAX_MSECS + 1), clamped);
	}
}
