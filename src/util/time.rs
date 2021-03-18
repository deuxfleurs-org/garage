use chrono::{SecondsFormat, TimeZone, Utc};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_msec() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("Fix your clock :o")
		.as_millis() as u64
}

pub fn msec_to_rfc3339(msecs: u64) -> String {
	let secs = msecs as i64 / 1000;
	let nanos = (msecs as i64 % 1000) as u32 * 1_000_000;
	let timestamp = Utc.timestamp(secs, nanos);
	timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)
}
