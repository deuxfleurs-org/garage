use std::sync::Arc;

use async_trait::async_trait;
use chrono::prelude::*;
use std::time::{Duration, Instant};
use tokio::sync::watch;

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::{Error, OkOrMessage};
use garage_util::persister::PersisterShared;
use garage_util::time::*;

use garage_table::EmptyKey;

use crate::bucket_table::*;
use crate::s3::object_table::*;

use crate::garage::Garage;

mod v090 {
	use chrono::naive::NaiveDate;
	use serde::{Deserialize, Serialize};

	#[derive(Serialize, Deserialize, Default, Clone, Copy)]
	pub struct LifecycleWorkerPersisted {
		pub last_completed: Option<NaiveDate>,
	}

	impl garage_util::migrate::InitialFormat for LifecycleWorkerPersisted {
		const VERSION_MARKER: &'static [u8] = b"G09lwp";
	}
}

pub use v090::*;

pub struct LifecycleWorker {
	garage: Arc<Garage>,

	state: State,

	persister: PersisterShared<LifecycleWorkerPersisted>,
}

enum State {
	Completed(NaiveDate),
	Running {
		date: NaiveDate,
		pos: Vec<u8>,
		counter: usize,
		objects_expired: usize,
		mpu_aborted: usize,
		last_bucket: Option<Bucket>,
	},
}

pub fn register_bg_vars(
	persister: &PersisterShared<LifecycleWorkerPersisted>,
	vars: &mut vars::BgVars,
) {
	vars.register_ro(persister, "lifecycle-last-completed", |p| {
		p.get_with(|x| {
			x.last_completed
				.map(|date| date.to_string())
				.unwrap_or("never".to_string())
		})
	});
}

impl LifecycleWorker {
	pub fn new(garage: Arc<Garage>, persister: PersisterShared<LifecycleWorkerPersisted>) -> Self {
		let today = today();
		let state = match persister.get_with(|x| x.last_completed) {
			Some(d) if d >= today => State::Completed(d),
			_ => State::Running {
				date: today,
				pos: vec![],
				counter: 0,
				objects_expired: 0,
				mpu_aborted: 0,
				last_bucket: None,
			},
		};
		Self {
			garage,
			state,
			persister,
		}
	}
}

#[async_trait]
impl Worker for LifecycleWorker {
	fn name(&self) -> String {
		"object lifecycle worker".to_string()
	}

	fn status(&self) -> WorkerStatus {
		match &self.state {
			State::Completed(d) => WorkerStatus {
				freeform: vec![format!("Last completed: {}", d)],
				..Default::default()
			},
			State::Running {
				date,
				counter,
				objects_expired,
				mpu_aborted,
				..
			} => {
				let n_objects = self
					.garage
					.object_table
					.data
					.store
					.fast_len()
					.unwrap_or(None);
				let progress = match n_objects {
					None => "...".to_string(),
					Some(total) => format!(
						"~{:.2}%",
						100. * std::cmp::min(*counter, total) as f32 / total as f32
					),
				};
				WorkerStatus {
					progress: Some(progress),
					freeform: vec![
						format!("Started: {}", date),
						format!("Objects expired: {}", objects_expired),
						format!("Multipart uploads aborted: { }", mpu_aborted),
					],
					..Default::default()
				}
			}
		}
	}

	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		match &mut self.state {
			State::Completed(_) => Ok(WorkerState::Idle),
			State::Running {
				date,
				counter,
				objects_expired,
				mpu_aborted,
				pos,
				last_bucket,
			} => {
				let (object_bytes, next_pos) = match self
					.garage
					.object_table
					.data
					.store
					.get_gt(&pos)?
				{
					None => {
						info!("Lifecycle worker finished for {}, objects expired: {}, mpu aborted: {}", date, *objects_expired, *mpu_aborted);
						self.persister
							.set_with(|x| x.last_completed = Some(*date))?;
						self.state = State::Completed(*date);
						return Ok(WorkerState::Idle);
					}
					Some((k, v)) => (v, k),
				};

				let object = self.garage.object_table.data.decode_entry(&object_bytes)?;
				process_object(
					&self.garage,
					*date,
					object,
					objects_expired,
					mpu_aborted,
					last_bucket,
				)
				.await?;

				*counter += 1;
				*pos = next_pos;

				Ok(WorkerState::Busy)
			}
		}
	}

	async fn wait_for_work(&mut self) -> WorkerState {
		match &self.state {
			State::Completed(d) => {
				let now = now_msec();
				let next_start = midnight_ts(d.succ_opt().expect("no next day"));
				if now < next_start {
					tokio::time::sleep_until(
						(Instant::now() + Duration::from_millis(next_start - now)).into(),
					)
					.await;
				}
				self.state = State::Running {
					date: today(),
					pos: vec![],
					counter: 0,
					objects_expired: 0,
					mpu_aborted: 0,
					last_bucket: None,
				};
			}
			State::Running { .. } => (),
		}
		WorkerState::Busy
	}
}

async fn process_object(
	garage: &Arc<Garage>,
	now_date: NaiveDate,
	object: Object,
	objects_expired: &mut usize,
	mpu_aborted: &mut usize,
	last_bucket: &mut Option<Bucket>,
) -> Result<(), Error> {
	let bucket = match last_bucket.take() {
		Some(b) if b.id == object.bucket_id => b,
		_ => garage
			.bucket_table
			.get(&EmptyKey, &object.bucket_id)
			.await?
			.ok_or_message("object in non-existent bucket")?,
	};

	let lifecycle_policy: &[LifecycleRule] = bucket
		.state
		.as_option()
		.and_then(|s| s.lifecycle_config.get().as_deref())
		.unwrap_or_default();

	for rule in lifecycle_policy.iter() {
		if let Some(pfx) = &rule.filter.prefix {
			if !object.key.starts_with(pfx) {
				continue;
			}
		}

		if let Some(expire) = &rule.expiration {
			if let Some(current_version) = object.versions().iter().rev().find(|v| v.is_data()) {
				let version_date = next_date(current_version.timestamp);

				let current_version_data = match &current_version.state {
					ObjectVersionState::Complete(c) => c,
					_ => unreachable!(),
				};

				let size_match = check_size_filter(current_version_data, &rule.filter);
				let date_match = match expire {
					LifecycleExpiration::AfterDays(n_days) => {
						(now_date - version_date) >= chrono::Duration::days(*n_days as i64)
					}
					LifecycleExpiration::AtDate(exp_date) => now_date >= *exp_date,
				};

				if size_match && date_match {
					// Delete expired version
					let deleted_object = Object::new(
						object.bucket_id,
						object.key.clone(),
						vec![ObjectVersion {
							uuid: gen_uuid(),
							timestamp: std::cmp::max(now_msec(), current_version.timestamp + 1),
							state: ObjectVersionState::Complete(ObjectVersionData::DeleteMarker),
						}],
					);
					garage.object_table.insert(&deleted_object).await?;
					*objects_expired += 1;
				}
			}
		}

		if let Some(abort_mpu_days) = &rule.abort_incomplete_mpu_days {
			let aborted_versions = object
				.versions()
				.iter()
				.filter_map(|v| {
					let version_date = next_date(v.timestamp);
					match &v.state {
						ObjectVersionState::Uploading { .. }
							if (now_date - version_date)
								>= chrono::Duration::days(*abort_mpu_days as i64) =>
						{
							Some(ObjectVersion {
								state: ObjectVersionState::Aborted,
								..*v
							})
						}
						_ => None,
					}
				})
				.collect::<Vec<_>>();
			if !aborted_versions.is_empty() {
				// Insert aborted mpu info
				let n_aborted = aborted_versions.len();
				let aborted_object =
					Object::new(object.bucket_id, object.key.clone(), aborted_versions);
				garage.object_table.insert(&aborted_object).await?;
				*mpu_aborted += n_aborted;
			}
		}
	}

	*last_bucket = Some(bucket);
	Ok(())
}

fn check_size_filter(version_data: &ObjectVersionData, filter: &LifecycleFilter) -> bool {
	let size = match version_data {
		ObjectVersionData::Inline(meta, _) | ObjectVersionData::FirstBlock(meta, _) => meta.size,
		_ => unreachable!(),
	};
	if let Some(size_gt) = filter.size_gt {
		if !(size > size_gt) {
			return false;
		}
	}
	if let Some(size_lt) = filter.size_lt {
		if !(size < size_lt) {
			return false;
		}
	}
	return true;
}

fn midnight_ts(date: NaiveDate) -> u64 {
	date.and_hms_opt(0, 0, 0)
		.expect("midnight does not exist")
		.timestamp_millis() as u64
}

fn next_date(ts: u64) -> NaiveDate {
	NaiveDateTime::from_timestamp_millis(ts as i64)
		.expect("bad timestamp")
		.date()
		.succ_opt()
		.expect("no next day")
}

fn today() -> NaiveDate {
	Utc::now().naive_utc().date()
}
