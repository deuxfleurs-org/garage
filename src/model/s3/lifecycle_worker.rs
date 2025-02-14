use std::sync::Arc;

use async_trait::async_trait;
use chrono::prelude::*;
use std::time::{Duration, Instant};
use tokio::sync::watch;

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::Error;
use garage_util::persister::PersisterShared;
use garage_util::time::*;

use garage_table::EmptyKey;

use crate::bucket_table::*;
use crate::s3::object_table::*;

use crate::garage::Garage;

mod v090 {
	use serde::{Deserialize, Serialize};

	#[derive(Serialize, Deserialize, Default, Clone)]
	pub struct LifecycleWorkerPersisted {
		pub last_completed: Option<String>,
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum Skip {
	SkipBucket,
	NextObject,
}

pub fn register_bg_vars(
	persister: &PersisterShared<LifecycleWorkerPersisted>,
	vars: &mut vars::BgVars,
) {
	vars.register_ro(persister, "lifecycle-last-completed", |p| {
		p.get_with(|x| x.last_completed.clone().unwrap_or("never".to_string()))
	});
}

impl LifecycleWorker {
	pub fn new(garage: Arc<Garage>, persister: PersisterShared<LifecycleWorkerPersisted>) -> Self {
		let today = today(garage.config.use_local_tz);
		let last_completed = persister.get_with(|x| {
			x.last_completed
				.as_deref()
				.and_then(|x| x.parse::<NaiveDate>().ok())
		});
		let state = match last_completed {
			Some(d) if d >= today => State::Completed(d),
			_ => State::start(today),
		};
		Self {
			garage,
			state,
			persister,
		}
	}
}

impl State {
	fn start(date: NaiveDate) -> Self {
		info!("Starting lifecycle worker for {}", date);
		State::Running {
			date,
			pos: vec![],
			counter: 0,
			objects_expired: 0,
			mpu_aborted: 0,
			last_bucket: None,
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
				let n_objects = self.garage.object_table.data.store.len().ok();
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
				// Process a batch of 100 items before yielding to bg task scheduler
				for _ in 0..100 {
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
								.set_with(|x| x.last_completed = Some(date.to_string()))?;
							self.state = State::Completed(*date);
							return Ok(WorkerState::Idle);
						}
						Some((k, v)) => (v, k),
					};

					let object = self.garage.object_table.data.decode_entry(&object_bytes)?;
					let skip = process_object(
						&self.garage,
						*date,
						&object,
						objects_expired,
						mpu_aborted,
						last_bucket,
					)
					.await?;

					*counter += 1;
					if skip == Skip::SkipBucket {
						let bucket_id_len = object.bucket_id.as_slice().len();
						assert_eq!(
							next_pos.get(..bucket_id_len),
							Some(object.bucket_id.as_slice())
						);
						let last_bucket_pos = [&next_pos[..bucket_id_len], &[0xFFu8][..]].concat();
						*pos = std::cmp::max(next_pos, last_bucket_pos);
					} else {
						*pos = next_pos;
					}
				}

				Ok(WorkerState::Busy)
			}
		}
	}

	async fn wait_for_work(&mut self) -> WorkerState {
		match &self.state {
			State::Completed(d) => {
				let use_local_tz = self.garage.config.use_local_tz;
				let next_day = d.succ_opt().expect("no next day");
				let next_start = midnight_ts(next_day, use_local_tz);
				loop {
					let now = now_msec();
					if now < next_start {
						tokio::time::sleep_until(
							(Instant::now() + Duration::from_millis(next_start - now)).into(),
						)
						.await;
					} else {
						break;
					}
				}
				self.state = State::start(std::cmp::max(next_day, today(use_local_tz)));
			}
			State::Running { .. } => (),
		}
		WorkerState::Busy
	}
}

async fn process_object(
	garage: &Arc<Garage>,
	now_date: NaiveDate,
	object: &Object,
	objects_expired: &mut usize,
	mpu_aborted: &mut usize,
	last_bucket: &mut Option<Bucket>,
) -> Result<Skip, Error> {
	if !object
		.versions()
		.iter()
		.any(|x| x.is_data() || x.is_uploading(None))
	{
		return Ok(Skip::NextObject);
	}

	let bucket = match last_bucket.take() {
		Some(b) if b.id == object.bucket_id => b,
		_ => {
			match garage
				.bucket_table
				.get(&EmptyKey, &object.bucket_id)
				.await?
			{
				Some(b) => b,
				None => {
					warn!(
						"Lifecycle worker: object in non-existent bucket {:?}",
						object.bucket_id
					);
					return Ok(Skip::SkipBucket);
				}
			}
		}
	};

	let lifecycle_policy: &[LifecycleRule] = bucket
		.state
		.as_option()
		.and_then(|s| s.lifecycle_config.get().as_deref())
		.unwrap_or_default();

	if lifecycle_policy.iter().all(|x| !x.enabled) {
		return Ok(Skip::SkipBucket);
	}

	let db = garage.object_table.data.store.db();

	for rule in lifecycle_policy.iter() {
		if !rule.enabled {
			continue;
		}

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
					LifecycleExpiration::AtDate(exp_date) => {
						if let Ok(exp_date) = parse_lifecycle_date(exp_date) {
							now_date >= exp_date
						} else {
							warn!("Invalid expiration date stored in bucket {:?} lifecycle config: {}", bucket.id, exp_date);
							false
						}
					}
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
					info!(
						"Lifecycle: expiring 1 object in bucket {:?}",
						object.bucket_id
					);
					db.transaction(|tx| garage.object_table.queue_insert(tx, &deleted_object))?;
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
					if (now_date - version_date) >= chrono::Duration::days(*abort_mpu_days as i64)
						&& matches!(&v.state, ObjectVersionState::Uploading { .. })
					{
						Some(ObjectVersion {
							state: ObjectVersionState::Aborted,
							..*v
						})
					} else {
						None
					}
				})
				.collect::<Vec<_>>();
			if !aborted_versions.is_empty() {
				// Insert aborted mpu info
				let n_aborted = aborted_versions.len();
				info!(
					"Lifecycle: aborting {} incomplete upload(s) in bucket {:?}",
					n_aborted, object.bucket_id
				);
				let aborted_object =
					Object::new(object.bucket_id, object.key.clone(), aborted_versions);
				db.transaction(|tx| garage.object_table.queue_insert(tx, &aborted_object))?;
				*mpu_aborted += n_aborted;
			}
		}
	}

	*last_bucket = Some(bucket);
	Ok(Skip::NextObject)
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
	true
}

fn midnight_ts(date: NaiveDate, use_local_tz: bool) -> u64 {
	let midnight = date.and_hms_opt(0, 0, 0).expect("midnight does not exist");
	if use_local_tz {
		return midnight
			.and_local_timezone(Local)
			.single()
			.expect("bad local midnight")
			.timestamp_millis() as u64;
	}
	midnight.and_utc().timestamp_millis() as u64
}

fn next_date(ts: u64) -> NaiveDate {
	DateTime::<Utc>::from_timestamp_millis(ts as i64)
		.expect("bad timestamp")
		.date_naive()
		.succ_opt()
		.expect("no next day")
}

fn today(use_local_tz: bool) -> NaiveDate {
	if use_local_tz {
		return Local::now().naive_local().date();
	}
	Utc::now().naive_utc().date()
}
