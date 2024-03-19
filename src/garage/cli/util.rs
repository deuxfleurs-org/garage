use std::collections::HashMap;
use std::time::Duration;

use format_table::format_table;
use garage_util::background::*;
use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::time::*;

use garage_block::manager::BlockResyncErrorInfo;

use garage_model::bucket_table::*;
use garage_model::key_table::*;
use garage_model::s3::mpu_table::{self, MultipartUpload};
use garage_model::s3::object_table;
use garage_model::s3::version_table::*;

use crate::cli::structs::WorkerListOpt;

pub fn print_bucket_list(bl: Vec<Bucket>) {
	println!("List of buckets:");

	let mut table = vec![];
	for bucket in bl {
		let aliases = bucket
			.aliases()
			.iter()
			.filter(|(_, _, active)| *active)
			.map(|(name, _, _)| name.to_string())
			.collect::<Vec<_>>();
		let local_aliases_n = match &bucket
			.local_aliases()
			.iter()
			.filter(|(_, _, active)| *active)
			.collect::<Vec<_>>()[..]
		{
			[] => "".into(),
			[((k, n), _, _)] => format!("{}:{}", k, n),
			s => format!("[{} local aliases]", s.len()),
		};

		table.push(format!(
			"\t{}\t{}\t{}",
			aliases.join(","),
			local_aliases_n,
			hex::encode(bucket.id),
		));
	}
	format_table(table);
}

pub fn print_key_list(kl: Vec<(String, String)>) {
	println!("List of keys:");
	let mut table = vec![];
	for key in kl {
		table.push(format!("\t{}\t{}", key.0, key.1));
	}
	format_table(table);
}

pub fn print_key_info(key: &Key, relevant_buckets: &HashMap<Uuid, Bucket>) {
	let bucket_global_aliases = |b: &Uuid| {
		if let Some(bucket) = relevant_buckets.get(b) {
			if let Some(p) = bucket.state.as_option() {
				return p
					.aliases
					.items()
					.iter()
					.filter(|(_, _, active)| *active)
					.map(|(a, _, _)| a.clone())
					.collect::<Vec<_>>()
					.join(", ");
			}
		}

		"".to_string()
	};

	match &key.state {
		Deletable::Present(p) => {
			println!("Key name: {}", p.name.get());
			println!("Key ID: {}", key.key_id);
			println!("Secret key: {}", p.secret_key);
			println!("Can create buckets: {}", p.allow_create_bucket.get());
			println!("\nKey-specific bucket aliases:");
			let mut table = vec![];
			for (alias_name, _, alias) in p.local_aliases.items().iter() {
				if let Some(bucket_id) = alias {
					table.push(format!(
						"\t{}\t{}\t{}",
						alias_name,
						bucket_global_aliases(bucket_id),
						hex::encode(bucket_id)
					));
				}
			}
			format_table(table);

			println!("\nAuthorized buckets:");
			let mut table = vec![];
			for (bucket_id, perm) in p.authorized_buckets.items().iter() {
				if !perm.is_any() {
					continue;
				}
				let rflag = if perm.allow_read { "R" } else { " " };
				let wflag = if perm.allow_write { "W" } else { " " };
				let oflag = if perm.allow_owner { "O" } else { " " };
				let local_aliases = p
					.local_aliases
					.items()
					.iter()
					.filter(|(_, _, a)| *a == Some(*bucket_id))
					.map(|(a, _, _)| a.clone())
					.collect::<Vec<_>>()
					.join(", ");
				table.push(format!(
					"\t{}{}{}\t{}\t{}\t{:?}",
					rflag,
					wflag,
					oflag,
					bucket_global_aliases(bucket_id),
					local_aliases,
					bucket_id
				));
			}
			format_table(table);
		}
		Deletable::Deleted => {
			println!("Key {} is deleted.", key.key_id);
		}
	}
}

pub fn print_bucket_info(
	bucket: &Bucket,
	relevant_keys: &HashMap<String, Key>,
	counters: &HashMap<String, i64>,
	mpu_counters: &HashMap<String, i64>,
) {
	let key_name = |k| {
		relevant_keys
			.get(k)
			.map(|k| k.params().unwrap().name.get().as_str())
			.unwrap_or("<deleted>")
	};

	println!("Bucket: {}", hex::encode(bucket.id));
	match &bucket.state {
		Deletable::Deleted => println!("Bucket is deleted."),
		Deletable::Present(p) => {
			let size =
				bytesize::ByteSize::b(*counters.get(object_table::BYTES).unwrap_or(&0) as u64);
			println!(
				"\nSize: {} ({})",
				size.to_string_as(true),
				size.to_string_as(false)
			);
			println!(
				"Objects: {}",
				*counters.get(object_table::OBJECTS).unwrap_or(&0)
			);
			println!(
				"Unfinished uploads (multipart and non-multipart): {}",
				*counters.get(object_table::UNFINISHED_UPLOADS).unwrap_or(&0)
			);
			println!(
				"Unfinished multipart uploads: {}",
				*mpu_counters.get(mpu_table::UPLOADS).unwrap_or(&0)
			);
			let mpu_size =
				bytesize::ByteSize::b(*mpu_counters.get(mpu_table::BYTES).unwrap_or(&0) as u64);
			println!(
				"Size of unfinished multipart uploads: {} ({})",
				mpu_size.to_string_as(true),
				mpu_size.to_string_as(false),
			);

			println!("\nWebsite access: {}", p.website_config.get().is_some());

			let quotas = p.quotas.get();
			if quotas.max_size.is_some() || quotas.max_objects.is_some() {
				println!("\nQuotas:");
				if let Some(ms) = quotas.max_size {
					let ms = bytesize::ByteSize::b(ms);
					println!(
						" maximum size: {} ({})",
						ms.to_string_as(true),
						ms.to_string_as(false)
					);
				}
				if let Some(mo) = quotas.max_objects {
					println!(" maximum number of objects: {}", mo);
				}
			}

			println!("\nGlobal aliases:");
			for (alias, _, active) in p.aliases.items().iter() {
				if *active {
					println!("  {}", alias);
				}
			}

			println!("\nKey-specific aliases:");
			let mut table = vec![];
			for ((key_id, alias), _, active) in p.local_aliases.items().iter() {
				if *active {
					table.push(format!("\t{} ({})\t{}", key_id, key_name(key_id), alias));
				}
			}
			format_table(table);

			println!("\nAuthorized keys:");
			let mut table = vec![];
			for (k, perm) in p.authorized_keys.items().iter() {
				if !perm.is_any() {
					continue;
				}
				let rflag = if perm.allow_read { "R" } else { " " };
				let wflag = if perm.allow_write { "W" } else { " " };
				let oflag = if perm.allow_owner { "O" } else { " " };
				table.push(format!(
					"\t{}{}{}\t{}\t{}",
					rflag,
					wflag,
					oflag,
					k,
					key_name(k)
				));
			}
			format_table(table);
		}
	};
}

pub fn find_matching_node(
	cand: impl std::iter::Iterator<Item = Uuid>,
	pattern: &str,
) -> Result<Uuid, Error> {
	let mut candidates = vec![];
	for c in cand {
		if hex::encode(c).starts_with(pattern) && !candidates.contains(&c) {
			candidates.push(c);
		}
	}
	if candidates.len() != 1 {
		Err(Error::Message(format!(
			"{} nodes match '{}'",
			candidates.len(),
			pattern,
		)))
	} else {
		Ok(candidates[0])
	}
}

pub fn print_worker_list(wi: HashMap<usize, WorkerInfo>, wlo: WorkerListOpt) {
	let mut wi = wi.into_iter().collect::<Vec<_>>();
	wi.sort_by_key(|(tid, info)| {
		(
			match info.state {
				WorkerState::Busy | WorkerState::Throttled(_) => 0,
				WorkerState::Idle => 1,
				WorkerState::Done => 2,
			},
			*tid,
		)
	});

	let mut table = vec!["TID\tState\tName\tTranq\tDone\tQueue\tErrors\tConsec\tLast".to_string()];
	for (tid, info) in wi.iter() {
		if wlo.busy && !matches!(info.state, WorkerState::Busy | WorkerState::Throttled(_)) {
			continue;
		}
		if wlo.errors && info.errors == 0 {
			continue;
		}

		let tf = timeago::Formatter::new();
		let err_ago = info
			.last_error
			.as_ref()
			.map(|(_, t)| tf.convert(Duration::from_millis(now_msec() - t)))
			.unwrap_or_default();
		let (total_err, consec_err) = if info.errors > 0 {
			(info.errors.to_string(), info.consecutive_errors.to_string())
		} else {
			("-".into(), "-".into())
		};

		table.push(format!(
			"{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
			tid,
			info.state,
			info.name,
			info.status
				.tranquility
				.as_ref()
				.map(ToString::to_string)
				.unwrap_or_else(|| "-".into()),
			info.status.progress.as_deref().unwrap_or("-"),
			info.status
				.queue_length
				.as_ref()
				.map(ToString::to_string)
				.unwrap_or_else(|| "-".into()),
			total_err,
			consec_err,
			err_ago,
		));
	}
	format_table(table);
}

pub fn print_worker_info(tid: usize, info: WorkerInfo) {
	let mut table = vec![];
	table.push(format!("Task id:\t{}", tid));
	table.push(format!("Worker name:\t{}", info.name));
	match info.state {
		WorkerState::Throttled(t) => {
			table.push(format!(
				"Worker state:\tBusy (throttled, paused for {:.3}s)",
				t
			));
		}
		s => {
			table.push(format!("Worker state:\t{}", s));
		}
	};
	if let Some(tql) = info.status.tranquility {
		table.push(format!("Tranquility:\t{}", tql));
	}

	table.push("".into());
	table.push(format!("Total errors:\t{}", info.errors));
	table.push(format!("Consecutive errs:\t{}", info.consecutive_errors));
	if let Some((s, t)) = info.last_error {
		table.push(format!("Last error:\t{}", s));
		let tf = timeago::Formatter::new();
		table.push(format!(
			"Last error time:\t{}",
			tf.convert(Duration::from_millis(now_msec() - t))
		));
	}

	table.push("".into());
	if let Some(p) = info.status.progress {
		table.push(format!("Progress:\t{}", p));
	}
	if let Some(ql) = info.status.queue_length {
		table.push(format!("Queue length:\t{}", ql));
	}
	if let Some(pe) = info.status.persistent_errors {
		table.push(format!("Persistent errors:\t{}", pe));
	}

	for (i, s) in info.status.freeform.iter().enumerate() {
		if i == 0 {
			if table.last() != Some(&"".into()) {
				table.push("".into());
			}
			table.push(format!("Message:\t{}", s));
		} else {
			table.push(format!("\t{}", s));
		}
	}
	format_table(table);
}

pub fn print_worker_vars(wv: Vec<(Uuid, String, String)>) {
	let table = wv
		.into_iter()
		.map(|(n, k, v)| format!("{:?}\t{}\t{}", n, k, v))
		.collect::<Vec<_>>();
	format_table(table);
}

pub fn print_block_error_list(el: Vec<BlockResyncErrorInfo>) {
	let now = now_msec();
	let tf = timeago::Formatter::new();
	let mut tf2 = timeago::Formatter::new();
	tf2.ago("");

	let mut table = vec!["Hash\tRC\tErrors\tLast error\tNext try".into()];
	for e in el {
		let next_try = if e.next_try > now {
			tf2.convert(Duration::from_millis(e.next_try - now))
		} else {
			"asap".to_string()
		};
		table.push(format!(
			"{}\t{}\t{}\t{}\tin {}",
			hex::encode(e.hash.as_slice()),
			e.refcount,
			e.error_count,
			tf.convert(Duration::from_millis(now - e.last_try)),
			next_try
		));
	}
	format_table(table);
}

pub fn print_block_info(
	hash: Hash,
	refcount: u64,
	versions: Vec<Result<Version, Uuid>>,
	uploads: Vec<MultipartUpload>,
) {
	println!("Block hash: {}", hex::encode(hash.as_slice()));
	println!("Refcount: {}", refcount);
	println!();

	let mut table = vec!["Version\tBucket\tKey\tMPU\tDeleted".into()];
	let mut nondeleted_count = 0;
	for v in versions.iter() {
		match v {
			Ok(ver) => {
				match &ver.backlink {
					VersionBacklink::Object { bucket_id, key } => {
						table.push(format!(
							"{:?}\t{:?}\t{}\t\t{:?}",
							ver.uuid,
							bucket_id,
							key,
							ver.deleted.get()
						));
					}
					VersionBacklink::MultipartUpload { upload_id } => {
						let upload = uploads.iter().find(|x| x.upload_id == *upload_id);
						table.push(format!(
							"{:?}\t{:?}\t{}\t{:?}\t{:?}",
							ver.uuid,
							upload.map(|u| u.bucket_id).unwrap_or_default(),
							upload.map(|u| u.key.as_str()).unwrap_or_default(),
							upload_id,
							ver.deleted.get()
						));
					}
				}
				if !ver.deleted.get() {
					nondeleted_count += 1;
				}
			}
			Err(vh) => {
				table.push(format!("{:?}\t\t\t\tyes", vh));
			}
		}
	}
	format_table(table);

	if refcount != nondeleted_count {
		println!();
		println!(
			"Warning: refcount does not match number of non-deleted versions, you should try `garage repair block-rc`."
		);
	}
}
