use std::collections::HashMap;
use std::time::Duration;

use format_table::format_table;
use garage_util::background::*;
use garage_util::data::*;
use garage_util::time::*;

use garage_block::manager::BlockResyncErrorInfo;

use garage_model::s3::mpu_table::MultipartUpload;
use garage_model::s3::version_table::*;

use crate::cli::structs::WorkerListOpt;

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
