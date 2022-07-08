use std::collections::HashMap;
use std::time::Duration;

use garage_util::background::*;
use garage_util::crdt::*;
use garage_util::data::Uuid;
use garage_util::error::*;
use garage_util::formater::format_table;
use garage_util::time::*;

use garage_model::bucket_table::*;
use garage_model::key_table::*;
use garage_model::s3::object_table::{BYTES, OBJECTS, UNFINISHED_UPLOADS};

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
				bytesize::ByteSize::b(counters.get(BYTES).cloned().unwrap_or_default() as u64);
			println!(
				"\nSize: {} ({})",
				size.to_string_as(true),
				size.to_string_as(false)
			);
			println!(
				"Objects: {}",
				counters.get(OBJECTS).cloned().unwrap_or_default()
			);
			println!(
				"Unfinished multipart uploads: {}",
				counters
					.get(UNFINISHED_UPLOADS)
					.cloned()
					.unwrap_or_default()
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
		if hex::encode(&c).starts_with(&pattern) && !candidates.contains(&c) {
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

pub fn print_worker_info(wi: HashMap<usize, WorkerInfo>, wlo: WorkerListOpt) {
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

	let mut table = vec![];
	for (tid, info) in wi.iter() {
		if wlo.busy && !matches!(info.state, WorkerState::Busy | WorkerState::Throttled(_)) {
			continue;
		}
		if wlo.errors && info.errors == 0 {
			continue;
		}

		table.push(format!("{}\t{}\t{}", tid, info.state, info.name));
		if let Some(i) = &info.info {
			table.push(format!("\t\t  {}", i));
		}
		let tf = timeago::Formatter::new();
		let (err_ago, err_msg) = info
			.last_error
			.as_ref()
			.map(|(m, t)| {
				(
					tf.convert(Duration::from_millis(now_msec() - t)),
					m.as_str(),
				)
			})
			.unwrap_or(("(?) ago".into(), "(?)"));
		if info.consecutive_errors > 0 {
			table.push(format!(
				"\t\t  {} consecutive errors ({} total), last {}",
				info.consecutive_errors, info.errors, err_ago,
			));
			table.push(format!("\t\t  {}", err_msg));
		} else if info.errors > 0 {
			table.push(format!("\t\t  ({} errors, last {})", info.errors, err_ago,));
			if wlo.errors {
				table.push(format!("\t\t  {}", err_msg));
			}
		}
	}
	format_table(table);
}
