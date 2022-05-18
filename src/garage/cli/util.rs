use std::collections::HashMap;

use garage_util::crdt::*;
use garage_util::data::Uuid;
use garage_util::error::*;
use garage_util::formater::format_table;

use garage_model::bucket_table::*;
use garage_model::key_table::*;

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
			hex::encode(bucket.id)
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

pub fn print_bucket_info(bucket: &Bucket, relevant_keys: &HashMap<String, Key>) {
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
			println!("Website access: {}", p.website_config.get().is_some());

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
