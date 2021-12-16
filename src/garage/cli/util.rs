use std::collections::HashMap;

use garage_util::crdt::*;
use garage_util::data::Uuid;
use garage_util::error::*;

use garage_model::bucket_table::*;
use garage_model::key_table::*;

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

	println!("Key name: {}", key.name.get());
	println!("Key ID: {}", key.key_id);
	println!("Secret key: {}", key.secret_key);
	match &key.state {
		Deletable::Present(p) => {
			println!("Can create buckets: {}", p.allow_create_bucket.get());
			println!("\nKey-specific bucket aliases:");
			let mut table = vec![];
			for (alias_name, _, alias) in p.local_aliases.items().iter() {
				if let Some(bucket_id) = alias.as_option() {
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
				let rflag = if perm.allow_read { "R" } else { " " };
				let wflag = if perm.allow_write { "W" } else { " " };
				let oflag = if perm.allow_owner { "O" } else { " " };
				let local_aliases = p
					.local_aliases
					.items()
					.iter()
					.filter(|(_, _, a)| a.as_option() == Some(bucket_id))
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
			println!("\nKey is deleted.");
		}
	}
}

pub fn print_bucket_info(bucket: &Bucket, relevant_keys: &HashMap<String, Key>) {
	println!("Bucket: {}", hex::encode(bucket.id));
	match &bucket.state {
		Deletable::Deleted => println!("Bucket is deleted."),
		Deletable::Present(p) => {
			println!("Website access: {}", p.website_access.get());

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
					let key_name = relevant_keys
						.get(key_id)
						.map(|k| k.name.get().as_str())
						.unwrap_or("");
					table.push(format!("\t{}\t{} ({})", alias, key_id, key_name));
				}
			}
			format_table(table);

			println!("\nAuthorized keys:");
			let mut table = vec![];
			for (k, perm) in p.authorized_keys.items().iter() {
				let rflag = if perm.allow_read { "R" } else { " " };
				let wflag = if perm.allow_write { "W" } else { " " };
				let oflag = if perm.allow_owner { "O" } else { " " };
				let key_name = relevant_keys
					.get(k)
					.map(|k| k.name.get().as_str())
					.unwrap_or("");
				table.push(format!(
					"\t{}{}{}\t{} ({})",
					rflag, wflag, oflag, k, key_name
				));
			}
			format_table(table);
		}
	};
}

pub fn format_table(data: Vec<String>) {
	let data = data
		.iter()
		.map(|s| s.split('\t').collect::<Vec<_>>())
		.collect::<Vec<_>>();

	let columns = data.iter().map(|row| row.len()).fold(0, std::cmp::max);
	let mut column_size = vec![0; columns];

	let mut out = String::new();

	for row in data.iter() {
		for (i, col) in row.iter().enumerate() {
			column_size[i] = std::cmp::max(column_size[i], col.chars().count());
		}
	}

	for row in data.iter() {
		for (col, col_len) in row[..row.len() - 1].iter().zip(column_size.iter()) {
			out.push_str(col);
			(0..col_len - col.chars().count() + 2).for_each(|_| out.push(' '));
		}
		out.push_str(row[row.len() - 1]);
		out.push('\n');
	}

	print!("{}", out);
}

pub fn find_matching_node(
	cand: impl std::iter::Iterator<Item = Uuid>,
	pattern: &str,
) -> Result<Uuid, Error> {
	let mut candidates = vec![];
	for c in cand {
		if hex::encode(&c).starts_with(&pattern) {
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
