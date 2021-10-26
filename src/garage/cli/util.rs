use garage_util::data::Uuid;
use garage_util::error::*;

use garage_model::bucket_table::*;
use garage_model::key_table::*;

pub fn print_key_info(key: &Key) {
	println!("Key name: {}", key.name.get());
	println!("Key ID: {}", key.key_id);
	println!("Secret key: {}", key.secret_key);
	if key.deleted.get() {
		println!("Key is deleted.");
	} else {
		println!("Authorized buckets:");
		for (b, _, perm) in key.authorized_buckets.items().iter() {
			println!("- {} R:{} W:{}", b, perm.allow_read, perm.allow_write);
		}
	}
}

pub fn print_bucket_info(bucket: &Bucket) {
	println!("Bucket name: {}", bucket.name);
	match bucket.state.get() {
		BucketState::Deleted => println!("Bucket is deleted."),
		BucketState::Present(p) => {
			println!("Authorized keys:");
			for (k, _, perm) in p.authorized_keys.items().iter() {
				println!("- {} R:{} W:{}", k, perm.allow_read, perm.allow_write);
			}
			println!("Website access: {}", p.website.get());
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
