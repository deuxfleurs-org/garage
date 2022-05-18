use k2v_client::*;

use garage_util::formater::format_table;

use rusoto_core::credential::AwsCredentials;
use rusoto_core::Region;

use clap::{Parser, Subcommand};

/// K2V command line interface
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
	/// Name of the region to use
	#[clap(short, long, env = "AWS_REGION", default_value = "garage")]
	region: String,
	/// Url of the endpoint to connect to
	#[clap(short, long, env = "K2V_ENDPOINT")]
	endpoint: String,
	/// Access key ID
	#[clap(short, long, env = "AWS_ACCESS_KEY_ID")]
	key_id: String,
	/// Access key ID
	#[clap(short, long, env = "AWS_SECRET_ACCESS_KEY")]
	secret: String,
	/// Bucket name
	#[clap(short, long, env = "K2V_BUCKET")]
	bucket: String,
	#[clap(subcommand)]
	command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
	/// Insert a single value
	Insert {
		/// Partition key to insert to
		partition_key: String,
		/// Sort key to insert to
		sort_key: String,
		/// Causality of the insertion
		#[clap(short, long)]
		causality: Option<String>,
		/// Value to insert
		#[clap(flatten)]
		value: Value,
	},
	/// Read a single value
	Read {
		/// Partition key to read from
		partition_key: String,
		/// Sort key to read from
		sort_key: String,
		/// Output formating
		#[clap(flatten)]
		output_kind: ReadOutputKind,
	},
	/// Delete a single value
	Delete {
		/// Partition key to delete from
		partition_key: String,
		/// Sort key to delete from
		sort_key: String,
		/// Causality information
		#[clap(short, long)]
		causality: String,
	},
	/// List partition keys
	ReadIndex {
		/// Output formating
		#[clap(flatten)]
		output_kind: BatchOutputKind,
		/// Output only partition keys matching this filter
		#[clap(flatten)]
		filter: Filter,
	},
	/// Read a range of sort keys
	ReadRange {
		/// Partition key to read from
		partition_key: String,
		/// Output formating
		#[clap(flatten)]
		output_kind: BatchOutputKind,
		/// Output only sort keys matching this filter
		#[clap(flatten)]
		filter: Filter,
	},
	/// Delete a range of sort keys
	DeleteRange {
		/// Partition key to delete from
		partition_key: String,
		/// Output formating
		#[clap(flatten)]
		output_kind: BatchOutputKind,
		/// Delete only sort keys matching this filter
		#[clap(flatten)]
		filter: Filter,
	},
}

/// Where to read a value from
#[derive(Parser, Debug)]
#[clap(group = clap::ArgGroup::new("value").multiple(false).required(true))]
struct Value {
	/// Read value from a file. use - to read from stdin
	#[clap(short, long, group = "value")]
	file: Option<String>,
	/// Read a base64 value from commandline
	#[clap(short, long, group = "value")]
	b64: Option<String>,
	/// Read a raw (UTF-8) value from the commandline
	#[clap(short, long, group = "value")]
	text: Option<String>,
}

impl Value {
	async fn to_data(&self) -> Result<Vec<u8>, Error> {
		if let Some(ref text) = self.text {
			Ok(text.as_bytes().to_vec())
		} else if let Some(ref b64) = self.b64 {
			base64::decode(b64).map_err(|_| Error::Message("invalid base64 input".into()))
		} else if let Some(ref path) = self.file {
			use tokio::io::AsyncReadExt;
			if path == "-" {
				let mut file = tokio::io::stdin();
				let mut vec = Vec::new();
				file.read_to_end(&mut vec).await?;
				Ok(vec)
			} else {
				let mut file = tokio::fs::File::open(path).await?;
				let mut vec = Vec::new();
				file.read_to_end(&mut vec).await?;
				Ok(vec)
			}
		} else {
			unreachable!("Value must have one option set")
		}
	}
}

#[derive(Parser, Debug)]
#[clap(group = clap::ArgGroup::new("output-kind").multiple(false).required(false))]
struct ReadOutputKind {
	/// Base64 output. Conflicts are line separated, first line is causality token
	#[clap(short, long, group = "output-kind")]
	b64: bool,
	/// Raw output. Conflicts generate error, causality token is not returned
	#[clap(short, long, group = "output-kind")]
	raw: bool,
	/// Human formated output
	#[clap(short = 'H', long, group = "output-kind")]
	human: bool,
	/// JSON formated output
	#[clap(short, long, group = "output-kind")]
	json: bool,
}

impl ReadOutputKind {
	fn display_output(&self, val: CausalValue) -> ! {
		use std::io::Write;
		use std::process::exit;

		if self.json {
			let stdout = std::io::stdout();
			serde_json::to_writer_pretty(stdout, &val).unwrap();
			exit(0);
		}

		if self.raw {
			let mut val = val.value;
			if val.len() != 1 {
				eprintln!(
					"Raw mode can only read non-concurent values, found {} values, expected 1",
					val.len()
				);
				exit(1);
			}
			let val = val.pop().unwrap();
			match val {
				K2vValue::Value(v) => {
					std::io::stdout().write_all(&v).unwrap();
					exit(0);
				}
				K2vValue::Tombstone => {
					eprintln!("Expected value, found tombstone");
					exit(2);
				}
			}
		}

		let causality: String = val.causality.into();
		if self.b64 {
			println!("{}", causality);
			for val in val.value {
				match val {
					K2vValue::Value(v) => {
						println!("{}", base64::encode(&v))
					}
					K2vValue::Tombstone => {
						println!();
					}
				}
			}
			exit(0);
		}

		// human
		println!("causality: {}", causality);
		println!("values:");
		for val in val.value {
			match val {
				K2vValue::Value(v) => {
					if let Ok(string) = std::str::from_utf8(&v) {
						println!("  utf-8: {}", string);
					} else {
						println!("  base64: {}", base64::encode(&v));
					}
				}
				K2vValue::Tombstone => {
					println!("  tombstone");
				}
			}
		}
		exit(0);
	}
}

#[derive(Parser, Debug)]
#[clap(group = clap::ArgGroup::new("output-kind").multiple(false).required(false))]
struct BatchOutputKind {
	/// Human formated output
	#[clap(short = 'H', long, group = "output-kind")]
	human: bool,
	/// JSON formated output
	#[clap(short, long, group = "output-kind")]
	json: bool,
}

/// Filter for batch operations
#[derive(Parser, Debug)]
#[clap(group = clap::ArgGroup::new("filter").multiple(true).required(true))]
struct Filter {
	/// Match only keys starting with this prefix
	#[clap(short, long, group = "filter")]
	prefix: Option<String>,
	/// Match only keys lexicographically after this key (including this key itself)
	#[clap(short, long, group = "filter")]
	start: Option<String>,
	/// Match only keys lexicographically before this key (excluding this key)
	#[clap(short, long, group = "filter")]
	end: Option<String>,
	/// Only match the first X keys
	#[clap(short, long)]
	limit: Option<u64>,
	/// Return keys in reverse order
	#[clap(short, long)]
	reverse: bool,
	/// Return only keys where conflict happened
	#[clap(short, long)]
	conflicts_only: bool,
	/// Also include keys storing only tombstones
	#[clap(short, long)]
	tombstones: bool,
	/// Return any key
	#[clap(short, long, group = "filter")]
	all: bool,
}

impl Filter {
	fn k2v_filter(&self) -> k2v_client::Filter<'_> {
		k2v_client::Filter {
			start: self.start.as_deref(),
			end: self.end.as_deref(),
			prefix: self.prefix.as_deref(),
			limit: self.limit,
			reverse: self.reverse,
		}
	}
}

#[tokio::main]
async fn main() -> Result<(), Error> {
	let args = Args::parse();

	let region = Region::Custom {
		name: args.region,
		endpoint: args.endpoint,
	};

	let creds = AwsCredentials::new(args.key_id, args.secret, None, None);

	let client = K2vClient::new(region, args.bucket, creds, None)?;

	match args.command {
		Command::Insert {
			partition_key,
			sort_key,
			causality,
			value,
		} => {
			client
				.insert_item(
					&partition_key,
					&sort_key,
					value.to_data().await?,
					causality.map(Into::into),
				)
				.await?;
		}
		Command::Delete {
			partition_key,
			sort_key,
			causality,
		} => {
			client
				.delete_item(&partition_key, &sort_key, causality.into())
				.await?;
		}
		Command::Read {
			partition_key,
			sort_key,
			output_kind,
		} => {
			let res = client.read_item(&partition_key, &sort_key).await?;
			output_kind.display_output(res);
		}
		Command::ReadIndex {
			output_kind,
			filter,
		} => {
			if filter.conflicts_only || filter.tombstones {
				return Err(Error::Message(
					"conlicts-only and tombstones are invalid for read-index".into(),
				));
			}
			let res = client.read_index(filter.k2v_filter()).await?;
			if output_kind.json {
				let values = res
					.items
					.into_iter()
					.map(|(k, v)| {
						let mut value = serde_json::to_value(v).unwrap();
						value
							.as_object_mut()
							.unwrap()
							.insert("sort_key".to_owned(), k.into());
						value
					})
					.collect::<Vec<_>>();
				let json = serde_json::json!({
					"next_key": res.next_start,
					"values": values,
				});

				let stdout = std::io::stdout();
				serde_json::to_writer_pretty(stdout, &json).unwrap();
			} else {
				if let Some(next) = res.next_start {
					println!("next key: {}", next);
				}

				let mut to_print = Vec::new();
				to_print.push(format!("key:\tentries\tconflicts\tvalues\tbytes"));
				for (k, v) in res.items {
					to_print.push(format!(
						"{}\t{}\t{}\t{}\t{}",
						k, v.entries, v.conflicts, v.values, v.bytes
					));
				}
				format_table(to_print);
			}
		}
		Command::ReadRange {
			partition_key,
			output_kind,
			filter,
		} => {
			let op = BatchReadOp {
				partition_key: &partition_key,
				filter: filter.k2v_filter(),
				conflicts_only: filter.conflicts_only,
				tombstones: filter.tombstones,
				single_item: false,
			};
			let mut res = client.read_batch(&[op]).await?;
			let res = res.pop().unwrap();
			if output_kind.json {
				let values = res
					.items
					.into_iter()
					.map(|(k, v)| {
						let mut value = serde_json::to_value(v).unwrap();
						value
							.as_object_mut()
							.unwrap()
							.insert("sort_key".to_owned(), k.into());
						value
					})
					.collect::<Vec<_>>();
				let json = serde_json::json!({
					"next_key": res.next_start,
					"values": values,
				});

				let stdout = std::io::stdout();
				serde_json::to_writer_pretty(stdout, &json).unwrap();
			} else {
				if let Some(next) = res.next_start {
					println!("next key: {}", next);
				}
				for (key, values) in res.items {
					println!("key: {}", key);
					let causality: String = values.causality.into();
					println!("causality: {}", causality);
					for value in values.value {
						match value {
							K2vValue::Value(v) => {
								if let Ok(string) = std::str::from_utf8(&v) {
									println!("  value(utf-8): {}", string);
								} else {
									println!("  value(base64): {}", base64::encode(&v));
								}
							}
							K2vValue::Tombstone => {
								println!("  tombstone");
							}
						}
					}
				}
			}
		}
		Command::DeleteRange {
			partition_key,
			output_kind,
			filter,
		} => {
			let op = BatchDeleteOp {
				partition_key: &partition_key,
				prefix: filter.prefix.as_deref(),
				start: filter.start.as_deref(),
				end: filter.end.as_deref(),
				single_item: false,
			};
			if filter.reverse
				|| filter.conflicts_only
				|| filter.tombstones
				|| filter.limit.is_some()
			{
				return Err(Error::Message(
					"limit, conlicts-only, reverse and tombstones are invalid for delete-range"
						.into(),
				));
			}

			let res = client.delete_batch(&[op]).await?;

			if output_kind.json {
				println!("{}", res[0]);
			} else {
				println!("deleted {} keys", res[0]);
			}
		}
	}

	Ok(())
}
