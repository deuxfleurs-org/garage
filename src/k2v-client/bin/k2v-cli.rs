use std::collections::BTreeMap;
use std::process::exit;
use std::time::Duration;

use base64::prelude::*;

use k2v_client::*;

use format_table::format_table;

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
		/// Output formatting
		#[clap(flatten)]
		output_kind: ReadOutputKind,
	},
	/// Watch changes on a single value
	PollItem {
		/// Partition key of item to watch
		partition_key: String,
		/// Sort key of item to watch
		sort_key: String,
		/// Causality information
		#[clap(short, long)]
		causality: String,
		/// Timeout, in seconds
		#[clap(short = 'T', long)]
		timeout: Option<u64>,
		/// Output formatting
		#[clap(flatten)]
		output_kind: ReadOutputKind,
	},
	/// Watch changes on a range of values
	PollRange {
		/// Partition key to poll from
		partition_key: String,
		/// Output only sort keys matching this filter
		#[clap(flatten)]
		filter: Filter,
		/// Marker of data that had previously been seen by a PollRange
		#[clap(short = 'S', long)]
		seen_marker: Option<String>,
		/// Timeout, in seconds
		#[clap(short = 'T', long)]
		timeout: Option<u64>,
		/// Output formatting
		#[clap(flatten)]
		output_kind: BatchOutputKind,
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
		/// Output formatting
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
		/// Output formatting
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
		/// Output formatting
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
			BASE64_STANDARD
				.decode(b64)
				.map_err(|_| Error::Message("invalid base64 input".into()))
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
	/// Human formatted output
	#[clap(short = 'H', long, group = "output-kind")]
	human: bool,
	/// JSON formatted output
	#[clap(short, long, group = "output-kind")]
	json: bool,
}

impl ReadOutputKind {
	fn display_output(&self, val: CausalValue) -> ! {
		use std::io::Write;

		if self.json {
			let stdout = std::io::stdout();
			serde_json::to_writer_pretty(stdout, &val).unwrap();
			exit(0);
		}

		if self.raw {
			let mut val = val.value;
			if val.len() != 1 {
				eprintln!(
					"Raw mode can only read non-concurrent values, found {} values, expected 1",
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
						println!("{}", BASE64_STANDARD.encode(&v))
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
						println!("  base64: {}", BASE64_STANDARD.encode(&v));
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
	/// Human formatted output
	#[clap(short = 'H', long, group = "output-kind")]
	human: bool,
	/// JSON formatted output
	#[clap(short, long, group = "output-kind")]
	json: bool,
}

impl BatchOutputKind {
	fn display_human_output(&self, values: BTreeMap<String, CausalValue>) -> ! {
		for (key, values) in values {
			println!("sort_key: {}", key);
			let causality: String = values.causality.into();
			println!("causality: {}", causality);
			for value in values.value {
				match value {
					K2vValue::Value(v) => {
						if let Ok(string) = std::str::from_utf8(&v) {
							println!("  value(utf-8): {}", string);
						} else {
							println!("  value(base64): {}", BASE64_STANDARD.encode(&v));
						}
					}
					K2vValue::Tombstone => {
						println!("  tombstone");
					}
				}
			}
		}
		exit(0);
	}

	fn values_json(&self, values: BTreeMap<String, CausalValue>) -> Vec<serde_json::Value> {
		values
			.into_iter()
			.map(|(k, v)| {
				let mut value = serde_json::to_value(v).unwrap();
				value
					.as_object_mut()
					.unwrap()
					.insert("sort_key".to_owned(), k.into());
				value
			})
			.collect::<Vec<_>>()
	}

	fn display_poll_range_output(&self, poll_range: PollRangeResult) -> ! {
		if self.json {
			let json = serde_json::json!({
				"values": self.values_json(poll_range.items),
				"seen_marker": poll_range.seen_marker,
			});

			let stdout = std::io::stdout();
			serde_json::to_writer_pretty(stdout, &json).unwrap();
			exit(0)
		} else {
			println!("seen marker: {}", poll_range.seen_marker);
			self.display_human_output(poll_range.items)
		}
	}

	fn display_read_range_output(&self, res: PaginatedRange<CausalValue>) -> ! {
		if self.json {
			let json = serde_json::json!({
				"next_key": res.next_start,
				"values": self.values_json(res.items),
			});

			let stdout = std::io::stdout();
			serde_json::to_writer_pretty(stdout, &json).unwrap();
			exit(0)
		} else {
			if let Some(next) = res.next_start {
				println!("next key: {}", next);
			}
			self.display_human_output(res.items)
		}
	}
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
	if std::env::var("RUST_LOG").is_err() {
		std::env::set_var("RUST_LOG", "warn")
	}

	tracing_subscriber::fmt()
		.with_writer(std::io::stderr)
		.with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
		.init();

	let args = Args::parse();

	let config = K2vClientConfig {
		endpoint: args.endpoint,
		region: args.region,
		aws_access_key_id: args.key_id,
		aws_secret_access_key: args.secret,
		bucket: args.bucket,
		user_agent: None,
	};

	let client = K2vClient::new(config)?;

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
		Command::PollItem {
			partition_key,
			sort_key,
			causality,
			timeout,
			output_kind,
		} => {
			let timeout = timeout.map(Duration::from_secs);
			let res_opt = client
				.poll_item(&partition_key, &sort_key, causality.into(), timeout)
				.await?;
			if let Some(res) = res_opt {
				output_kind.display_output(res);
			} else {
				if output_kind.json {
					println!("null");
				} else {
					println!("Delay expired and value didn't change.");
				}
			}
		}
		Command::PollRange {
			partition_key,
			filter,
			seen_marker,
			timeout,
			output_kind,
		} => {
			if filter.conflicts_only
				|| filter.tombstones
				|| filter.reverse
				|| filter.limit.is_some()
			{
				return Err(Error::Message(
					"limit, reverse, conlicts-only, tombstones are invalid for poll-range".into(),
				));
			}

			let timeout = timeout.map(Duration::from_secs);
			let res = client
				.poll_range(
					&partition_key,
					Some(PollRangeFilter {
						start: filter.start.as_deref(),
						end: filter.end.as_deref(),
						prefix: filter.prefix.as_deref(),
					}),
					seen_marker.as_deref(),
					timeout,
				)
				.await?;
			match res {
				Some(poll_range_output) => {
					output_kind.display_poll_range_output(poll_range_output);
				}
				None => {
					if output_kind.json {
						println!("null");
					} else {
						println!("Delay expired and value didn't change.");
					}
				}
			}
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
							.insert("partition_key".to_owned(), k.into());
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
				to_print.push(format!("partition_key\tentries\tconflicts\tvalues\tbytes"));
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
			output_kind.display_read_range_output(res);
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
