//use bytesize::ByteSize;
use format_table::format_table;

use garage_util::error::*;

use garage_api_admin::api::*;

use crate::cli::structs::*;
use crate::cli_v2::*;

impl Cli {
	pub async fn cmd_block(&self, cmd: BlockOperation) -> Result<(), Error> {
		match cmd {
			BlockOperation::ListErrors => self.cmd_list_block_errors().await,
			BlockOperation::Info { hash } => self.cmd_get_block_info(hash).await,
			BlockOperation::RetryNow { all, blocks } => self.cmd_block_retry_now(all, blocks).await,
			BlockOperation::Purge { yes, blocks } => self.cmd_block_purge(yes, blocks).await,
		}
	}

	pub async fn cmd_list_block_errors(&self) -> Result<(), Error> {
		let errors = self.local_api_request(LocalListBlockErrorsRequest).await?.0;

		let tf = timeago::Formatter::new();
		let mut tf2 = timeago::Formatter::new();
		tf2.ago("");

		let mut table = vec!["Hash\tRC\tErrors\tLast error\tNext try".into()];
		for e in errors {
			let next_try = if e.next_try_in_secs > 0 {
				tf2.convert(Duration::from_secs(e.next_try_in_secs))
			} else {
				"asap".to_string()
			};
			table.push(format!(
				"{}\t{}\t{}\t{}\tin {}",
				e.block_hash,
				e.refcount,
				e.error_count,
				tf.convert(Duration::from_secs(e.last_try_secs_ago)),
				next_try
			));
		}
		format_table(table);

		Ok(())
	}

	pub async fn cmd_get_block_info(&self, hash: String) -> Result<(), Error> {
		let info = self
			.local_api_request(LocalGetBlockInfoRequest { block_hash: hash })
			.await?;

		println!("Block hash: {}", info.block_hash);
		println!("Refcount: {}", info.refcount);
		println!();

		let mut table = vec!["Version\tBucket\tKey\tMPU\tDeleted".into()];
		let mut nondeleted_count = 0;
		for ver in info.versions.iter() {
			match &ver.backlink {
				Some(BlockVersionBacklink::Object { bucket_id, key }) => {
					table.push(format!(
						"{:.16}\t{:.16}\t{}\t\t{:?}",
						ver.version_id, bucket_id, key, ver.deleted
					));
				}
				Some(BlockVersionBacklink::Upload {
					upload_id,
					upload_deleted: _,
					upload_garbage_collected: _,
					bucket_id,
					key,
				}) => {
					table.push(format!(
						"{:.16}\t{:.16}\t{}\t{:.16}\t{:.16}",
						ver.version_id,
						bucket_id.as_deref().unwrap_or(""),
						key.as_deref().unwrap_or(""),
						upload_id,
						ver.deleted
					));
				}
				None => {
					table.push(format!("{:.16}\t\t\tyes", ver.version_id));
				}
			}
			if !ver.deleted {
				nondeleted_count += 1;
			}
		}
		format_table(table);

		if info.refcount != nondeleted_count {
			println!();
			println!(
                "Warning: refcount does not match number of non-deleted versions, you should try `garage repair block-rc`."
            );
		}

		Ok(())
	}

	pub async fn cmd_block_retry_now(&self, all: bool, blocks: Vec<String>) -> Result<(), Error> {
		let req = match (all, blocks.len()) {
			(true, 0) => LocalRetryBlockResyncRequest::All { all: true },
			(false, n) if n > 0 => LocalRetryBlockResyncRequest::Blocks {
				block_hashes: blocks,
			},
			_ => {
				return Err(Error::Message(
					"Please specify block hashes or --all (not both)".into(),
				))
			}
		};

		let res = self.local_api_request(req).await?;

		println!(
			"{} blocks returned in queue for a retry now (check logs to see results)",
			res.count
		);

		Ok(())
	}

	pub async fn cmd_block_purge(&self, yes: bool, blocks: Vec<String>) -> Result<(), Error> {
		if !yes {
			return Err(Error::Message(
				"Pass the --yes flag to confirm block purge operation.".into(),
			));
		}

		let res = self
			.local_api_request(LocalPurgeBlocksRequest(blocks))
			.await?;

		println!(
			"Purged {} blocks: deleted {} versions, {} objects, {} multipart uploads",
			res.blocks_purged, res.versions_deleted, res.objects_deleted, res.uploads_deleted,
		);

		Ok(())
	}
}
