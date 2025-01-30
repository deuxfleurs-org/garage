use std::fmt::Write;

use garage_model::helper::error::{Error, OkOrBadRequest};

use crate::cli::*;

use super::*;

impl AdminRpcHandler {
	pub(super) async fn handle_bucket_cmd(&self, cmd: &BucketOperation) -> Result<AdminRpc, Error> {
		match cmd {
			BucketOperation::CleanupIncompleteUploads(query) => {
				self.handle_bucket_cleanup_incomplete_uploads(query).await
			}
			_ => unreachable!(),
		}
	}

	async fn handle_bucket_cleanup_incomplete_uploads(
		&self,
		query: &CleanupIncompleteUploadsOpt,
	) -> Result<AdminRpc, Error> {
		let mut bucket_ids = vec![];
		for b in query.buckets.iter() {
			bucket_ids.push(
				self.garage
					.bucket_helper()
					.admin_get_existing_matching_bucket(b)
					.await?,
			);
		}

		let duration = parse_duration::parse::parse(&query.older_than)
			.ok_or_bad_request("Invalid duration passed for --older-than parameter")?;

		let mut ret = String::new();
		for bucket in bucket_ids {
			let count = self
				.garage
				.bucket_helper()
				.cleanup_incomplete_uploads(&bucket, duration)
				.await?;
			writeln!(
				&mut ret,
				"Bucket {:?}: {} incomplete uploads aborted",
				bucket, count
			)
			.unwrap();
		}

		Ok(AdminRpc::Ok(ret))
	}
}
