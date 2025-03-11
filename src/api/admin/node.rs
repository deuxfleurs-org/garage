use std::fmt::Write;
use std::sync::Arc;

use format_table::format_table_to_string;

use garage_util::error::Error as GarageError;

use garage_table::replication::*;
use garage_table::*;

use garage_model::garage::Garage;

use crate::api::*;
use crate::error::Error;
use crate::{Admin, RequestHandler};

impl RequestHandler for LocalGetNodeInfoRequest {
	type Response = LocalGetNodeInfoResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<LocalGetNodeInfoResponse, Error> {
		Ok(LocalGetNodeInfoResponse {
			node_id: hex::encode(garage.system.id),
			garage_version: garage_util::version::garage_version().to_string(),
			garage_features: garage_util::version::garage_features()
				.map(|features| features.iter().map(ToString::to_string).collect()),
			rust_version: garage_util::version::rust_version().to_string(),
			db_engine: garage.db.engine(),
		})
	}
}

impl RequestHandler for LocalCreateMetadataSnapshotRequest {
	type Response = LocalCreateMetadataSnapshotResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<LocalCreateMetadataSnapshotResponse, Error> {
		garage_model::snapshot::async_snapshot_metadata(garage).await?;
		Ok(LocalCreateMetadataSnapshotResponse)
	}
}

impl RequestHandler for LocalGetNodeStatisticsRequest {
	type Response = LocalGetNodeStatisticsResponse;

	// FIXME: return this as a JSON struct instead of text
	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<LocalGetNodeStatisticsResponse, Error> {
		let mut ret = String::new();
		writeln!(
			&mut ret,
			"Garage version: {} [features: {}]\nRust compiler version: {}",
			garage_util::version::garage_version(),
			garage_util::version::garage_features()
				.map(|list| list.join(", "))
				.unwrap_or_else(|| "(unknown)".into()),
			garage_util::version::rust_version(),
		)
		.unwrap();

		writeln!(&mut ret, "\nDatabase engine: {}", garage.db.engine()).unwrap();

		// Gather table statistics
		let mut table = vec!["  Table\tItems\tMklItems\tMklTodo\tGcTodo".into()];
		table.push(gather_table_stats(&garage.bucket_table)?);
		table.push(gather_table_stats(&garage.key_table)?);
		table.push(gather_table_stats(&garage.object_table)?);
		table.push(gather_table_stats(&garage.version_table)?);
		table.push(gather_table_stats(&garage.block_ref_table)?);
		write!(
			&mut ret,
			"\nTable stats:\n{}",
			format_table_to_string(table)
		)
		.unwrap();

		// Gather block manager statistics
		writeln!(&mut ret, "\nBlock manager stats:").unwrap();
		let rc_len = garage.block_manager.rc_len()?.to_string();

		writeln!(
			&mut ret,
			"  number of RC entries (~= number of blocks): {}",
			rc_len
		)
		.unwrap();
		writeln!(
			&mut ret,
			"  resync queue length: {}",
			garage.block_manager.resync.queue_len()?
		)
		.unwrap();
		writeln!(
			&mut ret,
			"  blocks with resync errors: {}",
			garage.block_manager.resync.errors_len()?
		)
		.unwrap();

		Ok(LocalGetNodeStatisticsResponse { freeform: ret })
	}
}

fn gather_table_stats<F, R>(t: &Arc<Table<F, R>>) -> Result<String, Error>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	let data_len = t.data.store.len().map_err(GarageError::from)?.to_string();
	let mkl_len = t.merkle_updater.merkle_tree_len()?.to_string();

	Ok(format!(
		"  {}\t{}\t{}\t{}\t{}",
		F::TABLE_NAME,
		data_len,
		mkl_len,
		t.merkle_updater.todo_len()?,
		t.data.gc_todo_len()?
	))
}
