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
		let sys_status = garage.system.local_status();
		let hostname = sys_status.hostname.unwrap_or_default().to_string();

		let layout = garage.system.cluster_layout();
		let current_layout = layout.inner().current();

		Ok(LocalGetNodeInfoResponse {
			node_id: hex::encode(garage.system.id),
			hostname: Some(hostname),
			garage_version: garage_util::version::garage_version().to_string(),
			garage_features: garage_util::version::garage_features()
				.map(|features| features.iter().map(ToString::to_string).collect()),
			rust_version: garage_util::version::rust_version().to_string(),
			db_engine: garage.db.engine(),
			is_up: Some(true),
			addr: garage
				.system
				.get_known_nodes()
				.iter()
				.find(|x| x.id == garage.system.id)
				.and_then(|x| x.addr),
			draining: Some(
				current_layout.node_role(&garage.system.id).is_none()
					&& layout
						.inner()
						.versions
						.iter()
						.filter(|x| x.version != current_layout.version)
						.any(|x| x.node_role(&garage.system.id).is_some()),
			),
			role: current_layout
				.node_role(&garage.system.id)
				.map(|v| NodeAssignedRole {
					zone: v.zone.clone(),
					capacity: v.capacity,
					tags: v.tags.clone(),
				}),
			data_partition: sys_status
				.data_disk_avail
				.map(|(avail, total)| FreeSpaceResp {
					available: avail,
					total,
				}),
			metadata_partition: sys_status
				.meta_disk_avail
				.map(|(avail, total)| FreeSpaceResp {
					available: avail,
					total,
				}),
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
		let sys_status = garage.system.local_status();

		let hostname = sys_status.hostname.unwrap_or_default().to_string();
		let garage_version = garage_util::version::garage_version().to_string();
		let garage_features = garage_util::version::garage_features()
			.unwrap()
			.iter()
			.map(ToString::to_string)
			.collect::<Vec<String>>();
		let rustc_version = garage_util::version::rust_version().to_string();
		let db_engine_descr = garage.db.engine();

		let mut ret = format_table_to_string(vec![
			format!("Node ID:\t{:?}", garage.system.id),
			format!("Hostname:\t{}", hostname),
			format!("Garage version:\t{}", garage_version),
			format!("Garage features:\t{}", garage_features.join(", ")),
			format!("Rust compiler version:\t{}", rustc_version),
			format!("Database engine:\t{}", db_engine_descr),
		]);

		let mut table_stats = vec![
			gather_table_stats(&garage.admin_token_table)?,
			gather_table_stats(&garage.bucket_table)?,
			gather_table_stats(&garage.bucket_alias_table)?,
			gather_table_stats(&garage.key_table)?,
			gather_table_stats(&garage.object_table)?,
			gather_table_stats(&garage.object_counter_table.table)?,
			gather_table_stats(&garage.mpu_table)?,
			gather_table_stats(&garage.mpu_counter_table.table)?,
			gather_table_stats(&garage.version_table)?,
			gather_table_stats(&garage.block_ref_table)?,
		];

		#[cfg(feature = "k2v")]
		{
			table_stats.push(gather_table_stats(&garage.k2v.item_table)?);
			table_stats.push(gather_table_stats(&garage.k2v.counter_table.table)?);
		}

		// Gather table statistics
		let mut table = vec!["  Table\tItems\tMklItems\tMklTodo\tInsQueue\tGcTodo".into()];
		table.extend(table_stats.iter().map(|ts| {
			format!(
				"  {}\t{}\t{}\t{}\t{}\t{}",
				ts.table_name,
				ts.items,
				ts.merkle_items,
				ts.merkle_queue_len,
				ts.insert_queue_len,
				ts.gc_queue_len,
			)
		}));

		write!(
			&mut ret,
			"\nTable stats:\n{}",
			format_table_to_string(table)
		)
		.unwrap();

		let block_manager_stats = NodeBlockManagerStats {
			rc_entries: garage.block_manager.rc_approximate_len()? as u64,
			resync_queue_len: garage.block_manager.resync.queue_approximate_len()? as u64,
			resync_errors: garage.block_manager.resync.errored() as u64,
		};

		// Gather block manager statistics
		writeln!(&mut ret, "\nBlock manager stats:").unwrap();

		ret += &format_table_to_string(vec![
			format!(
				"  number of RC entries:\t{} (~= number of blocks)",
				block_manager_stats.rc_entries
			),
			format!(
				"  resync queue length:\t{}",
				block_manager_stats.resync_queue_len,
			),
			format!(
				"  blocks with resync errors:\t{}",
				block_manager_stats.resync_errors
			),
		]);

		Ok(LocalGetNodeStatisticsResponse {
			freeform: ret,
			table_stats: Some(table_stats),
			block_manager_stats: Some(block_manager_stats),
		})
	}
}

fn gather_table_stats<F, R>(t: &Arc<Table<F, R>>) -> Result<NodeTableStats, Error>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	let data_len = t.data.store.approximate_len().map_err(GarageError::from)?;
	let mkl_len = t.merkle_updater.merkle_tree_approximate_len()?;

	Ok(NodeTableStats {
		table_name: F::TABLE_NAME.to_string(),
		items: data_len as u64,
		merkle_items: mkl_len as u64,
		merkle_queue_len: t.merkle_updater.todo_approximate_len()? as u64,
		insert_queue_len: t.data.insert_queue_approximate_len()? as u64,
		gc_queue_len: t.data.gc_todo_approximate_len()? as u64,
	})
}
