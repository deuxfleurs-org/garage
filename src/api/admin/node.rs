use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use async_trait::async_trait;

use format_table::format_table_to_string;

use garage_util::data::*;
use garage_util::error::Error as GarageError;

use garage_table::replication::*;
use garage_table::*;

use garage_rpc::layout::PARTITION_BITS;

use garage_model::garage::Garage;

use crate::api::*;
use crate::error::Error;
use crate::{Admin, RequestHandler};

#[async_trait]
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

#[async_trait]
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

#[async_trait]
impl RequestHandler for GetClusterStatisticsRequest {
	type Response = GetClusterStatisticsResponse;

	// FIXME: return this as a JSON struct instead of text
	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<GetClusterStatisticsResponse, Error> {
		let mut ret = String::new();

		// Gather storage node and free space statistics for current nodes
		let layout = &garage.system.cluster_layout();
		let mut node_partition_count = HashMap::<Uuid, u64>::new();
		for short_id in layout.current().ring_assignment_data.iter() {
			let id = layout.current().node_id_vec[*short_id as usize];
			*node_partition_count.entry(id).or_default() += 1;
		}
		let node_info = garage
			.system
			.get_known_nodes()
			.into_iter()
			.map(|n| (n.id, n))
			.collect::<HashMap<_, _>>();

		let mut table = vec!["  ID\tHostname\tZone\tCapacity\tPart.\tDataAvail\tMetaAvail".into()];
		for (id, parts) in node_partition_count.iter() {
			let info = node_info.get(id);
			let status = info.map(|x| &x.status);
			let role = layout.current().roles.get(id).and_then(|x| x.0.as_ref());
			let hostname = status.and_then(|x| x.hostname.as_deref()).unwrap_or("?");
			let zone = role.map(|x| x.zone.as_str()).unwrap_or("?");
			let capacity = role
				.map(|x| x.capacity_string())
				.unwrap_or_else(|| "?".into());
			let avail_str = |x| match x {
				Some((avail, total)) => {
					let pct = (avail as f64) / (total as f64) * 100.;
					let avail = bytesize::ByteSize::b(avail);
					let total = bytesize::ByteSize::b(total);
					format!("{}/{} ({:.1}%)", avail, total, pct)
				}
				None => "?".into(),
			};
			let data_avail = avail_str(status.and_then(|x| x.data_disk_avail));
			let meta_avail = avail_str(status.and_then(|x| x.meta_disk_avail));
			table.push(format!(
				"  {:?}\t{}\t{}\t{}\t{}\t{}\t{}",
				id, hostname, zone, capacity, parts, data_avail, meta_avail
			));
		}
		write!(
			&mut ret,
			"Storage nodes:\n{}",
			format_table_to_string(table)
		)
		.unwrap();

		let meta_part_avail = node_partition_count
			.iter()
			.filter_map(|(id, parts)| {
				node_info
					.get(id)
					.and_then(|x| x.status.meta_disk_avail)
					.map(|c| c.0 / *parts)
			})
			.collect::<Vec<_>>();
		let data_part_avail = node_partition_count
			.iter()
			.filter_map(|(id, parts)| {
				node_info
					.get(id)
					.and_then(|x| x.status.data_disk_avail)
					.map(|c| c.0 / *parts)
			})
			.collect::<Vec<_>>();
		if !meta_part_avail.is_empty() && !data_part_avail.is_empty() {
			let meta_avail =
				bytesize::ByteSize(meta_part_avail.iter().min().unwrap() * (1 << PARTITION_BITS));
			let data_avail =
				bytesize::ByteSize(data_part_avail.iter().min().unwrap() * (1 << PARTITION_BITS));
			writeln!(
				&mut ret,
				"\nEstimated available storage space cluster-wide (might be lower in practice):"
			)
			.unwrap();
			if meta_part_avail.len() < node_partition_count.len()
				|| data_part_avail.len() < node_partition_count.len()
			{
				writeln!(&mut ret, "  data: < {}", data_avail).unwrap();
				writeln!(&mut ret, "  metadata: < {}", meta_avail).unwrap();
				writeln!(&mut ret, "A precise estimate could not be given as information is missing for some storage nodes.").unwrap();
			} else {
				writeln!(&mut ret, "  data: {}", data_avail).unwrap();
				writeln!(&mut ret, "  metadata: {}", meta_avail).unwrap();
			}
		}

		Ok(GetClusterStatisticsResponse { freeform: ret })
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
