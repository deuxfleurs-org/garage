use format_table::format_table;

use garage_util::error::*;

use garage_api_admin::api::*;

use crate::cli::remote::*;
use crate::cli::structs::*;

impl Cli {
	pub async fn cmd_meta(&self, cmd: MetaOperation) -> Result<(), Error> {
		let MetaOperation::Snapshot { all } = cmd;

		let res = self
			.api_request(CreateMetadataSnapshotRequest {
				node: if all {
					"*".to_string()
				} else {
					hex::encode(self.rpc_host)
				},
				body: LocalCreateMetadataSnapshotRequest,
			})
			.await?;

		let mut table = vec!["Node\tResult".to_string()];
		for (node, _) in res.success.iter() {
			table.push(format!("{:.16}\tSnapshot created", node));
		}
		for (node, err) in res.error.iter() {
			table.push(format!("{:.16}\tError: {}", node, err));
		}
		format_table(table);

		if !res.error.is_empty() {
			return Err(Error::Message(format!(
				"{} nodes returned an error",
				res.error.len()
			)));
		}

		Ok(())
	}

	pub async fn cmd_stats(&self, cmd: StatsOpt) -> Result<(), Error> {
		let res = self
			.api_request(GetNodeStatisticsRequest {
				node: if cmd.all_nodes {
					"*".to_string()
				} else {
					hex::encode(self.rpc_host)
				},
				body: LocalGetNodeStatisticsRequest,
			})
			.await?;

		for (node, res) in res.success.iter() {
			println!("==== NODE [{:.16}] ====", node);
			println!("{}\n", res.freeform);
		}

		for (node, err) in res.error.iter() {
			println!("==== NODE [{:.16}] ====", node);
			println!("Error: {}\n", err);
		}

		println!("==== CLUSTER STATISTICS ====");

		let stats = self.api_request(GetClusterStatisticsRequest).await?;
		let status = self.api_request(GetClusterStatusRequest).await?;
		let layout = self.api_request(GetClusterLayoutRequest).await?;

		// Display storage node info
		println!("Storage nodes:");
		let mut table = vec!["  ID\tHostname\tZone\tCapacity\tPart.\tDataAvail\tMetaAvail".into()];
		for role in layout.roles.iter() {
			let Some(capacity) = role.capacity else {
				continue;
			};
			let status = status.nodes.iter().find(|x| x.id == role.id);

			let hostname = status.and_then(|x| x.hostname.as_deref()).unwrap_or("?");
			let zone = role.zone.as_str();
			let capacity = bytesize::ByteSize::b(capacity).to_string();
			let partitions = role
				.stored_partitions
				.map(|x| x.to_string())
				.unwrap_or_else(|| "?".into());

			let avail_str = |x| match x {
				Some(FreeSpaceResp { available, total }) => {
					let pct = (available as f64) / (total as f64) * 100.;
					let avail = bytesize::ByteSize::b(available);
					let total = bytesize::ByteSize::b(total);
					format!("{}/{} ({:.1}%)", avail, total, pct)
				}
				None => "?".into(),
			};
			let data_avail = avail_str(status.and_then(|x| x.data_partition));
			let meta_avail = avail_str(status.and_then(|x| x.metadata_partition));

			table.push(format!(
				"  {:.16}\t{}\t{}\t{}\t{}\t{}\t{}",
				role.id, hostname, zone, capacity, partitions, data_avail, meta_avail
			));
		}
		format_table(table);

		// Display bucket statistics
		let mut bucket_stats = vec![format!("Number of buckets:\t{}", stats.bucket_count)];
		if let Some(toc) = stats.total_object_count {
			bucket_stats.push(format!("Total number of objects:\t{}", toc));
		}
		if let Some(tob) = stats.total_object_bytes {
			bucket_stats.push(format!(
				"Total size of objects:\t{}",
				bytesize::ByteSize(tob)
			));
		}
		println!();
		format_table(bucket_stats);

		println!("\nEstimated available storage space cluster-wide (might be lower in practice):");
		let metadata_avail_str = bytesize::ByteSize(stats.metadata_avail);
		let data_avail_str = bytesize::ByteSize(stats.data_avail);
		if stats.incomplete_avail_info {
			format_table(vec![
				format!("  data:\t< {}", data_avail_str),
				format!("  metadata:\t< {}", metadata_avail_str),
			]);
			println!("A precise estimate could not be given as information is missing for some storage nodes.");
		} else {
			format_table(vec![
				format!("  data:\t{}", data_avail_str),
				format!("  metadata:\t{}", metadata_avail_str),
			]);
		}

		Ok(())
	}

	pub async fn cmd_repair(&self, cmd: RepairOpt) -> Result<(), Error> {
		if !cmd.yes {
			return Err(Error::Message(
				"Please add --yes to start the repair operation".into(),
			));
		}

		let repair_type = match cmd.what {
			RepairWhat::Tables => RepairType::Tables,
			RepairWhat::Blocks => RepairType::Blocks,
			RepairWhat::Versions => RepairType::Versions,
			RepairWhat::MultipartUploads => RepairType::MultipartUploads,
			RepairWhat::BlockRefs => RepairType::BlockRefs,
			RepairWhat::BlockRc => RepairType::BlockRc,
			RepairWhat::Rebalance => RepairType::Rebalance,
			RepairWhat::Scrub { cmd } => RepairType::Scrub(match cmd {
				ScrubCmd::Start => ScrubCommand::Start,
				ScrubCmd::Cancel => ScrubCommand::Cancel,
				ScrubCmd::Pause => ScrubCommand::Pause,
				ScrubCmd::Resume => ScrubCommand::Resume,
			}),
			RepairWhat::Aliases => RepairType::Aliases,
			RepairWhat::ClearResyncQueue => RepairType::ClearResyncQueue,
		};

		let res = self
			.api_request(LaunchRepairOperationRequest {
				node: if cmd.all_nodes {
					"*".to_string()
				} else {
					hex::encode(self.rpc_host)
				},
				body: LocalLaunchRepairOperationRequest { repair_type },
			})
			.await?;

		let mut table = vec![];
		for (node, err) in res.error.iter() {
			table.push(format!("{:.16}\tError: {}", node, err));
		}
		for (node, _) in res.success.iter() {
			table.push(format!("{:.16}\tRepair launched", node));
		}
		format_table(table);

		Ok(())
	}
}
