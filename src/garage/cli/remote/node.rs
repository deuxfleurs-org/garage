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

		let mut table = vec![];
		for (node, err) in res.error.iter() {
			table.push(format!("{:.16}\tError: {}", node, err));
		}
		for (node, _) in res.success.iter() {
			table.push(format!("{:.16}\tSnapshot created", node));
		}
		format_table(table);

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
			println!("======================");
			println!("Stats for node {:.16}:\n", node);
			println!("{}\n", res.freeform);
		}

		for (node, err) in res.error.iter() {
			println!("======================");
			println!("Node {:.16}: error: {}\n", node, err);
		}

		let res = self.api_request(GetClusterStatisticsRequest).await?;
		println!("======================");
		println!("Cluster statistics:\n");
		println!("{}\n", res.freeform);

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
