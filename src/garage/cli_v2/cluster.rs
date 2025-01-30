use format_table::format_table;

use garage_util::error::*;

use garage_api::admin::api::*;

use crate::cli::structs::*;
use crate::cli_v2::layout::*;
use crate::cli_v2::*;

impl Cli {
	pub async fn cmd_status(&self) -> Result<(), Error> {
		let status = self.api_request(GetClusterStatusRequest).await?;
		let layout = self.api_request(GetClusterLayoutRequest).await?;

		println!("==== HEALTHY NODES ====");

		let mut healthy_nodes =
			vec!["ID\tHostname\tAddress\tTags\tZone\tCapacity\tDataAvail".to_string()];

		for adv in status.nodes.iter().filter(|adv| adv.is_up) {
			let host = adv.hostname.as_deref().unwrap_or("?");
			let addr = match adv.addr {
				Some(addr) => addr.to_string(),
				None => "N/A".to_string(),
			};
			if let Some(cfg) = &adv.role {
				let data_avail = match &adv.data_partition {
					_ if cfg.capacity.is_none() => "N/A".into(),
					Some(FreeSpaceResp { available, total }) => {
						let pct = (*available as f64) / (*total as f64) * 100.;
						let avail_str = bytesize::ByteSize::b(*available);
						format!("{} ({:.1}%)", avail_str, pct)
					}
					None => "?".into(),
				};
				healthy_nodes.push(format!(
					"{id:.16}\t{host}\t{addr}\t[{tags}]\t{zone}\t{capacity}\t{data_avail}",
					id = adv.id,
					host = host,
					addr = addr,
					tags = cfg.tags.join(","),
					zone = cfg.zone,
					capacity = capacity_string(cfg.capacity),
					data_avail = data_avail,
				));
			} else {
				let status = match layout.staged_role_changes.iter().find(|x| x.id == adv.id) {
					Some(NodeRoleChange {
						action: NodeRoleChangeEnum::Update { .. },
						..
					}) => "pending...",
					_ if adv.draining => "draining metadata..",
					_ => "NO ROLE ASSIGNED",
				};
				healthy_nodes.push(format!(
					"{id:.16}\t{h}\t{addr}\t\t\t{status}",
					id = adv.id,
					h = host,
					addr = addr,
					status = status,
				));
			}
		}
		format_table(healthy_nodes);

		let tf = timeago::Formatter::new();
		let mut drain_msg = false;
		let mut failed_nodes = vec!["ID\tHostname\tTags\tZone\tCapacity\tLast seen".to_string()];
		for adv in status.nodes.iter().filter(|x| !x.is_up) {
			let node = &adv.id;

			let host = adv.hostname.as_deref().unwrap_or("?");
			let last_seen = adv
				.last_seen_secs_ago
				.map(|s| tf.convert(Duration::from_secs(s)))
				.unwrap_or_else(|| "never seen".into());

			if let Some(cfg) = &adv.role {
				let capacity = capacity_string(cfg.capacity);

				failed_nodes.push(format!(
					"{id:.16}\t{host}\t[{tags}]\t{zone}\t{capacity}\t{last_seen}",
					id = node,
					host = host,
					tags = cfg.tags.join(","),
					zone = cfg.zone,
					capacity = capacity,
					last_seen = last_seen,
				));
			} else {
				let status = match layout.staged_role_changes.iter().find(|x| x.id == adv.id) {
					Some(NodeRoleChange {
						action: NodeRoleChangeEnum::Update { .. },
						..
					}) => "pending...",
					_ if adv.draining => {
						drain_msg = true;
						"draining metadata.."
					}
					_ => unreachable!(),
				};

				failed_nodes.push(format!(
					"{id:.16}\t{host}\t\t\t{status}\t{last_seen}",
					id = node,
					host = host,
					status = status,
					last_seen = last_seen,
				));
			}
		}

		if failed_nodes.len() > 1 {
			println!("\n==== FAILED NODES ====");
			format_table(failed_nodes);
			if drain_msg {
				println!();
				println!("Your cluster is expecting to drain data from nodes that are currently unavailable.");
				println!(
					"If these nodes are definitely dead, please review the layout history with"
				);
				println!(
                    "`garage layout history` and use `garage layout skip-dead-nodes` to force progress."
                );
			}
		}

		if print_staging_role_changes(&layout) {
			println!();
			println!(
				"Please use `garage layout show` to check the proposed new layout and apply it."
			);
			println!();
		}

		Ok(())
	}

	pub async fn cmd_connect(&self, opt: ConnectNodeOpt) -> Result<(), Error> {
		let res = self
			.api_request(ConnectClusterNodesRequest(vec![opt.node]))
			.await?;
		if res.0.len() != 1 {
			return Err(Error::Message(format!("unexpected response: {:?}", res)));
		}
		let res = res.0.into_iter().next().unwrap();
		if res.success {
			println!("Success.");
			Ok(())
		} else {
			Err(Error::Message(format!(
				"Failure: {}",
				res.error.unwrap_or_default()
			)))
		}
	}
}
