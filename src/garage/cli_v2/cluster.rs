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
		// TODO: layout history

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
			} else if adv.draining {
				healthy_nodes.push(format!(
					"{id:.16}\t{host}\t{addr}\t\t\tdraining metadata...",
					id = adv.id,
					host = host,
					addr = addr,
				));
			} else {
				let new_role = match layout.staged_role_changes.iter().find(|x| x.id == adv.id) {
					Some(_) => "pending...",
					_ => "NO ROLE ASSIGNED",
				};
				healthy_nodes.push(format!(
					"{id:.16}\t{h}\t{addr}\t\t\t{new_role}",
					id = adv.id,
					h = host,
					addr = addr,
					new_role = new_role,
				));
			}
		}
		format_table(healthy_nodes);

		// Determine which nodes are unhealthy and print that to stdout
		// TODO: do we need this, or can it be done in the GetClusterStatus handler?
		let status_map = status
			.nodes
			.iter()
			.map(|adv| (&adv.id, adv))
			.collect::<HashMap<_, _>>();

		let tf = timeago::Formatter::new();
		let mut drain_msg = false;
		let mut failed_nodes = vec!["ID\tHostname\tTags\tZone\tCapacity\tLast seen".to_string()];
		let mut listed = HashSet::new();
		//for ver in layout.versions.iter().rev() {
		for ver in [&layout].iter() {
			for cfg in ver.roles.iter() {
				let node = &cfg.id;
				if listed.contains(node.as_str()) {
					continue;
				}
				listed.insert(node.as_str());

				let adv = status_map.get(node);
				if adv.map(|x| x.is_up).unwrap_or(false) {
					continue;
				}

				// Node is in a layout version, is not a gateway node, and is not up:
				// it is in a failed state, add proper line to the output
				let (host, last_seen) = match adv {
					Some(adv) => (
						adv.hostname.as_deref().unwrap_or("?"),
						adv.last_seen_secs_ago
							.map(|s| tf.convert(Duration::from_secs(s)))
							.unwrap_or_else(|| "never seen".into()),
					),
					None => ("??", "never seen".into()),
				};
				/*
				let capacity = if ver.version == layout.current().version {
					cfg.capacity_string()
				} else {
					drain_msg = true;
					"draining metadata...".to_string()
				};
				*/
				let capacity = capacity_string(cfg.capacity);

				failed_nodes.push(format!(
					"{id:?}\t{host}\t[{tags}]\t{zone}\t{capacity}\t{last_seen}",
					id = node,
					host = host,
					tags = cfg.tags.join(","),
					zone = cfg.zone,
					capacity = capacity,
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
