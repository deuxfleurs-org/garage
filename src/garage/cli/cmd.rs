use std::collections::{HashMap, HashSet};
use std::time::Duration;

use format_table::format_table;
use garage_util::error::*;

use garage_rpc::layout::*;
use garage_rpc::system::*;
use garage_rpc::*;

use garage_model::helper::error::Error as HelperError;

use crate::admin::*;
use crate::cli::*;

pub async fn cli_command_dispatch(
	cmd: Command,
	system_rpc_endpoint: &Endpoint<SystemRpc, ()>,
	admin_rpc_endpoint: &Endpoint<AdminRpc, ()>,
	rpc_host: NodeID,
) -> Result<(), HelperError> {
	match cmd {
		Command::Status => Ok(cmd_status(system_rpc_endpoint, rpc_host).await?),
		Command::Node(NodeOperation::Connect(connect_opt)) => {
			Ok(cmd_connect(system_rpc_endpoint, rpc_host, connect_opt).await?)
		}
		Command::Layout(layout_opt) => {
			Ok(cli_layout_command_dispatch(layout_opt, system_rpc_endpoint, rpc_host).await?)
		}
		Command::Bucket(bo) => {
			cmd_admin(admin_rpc_endpoint, rpc_host, AdminRpc::BucketOperation(bo)).await
		}
		Command::Key(ko) => {
			cmd_admin(admin_rpc_endpoint, rpc_host, AdminRpc::KeyOperation(ko)).await
		}
		Command::Repair(ro) => {
			cmd_admin(admin_rpc_endpoint, rpc_host, AdminRpc::LaunchRepair(ro)).await
		}
		Command::Stats(so) => cmd_admin(admin_rpc_endpoint, rpc_host, AdminRpc::Stats(so)).await,
		Command::Worker(wo) => cmd_admin(admin_rpc_endpoint, rpc_host, AdminRpc::Worker(wo)).await,
		Command::Block(bo) => {
			cmd_admin(admin_rpc_endpoint, rpc_host, AdminRpc::BlockOperation(bo)).await
		}
		Command::Meta(mo) => {
			cmd_admin(admin_rpc_endpoint, rpc_host, AdminRpc::MetaOperation(mo)).await
		}
		_ => unreachable!(),
	}
}

pub async fn cmd_status(rpc_cli: &Endpoint<SystemRpc, ()>, rpc_host: NodeID) -> Result<(), Error> {
	let status = fetch_status(rpc_cli, rpc_host).await?;
	let layout = fetch_layout(rpc_cli, rpc_host).await?;

	println!("==== HEALTHY NODES ====");
	let mut healthy_nodes =
		vec!["ID\tHostname\tAddress\tTags\tZone\tCapacity\tDataAvail".to_string()];
	for adv in status.iter().filter(|adv| adv.is_up) {
		let host = adv.status.hostname.as_deref().unwrap_or("?");
		let addr = match adv.addr {
			Some(addr) => addr.to_string(),
			None => "N/A".to_string(),
		};
		if let Some(NodeRoleV(Some(cfg))) = layout.current().roles.get(&adv.id) {
			let data_avail = match &adv.status.data_disk_avail {
				_ if cfg.capacity.is_none() => "N/A".into(),
				Some((avail, total)) => {
					let pct = (*avail as f64) / (*total as f64) * 100.;
					let avail = bytesize::ByteSize::b(*avail);
					format!("{} ({:.1}%)", avail, pct)
				}
				None => "?".into(),
			};
			healthy_nodes.push(format!(
				"{id:?}\t{host}\t{addr}\t[{tags}]\t{zone}\t{capacity}\t{data_avail}",
				id = adv.id,
				host = host,
				addr = addr,
				tags = cfg.tags.join(","),
				zone = cfg.zone,
				capacity = cfg.capacity_string(),
				data_avail = data_avail,
			));
		} else {
			let prev_role = layout
				.versions
				.iter()
				.rev()
				.find_map(|x| match x.roles.get(&adv.id) {
					Some(NodeRoleV(Some(cfg))) => Some(cfg),
					_ => None,
				});
			if let Some(cfg) = prev_role {
				healthy_nodes.push(format!(
					"{id:?}\t{host}\t{addr}\t[{tags}]\t{zone}\tdraining metadata...",
					id = adv.id,
					host = host,
					addr = addr,
					tags = cfg.tags.join(","),
					zone = cfg.zone,
				));
			} else {
				let new_role = match layout.staging.get().roles.get(&adv.id) {
					Some(NodeRoleV(Some(_))) => "pending...",
					_ => "NO ROLE ASSIGNED",
				};
				healthy_nodes.push(format!(
					"{id:?}\t{h}\t{addr}\t\t\t{new_role}",
					id = adv.id,
					h = host,
					addr = addr,
					new_role = new_role,
				));
			}
		}
	}
	format_table(healthy_nodes);

	// Determine which nodes are unhealthy and print that to stdout
	let status_map = status
		.iter()
		.map(|adv| (adv.id, adv))
		.collect::<HashMap<_, _>>();

	let tf = timeago::Formatter::new();
	let mut drain_msg = false;
	let mut failed_nodes = vec!["ID\tHostname\tTags\tZone\tCapacity\tLast seen".to_string()];
	let mut listed = HashSet::new();
	for ver in layout.versions.iter().rev() {
		for (node, _, role) in ver.roles.items().iter() {
			let cfg = match role {
				NodeRoleV(Some(role)) if role.capacity.is_some() => role,
				_ => continue,
			};

			if listed.contains(node) {
				continue;
			}
			listed.insert(*node);

			let adv = status_map.get(node);
			if adv.map(|x| x.is_up).unwrap_or(false) {
				continue;
			}

			// Node is in a layout version, is not a gateway node, and is not up:
			// it is in a failed state, add proper line to the output
			let (host, last_seen) = match adv {
				Some(adv) => (
					adv.status.hostname.as_deref().unwrap_or("?"),
					adv.last_seen_secs_ago
						.map(|s| tf.convert(Duration::from_secs(s)))
						.unwrap_or_else(|| "never seen".into()),
				),
				None => ("??", "never seen".into()),
			};
			let capacity = if ver.version == layout.current().version {
				cfg.capacity_string()
			} else {
				drain_msg = true;
				"draining metadata...".to_string()
			};
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
			println!("If these nodes are definitely dead, please review the layout history with");
			println!(
				"`garage layout history` and use `garage layout skip-dead-nodes` to force progress."
			);
		}
	}

	if print_staging_role_changes(&layout) {
		println!();
		println!("Please use `garage layout show` to check the proposed new layout and apply it.");
		println!();
	}

	Ok(())
}

pub async fn cmd_connect(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	args: ConnectNodeOpt,
) -> Result<(), Error> {
	match rpc_cli
		.call(&rpc_host, SystemRpc::Connect(args.node), PRIO_NORMAL)
		.await??
	{
		SystemRpc::Ok => {
			println!("Success.");
			Ok(())
		}
		m => Err(Error::unexpected_rpc_message(m)),
	}
}

pub async fn cmd_admin(
	rpc_cli: &Endpoint<AdminRpc, ()>,
	rpc_host: NodeID,
	args: AdminRpc,
) -> Result<(), HelperError> {
	match rpc_cli.call(&rpc_host, args, PRIO_NORMAL).await?? {
		AdminRpc::Ok(msg) => {
			println!("{}", msg);
		}
		AdminRpc::BucketList(bl) => {
			print_bucket_list(bl);
		}
		AdminRpc::BucketInfo {
			bucket,
			relevant_keys,
			counters,
			mpu_counters,
		} => {
			print_bucket_info(&bucket, &relevant_keys, &counters, &mpu_counters);
		}
		AdminRpc::KeyList(kl) => {
			print_key_list(kl);
		}
		AdminRpc::KeyInfo(key, rb) => {
			print_key_info(&key, &rb);
		}
		AdminRpc::WorkerList(wi, wlo) => {
			print_worker_list(wi, wlo);
		}
		AdminRpc::WorkerVars(wv) => {
			print_worker_vars(wv);
		}
		AdminRpc::WorkerInfo(tid, wi) => {
			print_worker_info(tid, wi);
		}
		AdminRpc::BlockErrorList(el) => {
			print_block_error_list(el);
		}
		AdminRpc::BlockInfo {
			hash,
			refcount,
			versions,
			uploads,
		} => {
			print_block_info(hash, refcount, versions, uploads);
		}
		r => {
			error!("Unexpected response: {:?}", r);
		}
	}
	Ok(())
}

// ---- utility ----

pub async fn fetch_status(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
) -> Result<Vec<KnownNodeInfo>, Error> {
	match rpc_cli
		.call(&rpc_host, SystemRpc::GetKnownNodes, PRIO_NORMAL)
		.await??
	{
		SystemRpc::ReturnKnownNodes(nodes) => Ok(nodes),
		resp => Err(Error::unexpected_rpc_message(resp)),
	}
}
