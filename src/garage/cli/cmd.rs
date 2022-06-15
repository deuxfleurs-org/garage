use std::collections::HashSet;

use garage_util::error::*;
use garage_util::formater::format_table;

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
		Command::Migrate(mo) => {
			cmd_admin(admin_rpc_endpoint, rpc_host, AdminRpc::Migrate(mo)).await
		}
		Command::Repair(ro) => {
			cmd_admin(admin_rpc_endpoint, rpc_host, AdminRpc::LaunchRepair(ro)).await
		}
		Command::Stats(so) => cmd_admin(admin_rpc_endpoint, rpc_host, AdminRpc::Stats(so)).await,
		_ => unreachable!(),
	}
}

pub async fn cmd_status(rpc_cli: &Endpoint<SystemRpc, ()>, rpc_host: NodeID) -> Result<(), Error> {
	let status = match rpc_cli
		.call(&rpc_host, &SystemRpc::GetKnownNodes, PRIO_NORMAL)
		.await??
	{
		SystemRpc::ReturnKnownNodes(nodes) => nodes,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	};
	let layout = fetch_layout(rpc_cli, rpc_host).await?;

	println!("==== HEALTHY NODES ====");
	let mut healthy_nodes = vec!["ID\tHostname\tAddress\tTags\tZone\tCapacity".to_string()];
	for adv in status.iter().filter(|adv| adv.is_up) {
		match layout.roles.get(&adv.id) {
			Some(NodeRoleV(Some(cfg))) => {
				healthy_nodes.push(format!(
					"{id:?}\t{host}\t{addr}\t[{tags}]\t{zone}\t{capacity}",
					id = adv.id,
					host = adv.status.hostname,
					addr = adv.addr,
					tags = cfg.tags.join(","),
					zone = cfg.zone,
					capacity = cfg.capacity_string(),
				));
			}
			_ => {
				let new_role = match layout.staging.get(&adv.id) {
					Some(NodeRoleV(Some(_))) => "(pending)",
					_ => "NO ROLE ASSIGNED",
				};
				healthy_nodes.push(format!(
					"{id:?}\t{h}\t{addr}\t{new_role}",
					id = adv.id,
					h = adv.status.hostname,
					addr = adv.addr,
					new_role = new_role,
				));
			}
		}
	}
	format_table(healthy_nodes);

	let status_keys = status.iter().map(|adv| adv.id).collect::<HashSet<_>>();
	let failure_case_1 = status
		.iter()
		.any(|adv| !adv.is_up && matches!(layout.roles.get(&adv.id), Some(NodeRoleV(Some(_)))));
	let failure_case_2 = layout
		.roles
		.items()
		.iter()
		.any(|(id, _, v)| !status_keys.contains(id) && v.0.is_some());
	if failure_case_1 || failure_case_2 {
		println!("\n==== FAILED NODES ====");
		let mut failed_nodes =
			vec!["ID\tHostname\tAddress\tTags\tZone\tCapacity\tLast seen".to_string()];
		for adv in status.iter().filter(|adv| !adv.is_up) {
			if let Some(NodeRoleV(Some(cfg))) = layout.roles.get(&adv.id) {
				failed_nodes.push(format!(
					"{id:?}\t{host}\t{addr}\t[{tags}]\t{zone}\t{capacity}\t{last_seen}",
					id = adv.id,
					host = adv.status.hostname,
					addr = adv.addr,
					tags = cfg.tags.join(","),
					zone = cfg.zone,
					capacity = cfg.capacity_string(),
					last_seen = adv
						.last_seen_secs_ago
						.map(|s| format!("{}s ago", s))
						.unwrap_or_else(|| "never seen".into()),
				));
			}
		}
		for (id, _, role_v) in layout.roles.items().iter() {
			if let NodeRoleV(Some(cfg)) = role_v {
				if !status_keys.contains(id) {
					failed_nodes.push(format!(
						"{id:?}\t??\t??\t[{tags}]\t{zone}\t{capacity}\tnever seen",
						id = id,
						tags = cfg.tags.join(","),
						zone = cfg.zone,
						capacity = cfg.capacity_string(),
					));
				}
			}
		}
		format_table(failed_nodes);
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
		.call(&rpc_host, &SystemRpc::Connect(args.node), PRIO_NORMAL)
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
	match rpc_cli.call(&rpc_host, &args, PRIO_NORMAL).await?? {
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
		} => {
			print_bucket_info(&bucket, &relevant_keys, &counters);
		}
		AdminRpc::KeyList(kl) => {
			print_key_list(kl);
		}
		AdminRpc::KeyInfo(key, rb) => {
			print_key_info(&key, &rb);
		}
		r => {
			error!("Unexpected response: {:?}", r);
		}
	}
	Ok(())
}
