use std::collections::HashSet;

use garage_util::error::*;

use garage_rpc::ring::*;
use garage_rpc::system::*;
use garage_rpc::*;

use crate::admin::*;
use crate::cli::*;

pub async fn cli_command_dispatch(
	cmd: Command,
	system_rpc_endpoint: &Endpoint<SystemRpc, ()>,
	admin_rpc_endpoint: &Endpoint<AdminRpc, ()>,
	rpc_host: NodeID,
) -> Result<(), Error> {
	match cmd {
		Command::Status => cmd_status(system_rpc_endpoint, rpc_host).await,
		Command::Node(NodeOperation::Connect(connect_opt)) => {
			cmd_connect(system_rpc_endpoint, rpc_host, connect_opt).await
		}
		Command::Node(NodeOperation::Configure(configure_opt)) => {
			cmd_configure(system_rpc_endpoint, rpc_host, configure_opt).await
		}
		Command::Node(NodeOperation::Remove(remove_opt)) => {
			cmd_remove(system_rpc_endpoint, rpc_host, remove_opt).await
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
	let config = match rpc_cli
		.call(&rpc_host, &SystemRpc::PullConfig, PRIO_NORMAL)
		.await??
	{
		SystemRpc::AdvertiseConfig(cfg) => cfg,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	};

	println!("==== HEALTHY NODES ====");
	let mut healthy_nodes = vec!["ID\tHostname\tAddress\tTag\tZone\tCapacity".to_string()];
	for adv in status.iter().filter(|adv| adv.is_up) {
		if let Some(cfg) = config.members.get(&adv.id) {
			healthy_nodes.push(format!(
				"{id:?}\t{host}\t{addr}\t[{tag}]\t{zone}\t{capacity}",
				id = adv.id,
				host = adv.status.hostname,
				addr = adv.addr,
				tag = cfg.tag,
				zone = cfg.zone,
				capacity = cfg.capacity_string(),
			));
		} else {
			healthy_nodes.push(format!(
				"{id:?}\t{h}\t{addr}\tNO ROLE ASSIGNED",
				id = adv.id,
				h = adv.status.hostname,
				addr = adv.addr,
			));
		}
	}
	format_table(healthy_nodes);

	let status_keys = status.iter().map(|adv| adv.id).collect::<HashSet<_>>();
	let failure_case_1 = status.iter().any(|adv| !adv.is_up);
	let failure_case_2 = config
		.members
		.iter()
		.any(|(id, _)| !status_keys.contains(id));
	if failure_case_1 || failure_case_2 {
		println!("\n==== FAILED NODES ====");
		let mut failed_nodes =
			vec!["ID\tHostname\tAddress\tTag\tZone\tCapacity\tLast seen".to_string()];
		for adv in status.iter().filter(|adv| !adv.is_up) {
			if let Some(cfg) = config.members.get(&adv.id) {
				failed_nodes.push(format!(
					"{id:?}\t{host}\t{addr}\t[{tag}]\t{zone}\t{capacity}\t{last_seen}",
					id = adv.id,
					host = adv.status.hostname,
					addr = adv.addr,
					tag = cfg.tag,
					zone = cfg.zone,
					capacity = cfg.capacity_string(),
					last_seen = adv
						.last_seen_secs_ago
						.map(|s| format!("{}s ago", s))
						.unwrap_or_else(|| "never seen".into()),
				));
			}
		}
		for (id, cfg) in config.members.iter() {
			if !status_keys.contains(id) {
				failed_nodes.push(format!(
					"{id:?}\t??\t??\t[{tag}]\t{zone}\t{capacity}\tnever seen",
					id = id,
					tag = cfg.tag,
					zone = cfg.zone,
					capacity = cfg.capacity_string(),
				));
			}
		}
		format_table(failed_nodes);
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
		r => Err(Error::BadRpc(format!("Unexpected response: {:?}", r))),
	}
}

pub async fn cmd_configure(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	args: ConfigureNodeOpt,
) -> Result<(), Error> {
	let status = match rpc_cli
		.call(&rpc_host, &SystemRpc::GetKnownNodes, PRIO_NORMAL)
		.await??
	{
		SystemRpc::ReturnKnownNodes(nodes) => nodes,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	};

	let added_node = find_matching_node(status.iter().map(|adv| adv.id), &args.node_id)?;

	let mut config = match rpc_cli
		.call(&rpc_host, &SystemRpc::PullConfig, PRIO_NORMAL)
		.await??
	{
		SystemRpc::AdvertiseConfig(cfg) => cfg,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	};

	for replaced in args.replace.iter() {
		let replaced_node = find_matching_node(config.members.keys().cloned(), replaced)?;
		if config.members.remove(&replaced_node).is_none() {
			return Err(Error::Message(format!(
				"Cannot replace node {:?} as it is not in current configuration",
				replaced_node
			)));
		}
	}

	if args.capacity.is_some() && args.gateway {
		return Err(Error::Message(
				"-c and -g are mutually exclusive, please configure node either with c>0 to act as a storage node or with -g to act as a gateway node".into()));
	}
	if args.capacity == Some(0) {
		return Err(Error::Message("Invalid capacity value: 0".into()));
	}

	let new_entry = match config.members.get(&added_node) {
		None => {
			let capacity = match args.capacity {
				Some(c) => Some(c),
				None if args.gateway => None,
				_ => return Err(Error::Message(
						"Please specify a capacity with the -c flag, or set node explicitly as gateway with -g".into())),
			};
			NetworkConfigEntry {
				zone: args.zone.ok_or("Please specifiy a zone with the -z flag")?,
				capacity,
				tag: args.tag.unwrap_or_default(),
			}
		}
		Some(old) => {
			let capacity = match args.capacity {
				Some(c) => Some(c),
				None if args.gateway => None,
				_ => old.capacity,
			};
			NetworkConfigEntry {
				zone: args.zone.unwrap_or_else(|| old.zone.to_string()),
				capacity,
				tag: args.tag.unwrap_or_else(|| old.tag.to_string()),
			}
		}
	};

	config.members.insert(added_node, new_entry);
	config.version += 1;

	rpc_cli
		.call(&rpc_host, &SystemRpc::AdvertiseConfig(config), PRIO_NORMAL)
		.await??;
	Ok(())
}

pub async fn cmd_remove(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	args: RemoveNodeOpt,
) -> Result<(), Error> {
	let mut config = match rpc_cli
		.call(&rpc_host, &SystemRpc::PullConfig, PRIO_NORMAL)
		.await??
	{
		SystemRpc::AdvertiseConfig(cfg) => cfg,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	};

	let deleted_node = find_matching_node(config.members.keys().cloned(), &args.node_id)?;

	if !args.yes {
		return Err(Error::Message(format!(
			"Add the flag --yes to really remove {:?} from the cluster",
			deleted_node
		)));
	}

	config.members.remove(&deleted_node);
	config.version += 1;

	rpc_cli
		.call(&rpc_host, &SystemRpc::AdvertiseConfig(config), PRIO_NORMAL)
		.await??;
	Ok(())
}

pub async fn cmd_admin(
	rpc_cli: &Endpoint<AdminRpc, ()>,
	rpc_host: NodeID,
	args: AdminRpc,
) -> Result<(), Error> {
	match rpc_cli.call(&rpc_host, &args, PRIO_NORMAL).await?? {
		AdminRpc::Ok(msg) => {
			println!("{}", msg);
		}
		AdminRpc::BucketList(bl) => {
			println!("List of buckets:");
			for bucket in bl {
				println!("{}", bucket);
			}
		}
		AdminRpc::BucketInfo(bucket) => {
			print_bucket_info(&bucket);
		}
		AdminRpc::KeyList(kl) => {
			println!("List of keys:");
			for key in kl {
				println!("{}\t{}", key.0, key.1);
			}
		}
		AdminRpc::KeyInfo(key) => {
			print_key_info(&key);
		}
		r => {
			error!("Unexpected response: {:?}", r);
		}
	}
	Ok(())
}

// --- Utility functions ----
