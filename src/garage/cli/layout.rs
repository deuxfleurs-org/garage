use garage_util::crdt::Crdt;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::formater::format_table;

use garage_rpc::layout::*;
use garage_rpc::system::*;
use garage_rpc::*;

use crate::cli::*;

pub async fn cli_layout_command_dispatch(
	cmd: LayoutOperation,
	system_rpc_endpoint: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
) -> Result<(), Error> {
	match cmd {
		LayoutOperation::Assign(configure_opt) => {
			cmd_assign_role(system_rpc_endpoint, rpc_host, configure_opt).await
		}
		LayoutOperation::Remove(remove_opt) => {
			cmd_remove_role(system_rpc_endpoint, rpc_host, remove_opt).await
		}
		LayoutOperation::Show => cmd_show_layout(system_rpc_endpoint, rpc_host).await,
		LayoutOperation::Apply(apply_opt) => {
			cmd_apply_layout(system_rpc_endpoint, rpc_host, apply_opt).await
		}
		LayoutOperation::Revert(revert_opt) => {
			cmd_revert_layout(system_rpc_endpoint, rpc_host, revert_opt).await
		}
	}
}

pub async fn cmd_assign_role(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	args: AssignRoleOpt,
) -> Result<(), Error> {
	let status = match rpc_cli
		.call(&rpc_host, &SystemRpc::GetKnownNodes, PRIO_NORMAL)
		.await??
	{
		SystemRpc::ReturnKnownNodes(nodes) => nodes,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	};

	let mut layout = fetch_layout(rpc_cli, rpc_host).await?;

	let added_nodes = args
		.node_ids
		.iter()
		.map(|node_id| {
			find_matching_node(
				status
					.iter()
					.map(|adv| adv.id)
					.chain(layout.node_ids().iter().cloned()),
				node_id,
			)
		})
		.collect::<Result<Vec<_>, _>>()?;

	let mut roles = layout.roles.clone();
	roles.merge(&layout.staging);

	for replaced in args.replace.iter() {
		let replaced_node = find_matching_node(layout.node_ids().iter().cloned(), replaced)?;
		match roles.get(&replaced_node) {
			Some(NodeRoleV(Some(_))) => {
				layout
					.staging
					.merge(&roles.update_mutator(replaced_node, NodeRoleV(None)));
			}
			_ => {
				return Err(Error::Message(format!(
					"Cannot replace node {:?} as it is not currently in planned layout",
					replaced_node
				)));
			}
		}
	}

	if args.capacity.is_some() && args.gateway {
		return Err(Error::Message(
				"-c and -g are mutually exclusive, please configure node either with c>0 to act as a storage node or with -g to act as a gateway node".into()));
	}
	if args.capacity == Some(0) {
		return Err(Error::Message("Invalid capacity value: 0".into()));
	}

	for added_node in added_nodes {
		let new_entry = match roles.get(&added_node) {
			Some(NodeRoleV(Some(old))) => {
				let capacity = match args.capacity {
					Some(c) => Some(c),
					None if args.gateway => None,
					None => old.capacity,
				};
				let tags = if args.tags.is_empty() {
					old.tags.clone()
				} else {
					args.tags.clone()
				};
				NodeRole {
					zone: args.zone.clone().unwrap_or_else(|| old.zone.to_string()),
					capacity,
					tags,
				}
			}
			_ => {
				let capacity = match args.capacity {
					Some(c) => Some(c),
					None if args.gateway => None,
					None => return Err(Error::Message(
							"Please specify a capacity with the -c flag, or set node explicitly as gateway with -g".into())),
				};
				NodeRole {
					zone: args
						.zone
						.clone()
						.ok_or("Please specifiy a zone with the -z flag")?,
					capacity,
					tags: args.tags.clone(),
				}
			}
		};

		layout
			.staging
			.merge(&roles.update_mutator(added_node, NodeRoleV(Some(new_entry))));
	}

	send_layout(rpc_cli, rpc_host, layout).await?;

	println!("Role changes are staged but not yet commited.");
	println!("Use `garage layout show` to view staged role changes,");
	println!("and `garage layout apply` to enact staged changes.");
	Ok(())
}

pub async fn cmd_remove_role(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	args: RemoveRoleOpt,
) -> Result<(), Error> {
	let mut layout = fetch_layout(rpc_cli, rpc_host).await?;

	let mut roles = layout.roles.clone();
	roles.merge(&layout.staging);

	let deleted_node =
		find_matching_node(roles.items().iter().map(|(id, _, _)| *id), &args.node_id)?;

	layout
		.staging
		.merge(&roles.update_mutator(deleted_node, NodeRoleV(None)));

	send_layout(rpc_cli, rpc_host, layout).await?;

	println!("Role removal is staged but not yet commited.");
	println!("Use `garage layout show` to view staged role changes,");
	println!("and `garage layout apply` to enact staged changes.");
	Ok(())
}

pub async fn cmd_show_layout(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
) -> Result<(), Error> {
	let mut layout = fetch_layout(rpc_cli, rpc_host).await?;

	println!("==== CURRENT CLUSTER LAYOUT ====");
	if !print_cluster_layout(&layout) {
		println!("No nodes currently have a role in the cluster.");
		println!("See `garage status` to view available nodes.");
	}
	println!();
	println!("Current cluster layout version: {}", layout.version);

	if print_staging_role_changes(&layout) {
		layout.roles.merge(&layout.staging);

		println!();
		println!("==== NEW CLUSTER LAYOUT AFTER APPLYING CHANGES ====");
		if !print_cluster_layout(&layout) {
			println!("No nodes have a role in the new layout.");
		}
		println!();

		// this will print the stats of what partitions
		// will move around when we apply
		if layout.calculate_partition_assignation() {
			println!("To enact the staged role changes, type:");
			println!();
			println!("    garage layout apply --version {}", layout.version + 1);
			println!();
			println!(
				"You can also revert all proposed changes with: garage layout revert --version {}",
				layout.version + 1
			);
		} else {
			println!("Not enough nodes have an assigned role to maintain enough copies of data.");
			println!("This new layout cannot yet be applied.");
		}
	}

	Ok(())
}

pub async fn cmd_apply_layout(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	apply_opt: ApplyLayoutOpt,
) -> Result<(), Error> {
	let mut layout = fetch_layout(rpc_cli, rpc_host).await?;

	match apply_opt.version {
		None => {
			println!("Please pass the --version flag to ensure that you are writing the correct version of the cluster layout.");
			println!("To know the correct value of the --version flag, invoke `garage layout show` and review the proposed changes.");
			return Err(Error::Message("--version flag is missing".into()));
		}
		Some(v) => {
			if v != layout.version + 1 {
				return Err(Error::Message("Invalid value of --version flag".into()));
			}
		}
	}

	layout.roles.merge(&layout.staging);

	if !layout.calculate_partition_assignation() {
		return Err(Error::Message("Could not calculate new assignation of partitions to nodes. This can happen if there are less nodes than the desired number of copies of your data (see the replication_mode configuration parameter).".into()));
	}

	layout.staging.clear();
	layout.staging_hash = blake2sum(&rmp_to_vec_all_named(&layout.staging).unwrap()[..]);

	layout.version += 1;

	send_layout(rpc_cli, rpc_host, layout).await?;

	println!("New cluster layout with updated role assignation has been applied in cluster.");
	println!("Data will now be moved around between nodes accordingly.");

	Ok(())
}

pub async fn cmd_revert_layout(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	revert_opt: RevertLayoutOpt,
) -> Result<(), Error> {
	let mut layout = fetch_layout(rpc_cli, rpc_host).await?;

	match revert_opt.version {
		None => {
			println!("Please pass the --version flag to ensure that you are writing the correct version of the cluster layout.");
			println!("To know the correct value of the --version flag, invoke `garage layout show` and review the proposed changes.");
			return Err(Error::Message("--version flag is missing".into()));
		}
		Some(v) => {
			if v != layout.version + 1 {
				return Err(Error::Message("Invalid value of --version flag".into()));
			}
		}
	}

	layout.staging.clear();
	layout.staging_hash = blake2sum(&rmp_to_vec_all_named(&layout.staging).unwrap()[..]);

	layout.version += 1;

	send_layout(rpc_cli, rpc_host, layout).await?;

	println!("All proposed role changes in cluster layout have been canceled.");
	Ok(())
}

// --- utility ---

pub async fn fetch_layout(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
) -> Result<ClusterLayout, Error> {
	match rpc_cli
		.call(&rpc_host, &SystemRpc::PullClusterLayout, PRIO_NORMAL)
		.await??
	{
		SystemRpc::AdvertiseClusterLayout(t) => Ok(t),
		resp => Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	}
}

pub async fn send_layout(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	layout: ClusterLayout,
) -> Result<(), Error> {
	rpc_cli
		.call(
			&rpc_host,
			&SystemRpc::AdvertiseClusterLayout(layout),
			PRIO_NORMAL,
		)
		.await??;
	Ok(())
}

pub fn print_cluster_layout(layout: &ClusterLayout) -> bool {
	let mut table = vec!["ID\tTags\tZone\tCapacity".to_string()];
	for (id, _, role) in layout.roles.items().iter() {
		let role = match &role.0 {
			Some(r) => r,
			_ => continue,
		};
		let tags = role.tags.join(",");
		table.push(format!(
			"{:?}\t{}\t{}\t{}",
			id,
			tags,
			role.zone,
			role.capacity_string()
		));
	}
	if table.len() == 1 {
		false
	} else {
		format_table(table);
		true
	}
}

pub fn print_staging_role_changes(layout: &ClusterLayout) -> bool {
	let has_changes = layout
		.staging
		.items()
		.iter()
		.any(|(k, _, v)| layout.roles.get(k) != Some(v));

	if has_changes {
		println!();
		println!("==== STAGED ROLE CHANGES ====");
		let mut table = vec!["ID\tTags\tZone\tCapacity".to_string()];
		for (id, _, role) in layout.staging.items().iter() {
			if layout.roles.get(id) == Some(role) {
				continue;
			}
			if let Some(role) = &role.0 {
				let tags = role.tags.join(",");
				table.push(format!(
					"{:?}\t{}\t{}\t{}",
					id,
					tags,
					role.zone,
					role.capacity_string()
				));
			} else {
				table.push(format!("{:?}\tREMOVED", id));
			}
		}
		format_table(table);
		true
	} else {
		false
	}
}
