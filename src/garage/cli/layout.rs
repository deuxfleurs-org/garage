use bytesize::ByteSize;

use garage_util::crdt::Crdt;
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
		LayoutOperation::Assign(assign_opt) => {
			cmd_assign_role(system_rpc_endpoint, rpc_host, assign_opt).await
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
		LayoutOperation::Config(config_opt) => {
			cmd_config_layout(system_rpc_endpoint, rpc_host, config_opt).await
		}
	}
}

pub async fn cmd_assign_role(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	args: AssignRoleOpt,
) -> Result<(), Error> {
	let status = match rpc_cli
		.call(&rpc_host, SystemRpc::GetKnownNodes, PRIO_NORMAL)
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
	roles.merge(&layout.staging_roles);

	for replaced in args.replace.iter() {
		let replaced_node = find_matching_node(layout.node_ids().iter().cloned(), replaced)?;
		match roles.get(&replaced_node) {
			Some(NodeRoleV(Some(_))) => {
				layout
					.staging_roles
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
	if args.capacity == Some(ByteSize::b(0)) {
		return Err(Error::Message("Invalid capacity value: 0".into()));
	}

	for added_node in added_nodes {
		let new_entry = match roles.get(&added_node) {
			Some(NodeRoleV(Some(old))) => {
				let capacity = match args.capacity {
					Some(c) => Some(c.as_u64()),
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
					Some(c) => Some(c.as_u64()),
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
			.staging_roles
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
	roles.merge(&layout.staging_roles);

	let deleted_node =
		find_matching_node(roles.items().iter().map(|(id, _, _)| *id), &args.node_id)?;

	layout
		.staging_roles
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
	let layout = fetch_layout(rpc_cli, rpc_host).await?;

	println!("==== CURRENT CLUSTER LAYOUT ====");
	if !print_cluster_layout(&layout) {
		println!("No nodes currently have a role in the cluster.");
		println!("See `garage status` to view available nodes.");
	}
	println!();
	println!("Current cluster layout version: {}", layout.version);

	let has_role_changes = print_staging_role_changes(&layout);
	let has_param_changes = print_staging_parameters_changes(&layout);
	if has_role_changes || has_param_changes {
		let v = layout.version;
		let res_apply = layout.apply_staged_changes(Some(v + 1));

		// this will print the stats of what partitions
		// will move around when we apply
		match res_apply {
			Ok((layout, msg)) => {
				println!();
				println!("==== NEW CLUSTER LAYOUT AFTER APPLYING CHANGES ====");
				if !print_cluster_layout(&layout) {
					println!("No nodes have a role in the new layout.");
				}
				println!();

				for line in msg.iter() {
					println!("{}", line);
				}
				println!("To enact the staged role changes, type:");
				println!();
				println!("    garage layout apply --version {}", v + 1);
				println!();
				println!(
                    "You can also revert all proposed changes with: garage layout revert --version {}",
                    v + 1)
			}
			Err(Error::Message(s)) => {
				println!("Error while trying to compute the assignation: {}", s);
				println!("This new layout cannot yet be applied.");
				println!(
                    "You can also revert all proposed changes with: garage layout revert --version {}",
                    v + 1)
			}
			_ => {
				println!("Unknown Error");
			}
		}
	}

	Ok(())
}

pub async fn cmd_apply_layout(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	apply_opt: ApplyLayoutOpt,
) -> Result<(), Error> {
	let layout = fetch_layout(rpc_cli, rpc_host).await?;

	let (layout, msg) = layout.apply_staged_changes(apply_opt.version)?;
	for line in msg.iter() {
		println!("{}", line);
	}

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
	let layout = fetch_layout(rpc_cli, rpc_host).await?;

	let layout = layout.revert_staged_changes(revert_opt.version)?;

	send_layout(rpc_cli, rpc_host, layout).await?;

	println!("All proposed role changes in cluster layout have been canceled.");
	Ok(())
}

pub async fn cmd_config_layout(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	config_opt: ConfigLayoutOpt,
) -> Result<(), Error> {
	let mut layout = fetch_layout(rpc_cli, rpc_host).await?;

	let mut did_something = false;
	match config_opt.redundancy {
		None => (),
		Some(r) => {
			if r > layout.replication_factor {
				println!(
					"The zone redundancy must be smaller or equal to the \
                replication factor ({}).",
					layout.replication_factor
				);
			} else if r < 1 {
				println!("The zone redundancy must be at least 1.");
			} else {
				layout
					.staging_parameters
					.update(LayoutParameters { zone_redundancy: r });
				println!("The new zone redundancy has been saved ({}).", r);
			}
			did_something = true;
		}
	}

	if !did_something {
		return Err(Error::Message(
			"Please specify an action for `garage layout config` to do".into(),
		));
	}

	send_layout(rpc_cli, rpc_host, layout).await?;
	Ok(())
}

// --- utility ---

pub async fn fetch_layout(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
) -> Result<ClusterLayout, Error> {
	match rpc_cli
		.call(&rpc_host, SystemRpc::PullClusterLayout, PRIO_NORMAL)
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
			SystemRpc::AdvertiseClusterLayout(layout),
			PRIO_NORMAL,
		)
		.await??;
	Ok(())
}

pub fn print_cluster_layout(layout: &ClusterLayout) -> bool {
	let mut table = vec!["ID\tTags\tZone\tCapacity\tUsable".to_string()];
	for (id, _, role) in layout.roles.items().iter() {
		let role = match &role.0 {
			Some(r) => r,
			_ => continue,
		};
		let tags = role.tags.join(",");
		let usage = layout.get_node_usage(id).unwrap_or(0);
		let capacity = layout.get_node_capacity(id).unwrap_or(1);
		table.push(format!(
			"{:?}\t{}\t{}\t{}\t{} ({:.1}%)",
			id,
			tags,
			role.zone,
			role.capacity_string(),
			ByteSize::b(usage as u64 * layout.partition_size).to_string_as(false),
			(100.0 * usage as f32 * layout.partition_size as f32) / (capacity as f32)
		));
	}
	println!();
	println!("Parameters of the layout computation:");
	println!("Zone redundancy: {}", layout.parameters.zone_redundancy);
	println!();
	if table.len() == 1 {
		false
	} else {
		format_table(table);
		true
	}
}

pub fn print_staging_parameters_changes(layout: &ClusterLayout) -> bool {
	let has_changes = layout.staging_parameters.get().clone() != layout.parameters;
	if has_changes {
		println!();
		println!("==== NEW LAYOUT PARAMETERS ====");
		println!(
			"Zone redundancy: {}",
			layout.staging_parameters.get().zone_redundancy
		);
		println!();
	}
	has_changes
}

pub fn print_staging_role_changes(layout: &ClusterLayout) -> bool {
	let has_changes = layout
		.staging_roles
		.items()
		.iter()
		.any(|(k, _, v)| layout.roles.get(k) != Some(v));

	if has_changes {
		println!();
		println!("==== STAGED ROLE CHANGES ====");
		let mut table = vec!["ID\tTags\tZone\tCapacity".to_string()];
		for (id, _, role) in layout.staging_roles.items().iter() {
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
