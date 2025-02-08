use bytesize::ByteSize;

use format_table::format_table;
use garage_util::crdt::Crdt;
use garage_util::error::*;

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
		LayoutOperation::History => cmd_layout_history(system_rpc_endpoint, rpc_host).await,
		LayoutOperation::SkipDeadNodes(assume_sync_opt) => {
			cmd_layout_skip_dead_nodes(system_rpc_endpoint, rpc_host, assume_sync_opt).await
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
	let all_nodes = layout.get_all_nodes();

	let added_nodes = args
		.node_ids
		.iter()
		.map(|node_id| {
			find_matching_node(
				status
					.iter()
					.map(|adv| adv.id)
					.chain(all_nodes.iter().cloned()),
				node_id,
			)
		})
		.collect::<Result<Vec<_>, _>>()?;

	let mut roles = layout.current().roles.clone();
	roles.merge(&layout.staging.get().roles);

	for replaced in args.replace.iter() {
		let replaced_node = find_matching_node(all_nodes.iter().cloned(), replaced)?;
		match roles.get(&replaced_node) {
			Some(NodeRoleV(Some(_))) => {
				layout
					.staging
					.get_mut()
					.roles
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
						.ok_or("Please specify a zone with the -z flag")?,
					capacity,
					tags: args.tags.clone(),
				}
			}
		};

		layout
			.staging
			.get_mut()
			.roles
			.merge(&roles.update_mutator(added_node, NodeRoleV(Some(new_entry))));
	}

	send_layout(rpc_cli, rpc_host, layout).await?;

	println!("Role changes are staged but not yet committed.");
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

	let mut roles = layout.current().roles.clone();
	roles.merge(&layout.staging.get().roles);

	let deleted_node =
		find_matching_node(roles.items().iter().map(|(id, _, _)| *id), &args.node_id)?;

	layout
		.staging
		.get_mut()
		.roles
		.merge(&roles.update_mutator(deleted_node, NodeRoleV(None)));

	send_layout(rpc_cli, rpc_host, layout).await?;

	println!("Role removal is staged but not yet committed.");
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
	print_cluster_layout(layout.current(), "No nodes currently have a role in the cluster.\nSee `garage status` to view available nodes.");
	println!();
	println!(
		"Current cluster layout version: {}",
		layout.current().version
	);

	let has_role_changes = print_staging_role_changes(&layout);
	if has_role_changes {
		let v = layout.current().version;
		let res_apply = layout.apply_staged_changes(Some(v + 1));

		// this will print the stats of what partitions
		// will move around when we apply
		match res_apply {
			Ok((layout, msg)) => {
				println!();
				println!("==== NEW CLUSTER LAYOUT AFTER APPLYING CHANGES ====");
				print_cluster_layout(layout.current(), "No nodes have a role in the new layout.");
				println!();

				for line in msg.iter() {
					println!("{}", line);
				}
				println!("To enact the staged role changes, type:");
				println!();
				println!("    garage layout apply --version {}", v + 1);
				println!();
				println!("You can also revert all proposed changes with: garage layout revert");
			}
			Err(e) => {
				println!("Error while trying to compute the assignment: {}", e);
				println!("This new layout cannot yet be applied.");
				println!("You can also revert all proposed changes with: garage layout revert");
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

	println!("New cluster layout with updated role assignment has been applied in cluster.");
	println!("Data will now be moved around between nodes accordingly.");

	Ok(())
}

pub async fn cmd_revert_layout(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	revert_opt: RevertLayoutOpt,
) -> Result<(), Error> {
	if !revert_opt.yes {
		return Err(Error::Message(
			"Please add the --yes flag to run the layout revert operation".into(),
		));
	}

	let layout = fetch_layout(rpc_cli, rpc_host).await?;

	let layout = layout.revert_staged_changes()?;

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
		Some(r_str) => {
			let r = r_str
				.parse::<ZoneRedundancy>()
				.ok_or_message("invalid zone redundancy value")?;
			if let ZoneRedundancy::AtLeast(r_int) = r {
				if r_int > layout.current().replication_factor {
					return Err(Error::Message(format!(
						"The zone redundancy must be smaller or equal to the \
                    replication factor ({}).",
						layout.current().replication_factor
					)));
				} else if r_int < 1 {
					return Err(Error::Message(
						"The zone redundancy must be at least 1.".into(),
					));
				}
			}

			layout
				.staging
				.get_mut()
				.parameters
				.update(LayoutParameters { zone_redundancy: r });
			println!("The zone redundancy parameter has been set to '{}'.", r);
			did_something = true;
		}
	}

	if !did_something {
		return Err(Error::Message(
			"Please specify an action for `garage layout config`".into(),
		));
	}

	send_layout(rpc_cli, rpc_host, layout).await?;
	Ok(())
}

pub async fn cmd_layout_history(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
) -> Result<(), Error> {
	let layout = fetch_layout(rpc_cli, rpc_host).await?;
	let min_stored = layout.min_stored();

	println!("==== LAYOUT HISTORY ====");
	let mut table = vec!["Version\tStatus\tStorage nodes\tGateway nodes".to_string()];
	for ver in layout
		.versions
		.iter()
		.rev()
		.chain(layout.old_versions.iter().rev())
	{
		let status = if ver.version == layout.current().version {
			"current"
		} else if ver.version >= min_stored {
			"draining"
		} else {
			"historical"
		};
		table.push(format!(
			"#{}\t{}\t{}\t{}",
			ver.version,
			status,
			ver.roles
				.items()
				.iter()
				.filter(|(_, _, x)| matches!(x, NodeRoleV(Some(c)) if c.capacity.is_some()))
				.count(),
			ver.roles
				.items()
				.iter()
				.filter(|(_, _, x)| matches!(x, NodeRoleV(Some(c)) if c.capacity.is_none()))
				.count(),
		));
	}
	format_table(table);
	println!();

	if layout.versions.len() > 1 {
		println!("==== UPDATE TRACKERS ====");
		println!("Several layout versions are currently live in the cluster, and data is being migrated.");
		println!(
			"This is the internal data that Garage stores to know which nodes have what data."
		);
		println!();
		let mut table = vec!["Node\tAck\tSync\tSync_ack".to_string()];
		let all_nodes = layout.get_all_nodes();
		for node in all_nodes.iter() {
			table.push(format!(
				"{:?}\t#{}\t#{}\t#{}",
				node,
				layout.update_trackers.ack_map.get(node, min_stored),
				layout.update_trackers.sync_map.get(node, min_stored),
				layout.update_trackers.sync_ack_map.get(node, min_stored),
			));
		}
		table[1..].sort();
		format_table(table);

		let min_ack = layout
			.update_trackers
			.ack_map
			.min_among(&all_nodes, layout.min_stored());

		println!();
		println!(
			"If some nodes are not catching up to the latest layout version in the update trackers,"
		);
		println!("it might be because they are offline or unable to complete a sync successfully.");
		if min_ack < layout.current().version {
			println!(
				"You may force progress using `garage layout skip-dead-nodes --version {}`",
				layout.current().version
			);
		} else {
			println!(
				"You may force progress using `garage layout skip-dead-nodes --version {} --allow-missing-data`.",
				layout.current().version
			);
		}
	} else {
		println!("Your cluster is currently in a stable state with a single live layout version.");
		println!("No metadata migration is in progress. Note that the migration of data blocks is not tracked,");
		println!(
			"so you might want to keep old nodes online until their data directories become empty."
		);
	}

	Ok(())
}

pub async fn cmd_layout_skip_dead_nodes(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	opt: SkipDeadNodesOpt,
) -> Result<(), Error> {
	let status = fetch_status(rpc_cli, rpc_host).await?;
	let mut layout = fetch_layout(rpc_cli, rpc_host).await?;

	if layout.versions.len() == 1 {
		return Err(Error::Message(
			"This command cannot be called when there is only one live cluster layout version"
				.into(),
		));
	}

	let min_v = layout.min_stored();
	if opt.version <= min_v || opt.version > layout.current().version {
		return Err(Error::Message(format!(
			"Invalid version, you may use the following version numbers: {}",
			(min_v + 1..=layout.current().version)
				.map(|x| x.to_string())
				.collect::<Vec<_>>()
				.join(" ")
		)));
	}

	let all_nodes = layout.get_all_nodes();
	let mut did_something = false;
	for node in all_nodes.iter() {
		// Update ACK tracker for dead nodes or for all nodes if --allow-missing-data
		if opt.allow_missing_data || !status.iter().any(|x| x.id == *node && x.is_up) {
			if layout.update_trackers.ack_map.set_max(*node, opt.version) {
				println!("Increased the ACK tracker for node {:?}", node);
				did_something = true;
			}
		}

		// If --allow-missing-data, update SYNC tracker for all nodes.
		if opt.allow_missing_data {
			if layout.update_trackers.sync_map.set_max(*node, opt.version) {
				println!("Increased the SYNC tracker for node {:?}", node);
				did_something = true;
			}
		}
	}

	if did_something {
		send_layout(rpc_cli, rpc_host, layout).await?;
		println!("Success.");
		Ok(())
	} else if !opt.allow_missing_data {
		Err(Error::Message("Nothing was done, try passing the `--allow-missing-data` flag to force progress even when not enough nodes can complete a metadata sync.".into()))
	} else {
		Err(Error::Message(
			"Sorry, there is nothing I can do for you. Please wait patiently. If you ask for help, please send the output of the `garage layout history` command.".into(),
		))
	}
}

// --- utility ---

pub async fn fetch_layout(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
) -> Result<LayoutHistory, Error> {
	match rpc_cli
		.call(&rpc_host, SystemRpc::PullClusterLayout, PRIO_NORMAL)
		.await??
	{
		SystemRpc::AdvertiseClusterLayout(t) => Ok(t),
		resp => Err(Error::unexpected_rpc_message(resp)),
	}
}

pub async fn send_layout(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
	layout: LayoutHistory,
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

pub fn print_cluster_layout(layout: &LayoutVersion, empty_msg: &str) {
	let mut table = vec!["ID\tTags\tZone\tCapacity\tUsable capacity".to_string()];
	for (id, _, role) in layout.roles.items().iter() {
		let role = match &role.0 {
			Some(r) => r,
			_ => continue,
		};
		let tags = role.tags.join(",");
		let usage = layout.get_node_usage(id).unwrap_or(0);
		let capacity = layout.get_node_capacity(id).unwrap_or(0);
		if capacity > 0 {
			table.push(format!(
				"{:?}\t{}\t{}\t{}\t{} ({:.1}%)",
				id,
				tags,
				role.zone,
				role.capacity_string(),
				ByteSize::b(usage as u64 * layout.partition_size).to_string_as(false),
				(100.0 * usage as f32 * layout.partition_size as f32) / (capacity as f32)
			));
		} else {
			table.push(format!(
				"{:?}\t{}\t{}\t{}",
				id,
				tags,
				role.zone,
				role.capacity_string()
			));
		};
	}
	if table.len() > 1 {
		format_table(table);
		println!();
		println!("Zone redundancy: {}", layout.parameters.zone_redundancy);
	} else {
		println!("{}", empty_msg);
	}
}

pub fn print_staging_role_changes(layout: &LayoutHistory) -> bool {
	let staging = layout.staging.get();
	let has_role_changes = staging
		.roles
		.items()
		.iter()
		.any(|(k, _, v)| layout.current().roles.get(k) != Some(v));
	let has_layout_changes = *staging.parameters.get() != layout.current().parameters;

	if has_role_changes || has_layout_changes {
		println!();
		println!("==== STAGED ROLE CHANGES ====");
		if has_role_changes {
			let mut table = vec!["ID\tTags\tZone\tCapacity".to_string()];
			for (id, _, role) in staging.roles.items().iter() {
				if layout.current().roles.get(id) == Some(role) {
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
			println!();
		}
		if has_layout_changes {
			println!(
				"Zone redundancy: {}",
				staging.parameters.get().zone_redundancy
			);
		}
		true
	} else {
		false
	}
}
