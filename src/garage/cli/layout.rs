use format_table::format_table;
use garage_util::error::*;

use garage_rpc::layout::*;
use garage_rpc::system::*;
use garage_rpc::*;

use crate::cli::structs::*;

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
