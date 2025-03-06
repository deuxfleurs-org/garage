use bytesize::ByteSize;
use format_table::format_table;

use garage_util::error::*;

use garage_api_admin::api::*;
use garage_rpc::layout;

use crate::cli::layout as cli_v1;
use crate::cli::structs::*;
use crate::cli_v2::*;

impl Cli {
	pub async fn layout_command_dispatch(&self, cmd: LayoutOperation) -> Result<(), Error> {
		match cmd {
			LayoutOperation::Show => self.cmd_show_layout().await,
			LayoutOperation::Assign(assign_opt) => self.cmd_assign_role(assign_opt).await,
			LayoutOperation::Remove(remove_opt) => self.cmd_remove_role(remove_opt).await,
			LayoutOperation::Config(config_opt) => self.cmd_config_layout(config_opt).await,
			LayoutOperation::Apply(apply_opt) => self.cmd_apply_layout(apply_opt).await,
			LayoutOperation::Revert(revert_opt) => self.cmd_revert_layout(revert_opt).await,

			// TODO
			LayoutOperation::History => {
				cli_v1::cmd_layout_history(&self.system_rpc_endpoint, self.rpc_host).await
			}
			LayoutOperation::SkipDeadNodes(assume_sync_opt) => {
				cli_v1::cmd_layout_skip_dead_nodes(
					&self.system_rpc_endpoint,
					self.rpc_host,
					assume_sync_opt,
				)
				.await
			}
		}
	}

	pub async fn cmd_show_layout(&self) -> Result<(), Error> {
		let layout = self.api_request(GetClusterLayoutRequest).await?;

		println!("==== CURRENT CLUSTER LAYOUT ====");
		print_cluster_layout(&layout, "No nodes currently have a role in the cluster.\nSee `garage status` to view available nodes.");
		println!();
		println!("Current cluster layout version: {}", layout.version);

		let has_role_changes = print_staging_role_changes(&layout);
		if has_role_changes {
			let res_apply = self.api_request(PreviewClusterLayoutChangesRequest).await?;

			// this will print the stats of what partitions
			// will move around when we apply
			match res_apply {
				PreviewClusterLayoutChangesResponse::Success {
					message,
					new_layout,
				} => {
					println!();
					println!("==== NEW CLUSTER LAYOUT AFTER APPLYING CHANGES ====");
					print_cluster_layout(&new_layout, "No nodes have a role in the new layout.");
					println!();

					for line in message.iter() {
						println!("{}", line);
					}
					println!("To enact the staged role changes, type:");
					println!();
					println!("    garage layout apply --version {}", new_layout.version);
					println!();
					println!("You can also revert all proposed changes with: garage layout revert");
				}
				PreviewClusterLayoutChangesResponse::Error { error } => {
					println!("Error while trying to compute the assignment: {}", error);
					println!("This new layout cannot yet be applied.");
					println!("You can also revert all proposed changes with: garage layout revert");
				}
			}
		}

		Ok(())
	}

	pub async fn cmd_assign_role(&self, opt: AssignRoleOpt) -> Result<(), Error> {
		let status = self.api_request(GetClusterStatusRequest).await?;
		let layout = self.api_request(GetClusterLayoutRequest).await?;

		let all_node_ids_iter = status
			.nodes
			.iter()
			.map(|x| x.id.as_str())
			.chain(layout.roles.iter().map(|x| x.id.as_str()));

		let mut actions = vec![];

		for node in opt.replace.iter() {
			let id = find_matching_node(all_node_ids_iter.clone(), &node)?;

			actions.push(NodeRoleChange {
				id,
				action: NodeRoleChangeEnum::Remove { remove: true },
			});
		}

		for node in opt.node_ids.iter() {
			let id = find_matching_node(all_node_ids_iter.clone(), &node)?;

			let current = get_staged_or_current_role(&id, &layout);

			let zone = opt
				.zone
				.clone()
				.or_else(|| current.as_ref().map(|c| c.zone.clone()))
				.ok_or_message("Please specify a zone with the -z flag")?;

			let capacity = if opt.gateway {
				if opt.capacity.is_some() {
					return Err(Error::Message("Please specify only -c or -g".into()));
				}
				None
			} else if let Some(cap) = opt.capacity {
				Some(cap.as_u64())
			} else {
				current.as_ref().ok_or_message("Please specify a capacity with the -c flag, or set node explicitly as gateway with -g")?.capacity
			};

			let tags = if !opt.tags.is_empty() {
				opt.tags.clone()
			} else if let Some(cur) = current.as_ref() {
				cur.tags.clone()
			} else {
				vec![]
			};

			actions.push(NodeRoleChange {
				id,
				action: NodeRoleChangeEnum::Update {
					zone,
					capacity,
					tags,
				},
			});
		}

		self.api_request(UpdateClusterLayoutRequest {
			roles: actions,
			parameters: None,
		})
		.await?;

		println!("Role changes are staged but not yet committed.");
		println!("Use `garage layout show` to view staged role changes,");
		println!("and `garage layout apply` to enact staged changes.");
		Ok(())
	}

	pub async fn cmd_remove_role(&self, opt: RemoveRoleOpt) -> Result<(), Error> {
		let status = self.api_request(GetClusterStatusRequest).await?;
		let layout = self.api_request(GetClusterLayoutRequest).await?;

		let all_node_ids_iter = status
			.nodes
			.iter()
			.map(|x| x.id.as_str())
			.chain(layout.roles.iter().map(|x| x.id.as_str()));

		let id = find_matching_node(all_node_ids_iter.clone(), &opt.node_id)?;

		let actions = vec![NodeRoleChange {
			id,
			action: NodeRoleChangeEnum::Remove { remove: true },
		}];

		self.api_request(UpdateClusterLayoutRequest {
			roles: actions,
			parameters: None,
		})
		.await?;

		println!("Role removal is staged but not yet committed.");
		println!("Use `garage layout show` to view staged role changes,");
		println!("and `garage layout apply` to enact staged changes.");
		Ok(())
	}

	pub async fn cmd_config_layout(&self, config_opt: ConfigLayoutOpt) -> Result<(), Error> {
		let mut did_something = false;
		match config_opt.redundancy {
			None => (),
			Some(r_str) => {
				let r = r_str
					.parse::<layout::ZoneRedundancy>()
					.ok_or_message("invalid zone redundancy value")?;

				self.api_request(UpdateClusterLayoutRequest {
					roles: vec![],
					parameters: Some(LayoutParameters {
						zone_redundancy: r.into(),
					}),
				})
				.await?;
				println!("The zone redundancy parameter has been set to '{}'.", r);
				did_something = true;
			}
		}

		if !did_something {
			return Err(Error::Message(
				"Please specify an action for `garage layout config`".into(),
			));
		}

		Ok(())
	}

	pub async fn cmd_apply_layout(&self, apply_opt: ApplyLayoutOpt) -> Result<(), Error> {
		let missing_version_error = r#"
Please pass the new layout version number to ensure that you are writing the correct version of the cluster layout.
To know the correct value of the new layout version, invoke `garage layout show` and review the proposed changes.
        "#;

		let req = ApplyClusterLayoutRequest {
			version: apply_opt.version.ok_or_message(missing_version_error)?,
		};
		let res = self.api_request(req).await?;

		for line in res.message.iter() {
			println!("{}", line);
		}

		println!("New cluster layout with updated role assignment has been applied in cluster.");
		println!("Data will now be moved around between nodes accordingly.");

		Ok(())
	}

	pub async fn cmd_revert_layout(&self, revert_opt: RevertLayoutOpt) -> Result<(), Error> {
		if !revert_opt.yes {
			return Err(Error::Message(
				"Please add the --yes flag to run the layout revert operation".into(),
			));
		}

		self.api_request(RevertClusterLayoutRequest).await?;

		println!("All proposed role changes in cluster layout have been canceled.");
		Ok(())
	}
}

// --------------------------
// ---- helper functions ----
// --------------------------

pub fn capacity_string(v: Option<u64>) -> String {
	match v {
		Some(c) => ByteSize::b(c).to_string_as(false),
		None => "gateway".to_string(),
	}
}

pub fn get_staged_or_current_role(
	id: &str,
	layout: &GetClusterLayoutResponse,
) -> Option<NodeAssignedRole> {
	for node in layout.staged_role_changes.iter() {
		if node.id == id {
			return match &node.action {
				NodeRoleChangeEnum::Remove { .. } => None,
				NodeRoleChangeEnum::Update {
					zone,
					capacity,
					tags,
				} => Some(NodeAssignedRole {
					id: id.to_string(),
					zone: zone.to_string(),
					capacity: *capacity,
					tags: tags.clone(),
				}),
			};
		}
	}

	for node in layout.roles.iter() {
		if node.id == id {
			return Some(NodeAssignedRole {
				id: node.id.clone(),
				zone: node.zone.clone(),
				capacity: node.capacity,
				tags: node.tags.clone(),
			});
		}
	}

	None
}

pub fn find_matching_node<'a>(
	cand: impl std::iter::Iterator<Item = &'a str>,
	pattern: &'a str,
) -> Result<String, Error> {
	let mut candidates = vec![];
	for c in cand {
		if c.starts_with(pattern) && !candidates.contains(&c) {
			candidates.push(c);
		}
	}
	if candidates.len() != 1 {
		Err(Error::Message(format!(
			"{} nodes match '{}'",
			candidates.len(),
			pattern,
		)))
	} else {
		Ok(candidates[0].to_string())
	}
}

pub fn print_cluster_layout(layout: &GetClusterLayoutResponse, empty_msg: &str) {
	let mut table = vec!["ID\tTags\tZone\tCapacity\tUsable capacity".to_string()];
	for role in layout.roles.iter() {
		let tags = role.tags.join(",");
		if let (Some(capacity), Some(usable_capacity)) = (role.capacity, role.usable_capacity) {
			table.push(format!(
				"{:.16}\t{}\t{}\t{}\t{} ({:.1}%)",
				role.id,
				tags,
				role.zone,
				capacity_string(role.capacity),
				ByteSize::b(usable_capacity).to_string_as(false),
				(100.0 * usable_capacity as f32) / (capacity as f32)
			));
		} else {
			table.push(format!(
				"{:.16}\t{}\t{}\t{}",
				role.id,
				tags,
				role.zone,
				capacity_string(role.capacity),
			));
		};
	}
	if table.len() > 1 {
		format_table(table);
		println!();
		println!(
			"Zone redundancy: {}",
			Into::<layout::ZoneRedundancy>::into(layout.parameters.zone_redundancy)
		);
	} else {
		println!("{}", empty_msg);
	}
}

pub fn print_staging_role_changes(layout: &GetClusterLayoutResponse) -> bool {
	let has_role_changes = !layout.staged_role_changes.is_empty();

	let has_layout_changes = layout.staged_parameters.is_some();

	if has_role_changes || has_layout_changes {
		println!();
		println!("==== STAGED ROLE CHANGES ====");
		if has_role_changes {
			let mut table = vec!["ID\tTags\tZone\tCapacity".to_string()];
			for change in layout.staged_role_changes.iter() {
				match &change.action {
					NodeRoleChangeEnum::Update {
						tags,
						zone,
						capacity,
					} => {
						let tags = tags.join(",");
						table.push(format!(
							"{:.16}\t{}\t{}\t{}",
							change.id,
							tags,
							zone,
							capacity_string(*capacity),
						));
					}
					NodeRoleChangeEnum::Remove { .. } => {
						table.push(format!("{:.16}\tREMOVED", change.id));
					}
				}
			}
			format_table(table);
			println!();
		}
		if let Some(p) = layout.staged_parameters.as_ref() {
			println!(
				"Zone redundancy: {}",
				Into::<layout::ZoneRedundancy>::into(p.zone_redundancy)
			);
		}
		true
	} else {
		false
	}
}
