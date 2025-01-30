//use bytesize::ByteSize;
//use format_table::format_table;

use garage_util::error::*;

use garage_api::admin::api::*;

use crate::cli::layout as cli_v1;
use crate::cli::structs::*;
use crate::cli_v2::util::*;
use crate::cli_v2::*;

impl Cli {
	pub async fn layout_command_dispatch(&self, cmd: LayoutOperation) -> Result<(), Error> {
		match cmd {
			LayoutOperation::Assign(assign_opt) => self.cmd_assign_role(assign_opt).await,
			LayoutOperation::Remove(remove_opt) => self.cmd_remove_role(remove_opt).await,
			LayoutOperation::Apply(apply_opt) => self.cmd_apply_layout(apply_opt).await,
			LayoutOperation::Revert(revert_opt) => self.cmd_revert_layout(revert_opt).await,

			// TODO
			LayoutOperation::Show => {
				cli_v1::cmd_show_layout(&self.system_rpc_endpoint, self.rpc_host).await
			}
			LayoutOperation::Config(config_opt) => {
				cli_v1::cmd_config_layout(&self.system_rpc_endpoint, self.rpc_host, config_opt)
					.await
			}
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

		self.api_request(UpdateClusterLayoutRequest(actions))
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

		self.api_request(UpdateClusterLayoutRequest(actions))
			.await?;

		println!("Role removal is staged but not yet committed.");
		println!("Use `garage layout show` to view staged role changes,");
		println!("and `garage layout apply` to enact staged changes.");
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
