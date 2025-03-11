use std::sync::Arc;

use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::error::Error as GarageError;

use garage_rpc::layout;

use garage_model::garage::Garage;

use crate::api::*;
use crate::error::*;
use crate::{Admin, RequestHandler};

impl RequestHandler for GetClusterLayoutRequest {
	type Response = GetClusterLayoutResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<GetClusterLayoutResponse, Error> {
		Ok(format_cluster_layout(
			garage.system.cluster_layout().inner(),
		))
	}
}

fn format_cluster_layout(layout: &layout::LayoutHistory) -> GetClusterLayoutResponse {
	let current = layout.current();

	let roles = current
		.roles
		.items()
		.iter()
		.filter_map(|(k, _, v)| v.0.clone().map(|x| (k, x)))
		.map(|(k, v)| {
			let stored_partitions = current.get_node_usage(k).ok().map(|x| x as u64);
			LayoutNodeRole {
				id: hex::encode(k),
				zone: v.zone.clone(),
				capacity: v.capacity,
				stored_partitions,
				usable_capacity: stored_partitions.map(|x| x * current.partition_size),
				tags: v.tags.clone(),
			}
		})
		.collect::<Vec<_>>();

	let staged_role_changes = layout
		.staging
		.get()
		.roles
		.items()
		.iter()
		.filter(|(k, _, v)| current.roles.get(k) != Some(v))
		.map(|(k, _, v)| match &v.0 {
			None => NodeRoleChange {
				id: hex::encode(k),
				action: NodeRoleChangeEnum::Remove { remove: true },
			},
			Some(r) => NodeRoleChange {
				id: hex::encode(k),
				action: NodeRoleChangeEnum::Update(NodeAssignedRole {
					zone: r.zone.clone(),
					capacity: r.capacity,
					tags: r.tags.clone(),
				}),
			},
		})
		.collect::<Vec<_>>();

	let staged_parameters = if *layout.staging.get().parameters.get() != current.parameters {
		Some((*layout.staging.get().parameters.get()).into())
	} else {
		None
	};

	GetClusterLayoutResponse {
		version: current.version,
		roles,
		partition_size: current.partition_size,
		parameters: current.parameters.into(),
		staged_role_changes,
		staged_parameters,
	}
}

impl RequestHandler for GetClusterLayoutHistoryRequest {
	type Response = GetClusterLayoutHistoryResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<GetClusterLayoutHistoryResponse, Error> {
		let layout_helper = garage.system.cluster_layout();
		let layout = layout_helper.inner();
		let min_stored = layout.min_stored();

		let versions = layout
			.versions
			.iter()
			.rev()
			.chain(layout.old_versions.iter().rev())
			.map(|ver| {
				let status = if ver.version == layout.current().version {
					ClusterLayoutVersionStatus::Current
				} else if ver.version >= min_stored {
					ClusterLayoutVersionStatus::Draining
				} else {
					ClusterLayoutVersionStatus::Historical
				};
				ClusterLayoutVersion {
					version: ver.version,
					status,
					storage_nodes: ver
						.roles
						.items()
						.iter()
						.filter(
							|(_, _, x)| matches!(x, layout::NodeRoleV(Some(c)) if c.capacity.is_some()),
						)
						.count() as u64,
					gateway_nodes: ver
						.roles
						.items()
						.iter()
						.filter(
							|(_, _, x)| matches!(x, layout::NodeRoleV(Some(c)) if c.capacity.is_none()),
						)
						.count() as u64,
				}
			})
			.collect::<Vec<_>>();

		let all_nodes = layout.get_all_nodes();
		let min_ack = layout_helper.ack_map_min();

		let update_trackers = if layout.versions.len() > 1 {
			Some(
				all_nodes
					.iter()
					.map(|node| {
						(
							hex::encode(&node),
							NodeUpdateTrackers {
								ack: layout.update_trackers.ack_map.get(node, min_stored),
								sync: layout.update_trackers.sync_map.get(node, min_stored),
								sync_ack: layout.update_trackers.sync_ack_map.get(node, min_stored),
							},
						)
					})
					.collect(),
			)
		} else {
			None
		};

		Ok(GetClusterLayoutHistoryResponse {
			current_version: layout.current().version,
			min_ack,
			versions,
			update_trackers,
		})
	}
}

// ----

// ---- update functions ----

impl RequestHandler for UpdateClusterLayoutRequest {
	type Response = UpdateClusterLayoutResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<UpdateClusterLayoutResponse, Error> {
		let mut layout = garage.system.cluster_layout().inner().clone();

		let mut roles = layout.current().roles.clone();
		roles.merge(&layout.staging.get().roles);

		for change in self.roles {
			let node = hex::decode(&change.id).ok_or_bad_request("Invalid node identifier")?;
			let node = Uuid::try_from(&node).ok_or_bad_request("Invalid node identifier")?;

			let new_role = match change.action {
				NodeRoleChangeEnum::Remove { remove: true } => None,
				NodeRoleChangeEnum::Update(NodeAssignedRole {
					zone,
					capacity,
					tags,
				}) => {
					if matches!(capacity, Some(cap) if cap < 1024) {
						return Err(Error::bad_request("Capacity should be at least 1K (1024)"));
					}
					Some(layout::NodeRole {
						zone,
						capacity,
						tags,
					})
				}
				_ => return Err(Error::bad_request("Invalid layout change")),
			};

			layout
				.staging
				.get_mut()
				.roles
				.merge(&roles.update_mutator(node, layout::NodeRoleV(new_role)));
		}

		if let Some(param) = self.parameters {
			if let ZoneRedundancy::AtLeast(r_int) = param.zone_redundancy {
				if r_int > layout.current().replication_factor {
					return Err(Error::bad_request(format!(
						"The zone redundancy must be smaller or equal to the replication factor ({}).",
						layout.current().replication_factor
					)));
				} else if r_int < 1 {
					return Err(Error::bad_request(
						"The zone redundancy must be at least 1.",
					));
				}
			}
			layout.staging.get_mut().parameters.update(param.into());
		}

		garage
			.system
			.layout_manager
			.update_cluster_layout(&layout)
			.await?;

		let res = format_cluster_layout(&layout);
		Ok(UpdateClusterLayoutResponse(res))
	}
}

impl RequestHandler for PreviewClusterLayoutChangesRequest {
	type Response = PreviewClusterLayoutChangesResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<PreviewClusterLayoutChangesResponse, Error> {
		let layout = garage.system.cluster_layout().inner().clone();
		let new_ver = layout.current().version + 1;
		match layout.apply_staged_changes(new_ver) {
			Err(GarageError::Message(error)) => {
				Ok(PreviewClusterLayoutChangesResponse::Error { error })
			}
			Err(e) => Err(e.into()),
			Ok((new_layout, msg)) => Ok(PreviewClusterLayoutChangesResponse::Success {
				message: msg,
				new_layout: format_cluster_layout(&new_layout),
			}),
		}
	}
}

impl RequestHandler for ApplyClusterLayoutRequest {
	type Response = ApplyClusterLayoutResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<ApplyClusterLayoutResponse, Error> {
		let layout = garage.system.cluster_layout().inner().clone();
		let (layout, msg) = layout.apply_staged_changes(self.version)?;

		garage
			.system
			.layout_manager
			.update_cluster_layout(&layout)
			.await?;

		Ok(ApplyClusterLayoutResponse {
			message: msg,
			layout: format_cluster_layout(&layout),
		})
	}
}

impl RequestHandler for RevertClusterLayoutRequest {
	type Response = RevertClusterLayoutResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<RevertClusterLayoutResponse, Error> {
		let layout = garage.system.cluster_layout().inner().clone();
		let layout = layout.revert_staged_changes()?;
		garage
			.system
			.layout_manager
			.update_cluster_layout(&layout)
			.await?;

		let res = format_cluster_layout(&layout);
		Ok(RevertClusterLayoutResponse(res))
	}
}

impl RequestHandler for ClusterLayoutSkipDeadNodesRequest {
	type Response = ClusterLayoutSkipDeadNodesResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<ClusterLayoutSkipDeadNodesResponse, Error> {
		let status = garage.system.get_known_nodes();

		let mut layout = garage.system.cluster_layout().inner().clone();
		let mut ack_updated = vec![];
		let mut sync_updated = vec![];

		if layout.versions.len() == 1 {
			return Err(Error::bad_request(
				"This command cannot be called when there is only one live cluster layout version",
			));
		}

		let min_v = layout.min_stored();
		if self.version <= min_v || self.version > layout.current().version {
			return Err(Error::bad_request(format!(
				"Invalid version, you may use the following version numbers: {}",
				(min_v + 1..=layout.current().version)
					.map(|x| x.to_string())
					.collect::<Vec<_>>()
					.join(" ")
			)));
		}

		let all_nodes = layout.get_all_nodes();
		for node in all_nodes.iter() {
			// Update ACK tracker for dead nodes or for all nodes if --allow-missing-data
			if self.allow_missing_data || !status.iter().any(|x| x.id == *node && x.is_up) {
				if layout.update_trackers.ack_map.set_max(*node, self.version) {
					ack_updated.push(hex::encode(node));
				}
			}

			// If --allow-missing-data, update SYNC tracker for all nodes.
			if self.allow_missing_data {
				if layout.update_trackers.sync_map.set_max(*node, self.version) {
					sync_updated.push(hex::encode(node));
				}
			}
		}

		garage
			.system
			.layout_manager
			.update_cluster_layout(&layout)
			.await?;

		Ok(ClusterLayoutSkipDeadNodesResponse {
			ack_updated,
			sync_updated,
		})
	}
}

// ----

impl From<layout::ZoneRedundancy> for ZoneRedundancy {
	fn from(x: layout::ZoneRedundancy) -> Self {
		match x {
			layout::ZoneRedundancy::Maximum => ZoneRedundancy::Maximum,
			layout::ZoneRedundancy::AtLeast(x) => ZoneRedundancy::AtLeast(x),
		}
	}
}

impl Into<layout::ZoneRedundancy> for ZoneRedundancy {
	fn into(self) -> layout::ZoneRedundancy {
		match self {
			ZoneRedundancy::Maximum => layout::ZoneRedundancy::Maximum,
			ZoneRedundancy::AtLeast(x) => layout::ZoneRedundancy::AtLeast(x),
		}
	}
}

impl From<layout::LayoutParameters> for LayoutParameters {
	fn from(x: layout::LayoutParameters) -> Self {
		LayoutParameters {
			zone_redundancy: x.zone_redundancy.into(),
		}
	}
}

impl Into<layout::LayoutParameters> for LayoutParameters {
	fn into(self) -> layout::LayoutParameters {
		layout::LayoutParameters {
			zone_redundancy: self.zone_redundancy.into(),
		}
	}
}
