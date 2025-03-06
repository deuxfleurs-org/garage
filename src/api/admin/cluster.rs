use std::collections::HashMap;
use std::sync::Arc;

use garage_util::crdt::*;
use garage_util::data::*;

use garage_rpc::layout;

use garage_model::garage::Garage;

use crate::api::*;
use crate::error::*;
use crate::{Admin, RequestHandler};

impl RequestHandler for GetClusterStatusRequest {
	type Response = GetClusterStatusResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<GetClusterStatusResponse, Error> {
		let layout = garage.system.cluster_layout();
		let mut nodes = garage
			.system
			.get_known_nodes()
			.into_iter()
			.map(|i| {
				(
					i.id,
					NodeResp {
						id: hex::encode(i.id),
						addr: i.addr,
						hostname: i.status.hostname,
						is_up: i.is_up,
						last_seen_secs_ago: i.last_seen_secs_ago,
						data_partition: i.status.data_disk_avail.map(|(avail, total)| {
							FreeSpaceResp {
								available: avail,
								total,
							}
						}),
						metadata_partition: i.status.meta_disk_avail.map(|(avail, total)| {
							FreeSpaceResp {
								available: avail,
								total,
							}
						}),
						..Default::default()
					},
				)
			})
			.collect::<HashMap<_, _>>();

		for (id, _, role) in layout.current().roles.items().iter() {
			if let layout::NodeRoleV(Some(r)) = role {
				let role = NodeRoleResp {
					id: hex::encode(id),
					zone: r.zone.to_string(),
					capacity: r.capacity,
					tags: r.tags.clone(),
				};
				match nodes.get_mut(id) {
					None => {
						nodes.insert(
							*id,
							NodeResp {
								id: hex::encode(id),
								role: Some(role),
								..Default::default()
							},
						);
					}
					Some(n) => {
						n.role = Some(role);
					}
				}
			}
		}

		for ver in layout.versions().iter().rev().skip(1) {
			for (id, _, role) in ver.roles.items().iter() {
				if let layout::NodeRoleV(Some(r)) = role {
					if r.capacity.is_some() {
						if let Some(n) = nodes.get_mut(id) {
							if n.role.is_none() {
								n.draining = true;
							}
						} else {
							nodes.insert(
								*id,
								NodeResp {
									id: hex::encode(id),
									draining: true,
									..Default::default()
								},
							);
						}
					}
				}
			}
		}

		let mut nodes = nodes.into_values().collect::<Vec<_>>();
		nodes.sort_by(|x, y| x.id.cmp(&y.id));

		Ok(GetClusterStatusResponse {
			layout_version: layout.current().version,
			nodes,
		})
	}
}

impl RequestHandler for GetClusterHealthRequest {
	type Response = GetClusterHealthResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<GetClusterHealthResponse, Error> {
		use garage_rpc::system::ClusterHealthStatus;
		let health = garage.system.health();
		let health = GetClusterHealthResponse {
			status: match health.status {
				ClusterHealthStatus::Healthy => "healthy",
				ClusterHealthStatus::Degraded => "degraded",
				ClusterHealthStatus::Unavailable => "unavailable",
			}
			.to_string(),
			known_nodes: health.known_nodes,
			connected_nodes: health.connected_nodes,
			storage_nodes: health.storage_nodes,
			storage_nodes_ok: health.storage_nodes_ok,
			partitions: health.partitions,
			partitions_quorum: health.partitions_quorum,
			partitions_all_ok: health.partitions_all_ok,
		};
		Ok(health)
	}
}

impl RequestHandler for ConnectClusterNodesRequest {
	type Response = ConnectClusterNodesResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<ConnectClusterNodesResponse, Error> {
		let res = futures::future::join_all(self.0.iter().map(|node| garage.system.connect(node)))
			.await
			.into_iter()
			.map(|r| match r {
				Ok(()) => ConnectNodeResponse {
					success: true,
					error: None,
				},
				Err(e) => ConnectNodeResponse {
					success: false,
					error: Some(format!("{}", e)),
				},
			})
			.collect::<Vec<_>>();
		Ok(ConnectClusterNodesResponse(res))
	}
}

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
	let roles = layout
		.current()
		.roles
		.items()
		.iter()
		.filter_map(|(k, _, v)| v.0.clone().map(|x| (k, x)))
		.map(|(k, v)| NodeRoleResp {
			id: hex::encode(k),
			zone: v.zone.clone(),
			capacity: v.capacity,
			tags: v.tags.clone(),
		})
		.collect::<Vec<_>>();

	let staged_role_changes = layout
		.staging
		.get()
		.roles
		.items()
		.iter()
		.filter(|(k, _, v)| layout.current().roles.get(k) != Some(v))
		.map(|(k, _, v)| match &v.0 {
			None => NodeRoleChange {
				id: hex::encode(k),
				action: NodeRoleChangeEnum::Remove { remove: true },
			},
			Some(r) => NodeRoleChange {
				id: hex::encode(k),
				action: NodeRoleChangeEnum::Update {
					zone: r.zone.clone(),
					capacity: r.capacity,
					tags: r.tags.clone(),
				},
			},
		})
		.collect::<Vec<_>>();

	let staged_parameters = if *layout.staging.get().parameters.get() != layout.current().parameters
	{
		Some((*layout.staging.get().parameters.get()).into())
	} else {
		None
	};

	GetClusterLayoutResponse {
		version: layout.current().version,
		roles,
		parameters: layout.current().parameters.into(),
		staged_role_changes,
		staged_parameters,
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
				NodeRoleChangeEnum::Update {
					zone,
					capacity,
					tags,
				} => {
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

impl RequestHandler for ApplyClusterLayoutRequest {
	type Response = ApplyClusterLayoutResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<ApplyClusterLayoutResponse, Error> {
		let layout = garage.system.cluster_layout().inner().clone();
		let (layout, msg) = layout.apply_staged_changes(Some(self.version))?;

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
