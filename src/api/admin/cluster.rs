use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use garage_util::crdt::*;
use garage_util::data::*;

use garage_rpc::layout;

use garage_model::garage::Garage;

use crate::admin::api::{
	ApplyClusterLayoutRequest, ApplyClusterLayoutResponse, ConnectClusterNodeResponse,
	ConnectClusterNodesRequest, ConnectClusterNodesResponse, FreeSpaceResp,
	GetClusterHealthRequest, GetClusterHealthResponse, GetClusterLayoutRequest,
	GetClusterLayoutResponse, GetClusterStatusRequest, GetClusterStatusResponse, NodeResp,
	NodeRoleChange, NodeRoleChangeEnum, NodeRoleResp, RevertClusterLayoutRequest,
	RevertClusterLayoutResponse, UpdateClusterLayoutRequest, UpdateClusterLayoutResponse,
};
use crate::admin::error::*;
use crate::admin::EndpointHandler;

#[async_trait]
impl EndpointHandler for GetClusterStatusRequest {
	type Response = GetClusterStatusResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<GetClusterStatusResponse, Error> {
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
			node: hex::encode(garage.system.id),
			garage_version: garage_util::version::garage_version(),
			garage_features: garage_util::version::garage_features(),
			rust_version: garage_util::version::rust_version(),
			db_engine: garage.db.engine(),
			layout_version: layout.current().version,
			nodes,
		})
	}
}

#[async_trait]
impl EndpointHandler for GetClusterHealthRequest {
	type Response = GetClusterHealthResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<GetClusterHealthResponse, Error> {
		use garage_rpc::system::ClusterHealthStatus;
		let health = garage.system.health();
		let health = GetClusterHealthResponse {
			status: match health.status {
				ClusterHealthStatus::Healthy => "healthy",
				ClusterHealthStatus::Degraded => "degraded",
				ClusterHealthStatus::Unavailable => "unavailable",
			},
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

#[async_trait]
impl EndpointHandler for ConnectClusterNodesRequest {
	type Response = ConnectClusterNodesResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<ConnectClusterNodesResponse, Error> {
		let res = futures::future::join_all(self.0.iter().map(|node| garage.system.connect(node)))
			.await
			.into_iter()
			.map(|r| match r {
				Ok(()) => ConnectClusterNodeResponse {
					success: true,
					error: None,
				},
				Err(e) => ConnectClusterNodeResponse {
					success: false,
					error: Some(format!("{}", e)),
				},
			})
			.collect::<Vec<_>>();
		Ok(ConnectClusterNodesResponse(res))
	}
}

#[async_trait]
impl EndpointHandler for GetClusterLayoutRequest {
	type Response = GetClusterLayoutResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<GetClusterLayoutResponse, Error> {
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

	GetClusterLayoutResponse {
		version: layout.current().version,
		roles,
		staged_role_changes,
	}
}

// ----

// ---- update functions ----

#[async_trait]
impl EndpointHandler for UpdateClusterLayoutRequest {
	type Response = UpdateClusterLayoutResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<UpdateClusterLayoutResponse, Error> {
		let mut layout = garage.system.cluster_layout().inner().clone();

		let mut roles = layout.current().roles.clone();
		roles.merge(&layout.staging.get().roles);

		for change in self.0 {
			let node = hex::decode(&change.id).ok_or_bad_request("Invalid node identifier")?;
			let node = Uuid::try_from(&node).ok_or_bad_request("Invalid node identifier")?;

			let new_role = match change.action {
				NodeRoleChangeEnum::Remove { remove: true } => None,
				NodeRoleChangeEnum::Update {
					zone,
					capacity,
					tags,
				} => Some(layout::NodeRole {
					zone,
					capacity,
					tags,
				}),
				_ => return Err(Error::bad_request("Invalid layout change")),
			};

			layout
				.staging
				.get_mut()
				.roles
				.merge(&roles.update_mutator(node, layout::NodeRoleV(new_role)));
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

#[async_trait]
impl EndpointHandler for ApplyClusterLayoutRequest {
	type Response = ApplyClusterLayoutResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<ApplyClusterLayoutResponse, Error> {
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

#[async_trait]
impl EndpointHandler for RevertClusterLayoutRequest {
	type Response = RevertClusterLayoutResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<RevertClusterLayoutResponse, Error> {
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
