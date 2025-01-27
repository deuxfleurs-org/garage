use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hyper::{body::Incoming as IncomingBody, Request, Response};

use garage_util::crdt::*;
use garage_util::data::*;

use garage_rpc::layout;

use garage_model::garage::Garage;

use crate::admin::api::{
	ApplyClusterLayoutRequest, ApplyClusterLayoutResponse, ConnectClusterNodeResponse,
	ConnectClusterNodesRequest, ConnectClusterNodesResponse, FreeSpaceResp,
	GetClusterHealthRequest, GetClusterHealthResponse, GetClusterLayoutResponse,
	GetClusterStatusRequest, GetClusterStatusResponse, NodeResp, NodeRoleChange,
	NodeRoleChangeEnum, NodeRoleResp, UpdateClusterLayoutRequest,
};
use crate::admin::api_server::ResBody;
use crate::admin::error::*;
use crate::admin::EndpointHandler;
use crate::helpers::{json_ok_response, parse_json_body};

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

pub async fn handle_connect_cluster_nodes(
	garage: &Arc<Garage>,
	req: Request<IncomingBody>,
) -> Result<Response<ResBody>, Error> {
	let req = parse_json_body::<ConnectClusterNodesRequest, _, Error>(req).await?;

	let res = req.handle(garage).await?;

	Ok(json_ok_response(&res)?)
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

pub async fn handle_get_cluster_layout(garage: &Arc<Garage>) -> Result<Response<ResBody>, Error> {
	let res = format_cluster_layout(garage.system.cluster_layout().inner());

	Ok(json_ok_response(&res)?)
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

pub async fn handle_update_cluster_layout(
	garage: &Arc<Garage>,
	req: Request<IncomingBody>,
) -> Result<Response<ResBody>, Error> {
	let updates = parse_json_body::<UpdateClusterLayoutRequest, _, Error>(req).await?;

	let mut layout = garage.system.cluster_layout().inner().clone();

	let mut roles = layout.current().roles.clone();
	roles.merge(&layout.staging.get().roles);

	for change in updates.0 {
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
	Ok(json_ok_response(&res)?)
}

pub async fn handle_apply_cluster_layout(
	garage: &Arc<Garage>,
	req: Request<IncomingBody>,
) -> Result<Response<ResBody>, Error> {
	let param = parse_json_body::<ApplyClusterLayoutRequest, _, Error>(req).await?;

	let layout = garage.system.cluster_layout().inner().clone();
	let (layout, msg) = layout.apply_staged_changes(Some(param.version))?;

	garage
		.system
		.layout_manager
		.update_cluster_layout(&layout)
		.await?;

	let res = ApplyClusterLayoutResponse {
		message: msg,
		layout: format_cluster_layout(&layout),
	};
	Ok(json_ok_response(&res)?)
}

pub async fn handle_revert_cluster_layout(
	garage: &Arc<Garage>,
) -> Result<Response<ResBody>, Error> {
	let layout = garage.system.cluster_layout().inner().clone();
	let layout = layout.revert_staged_changes()?;
	garage
		.system
		.layout_manager
		.update_cluster_layout(&layout)
		.await?;

	let res = format_cluster_layout(&layout);
	Ok(json_ok_response(&res)?)
}
