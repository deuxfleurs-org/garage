use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use hyper::{body::Incoming as IncomingBody, Request, Response};
use serde::{Deserialize, Serialize};

use garage_util::crdt::*;
use garage_util::data::*;

use garage_rpc::layout;

use garage_model::garage::Garage;

use garage_api_common::helpers::{json_ok_response, parse_json_body};

use crate::api_server::ResBody;
use crate::error::*;

pub async fn handle_get_cluster_status(garage: &Arc<Garage>) -> Result<Response<ResBody>, Error> {
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
					data_partition: i
						.status
						.data_disk_avail
						.map(|(avail, total)| FreeSpaceResp {
							available: avail,
							total,
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

	let res = GetClusterStatusResponse {
		node: hex::encode(garage.system.id),
		garage_version: garage_util::version::garage_version(),
		garage_features: garage_util::version::garage_features(),
		rust_version: garage_util::version::rust_version(),
		db_engine: garage.db.engine(),
		layout_version: layout.current().version,
		nodes,
	};

	Ok(json_ok_response(&res)?)
}

pub async fn handle_get_cluster_health(garage: &Arc<Garage>) -> Result<Response<ResBody>, Error> {
	use garage_rpc::system::ClusterHealthStatus;
	let health = garage.system.health();
	let health = ClusterHealth {
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
	Ok(json_ok_response(&health)?)
}

pub async fn handle_connect_cluster_nodes(
	garage: &Arc<Garage>,
	req: Request<IncomingBody>,
) -> Result<Response<ResBody>, Error> {
	let req = parse_json_body::<Vec<String>, _, Error>(req).await?;

	let res = futures::future::join_all(req.iter().map(|node| garage.system.connect(node)))
		.await
		.into_iter()
		.map(|r| match r {
			Ok(()) => ConnectClusterNodesResponse {
				success: true,
				error: None,
			},
			Err(e) => ConnectClusterNodesResponse {
				success: false,
				error: Some(format!("{}", e)),
			},
		})
		.collect::<Vec<_>>();

	Ok(json_ok_response(&res)?)
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

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterHealth {
	status: &'static str,
	known_nodes: usize,
	connected_nodes: usize,
	storage_nodes: usize,
	storage_nodes_ok: usize,
	partitions: usize,
	partitions_quorum: usize,
	partitions_all_ok: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetClusterStatusResponse {
	node: String,
	garage_version: &'static str,
	garage_features: Option<&'static [&'static str]>,
	rust_version: &'static str,
	db_engine: String,
	layout_version: u64,
	nodes: Vec<NodeResp>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ApplyClusterLayoutResponse {
	message: Vec<String>,
	layout: GetClusterLayoutResponse,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConnectClusterNodesResponse {
	success: bool,
	error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetClusterLayoutResponse {
	version: u64,
	roles: Vec<NodeRoleResp>,
	staged_role_changes: Vec<NodeRoleChange>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeRoleResp {
	id: String,
	zone: String,
	capacity: Option<u64>,
	tags: Vec<String>,
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct FreeSpaceResp {
	available: u64,
	total: u64,
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct NodeResp {
	id: String,
	role: Option<NodeRoleResp>,
	addr: Option<SocketAddr>,
	hostname: Option<String>,
	is_up: bool,
	last_seen_secs_ago: Option<u64>,
	draining: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	data_partition: Option<FreeSpaceResp>,
	#[serde(skip_serializing_if = "Option::is_none")]
	metadata_partition: Option<FreeSpaceResp>,
}

// ---- update functions ----

pub async fn handle_update_cluster_layout(
	garage: &Arc<Garage>,
	req: Request<IncomingBody>,
) -> Result<Response<ResBody>, Error> {
	let updates = parse_json_body::<UpdateClusterLayoutRequest, _, Error>(req).await?;

	let mut layout = garage.system.cluster_layout().inner().clone();

	let mut roles = layout.current().roles.clone();
	roles.merge(&layout.staging.get().roles);

	for change in updates {
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
	let param = parse_json_body::<ApplyLayoutRequest, _, Error>(req).await?;

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

// ----

type UpdateClusterLayoutRequest = Vec<NodeRoleChange>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApplyLayoutRequest {
	version: u64,
}

// ----

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeRoleChange {
	id: String,
	#[serde(flatten)]
	action: NodeRoleChangeEnum,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum NodeRoleChangeEnum {
	#[serde(rename_all = "camelCase")]
	Remove { remove: bool },
	#[serde(rename_all = "camelCase")]
	Update {
		zone: String,
		capacity: Option<u64>,
		tags: Vec<String>,
	},
}
