use std::net::SocketAddr;
use std::sync::Arc;

use hyper::{Body, Request, Response};
use serde::{Deserialize, Serialize};

use garage_util::crdt::*;
use garage_util::data::*;

use garage_rpc::layout;

use garage_model::garage::Garage;

use crate::admin::error::*;
use crate::helpers::{json_ok_response, parse_json_body};

pub async fn handle_get_cluster_status(garage: &Arc<Garage>) -> Result<Response<Body>, Error> {
	let res = GetClusterStatusResponse {
		node: hex::encode(garage.system.id),
		garage_version: garage_util::version::garage_version(),
		garage_features: garage_util::version::garage_features(),
		rust_version: garage_util::version::rust_version(),
		db_engine: garage.db.engine(),
		known_nodes: garage
			.system
			.get_known_nodes()
			.into_iter()
			.map(|i| KnownNodeResp {
				id: hex::encode(i.id),
				addr: i.addr,
				is_up: i.is_up,
				last_seen_secs_ago: i.last_seen_secs_ago,
				hostname: i.status.hostname,
			})
			.collect(),
		layout: format_cluster_layout(&garage.system.cluster_layout()),
	};

	Ok(json_ok_response(&res)?)
}

pub async fn handle_get_cluster_health(garage: &Arc<Garage>) -> Result<Response<Body>, Error> {
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
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let req = parse_json_body::<Vec<String>>(req).await?;

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

pub async fn handle_get_cluster_layout(garage: &Arc<Garage>) -> Result<Response<Body>, Error> {
	let res = format_cluster_layout(&garage.system.cluster_layout());

	Ok(json_ok_response(&res)?)
}

fn format_cluster_layout(layout: &layout::ClusterLayout) -> GetClusterLayoutResponse {
	let roles = layout
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
		.staging_roles
		.items()
		.iter()
		.filter(|(k, _, v)| layout.roles.get(k) != Some(v))
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
		version: layout.version,
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
	known_nodes: Vec<KnownNodeResp>,
	layout: GetClusterLayoutResponse,
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct KnownNodeResp {
	id: String,
	addr: SocketAddr,
	is_up: bool,
	last_seen_secs_ago: Option<u64>,
	hostname: String,
}

// ---- update functions ----

pub async fn handle_update_cluster_layout(
	garage: &Arc<Garage>,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let updates = parse_json_body::<UpdateClusterLayoutRequest>(req).await?;

	let mut layout = garage.system.cluster_layout().as_ref().clone();

	let mut roles = layout.roles.clone();
	roles.merge(&layout.staging_roles);

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
			.staging_roles
			.merge(&roles.update_mutator(node, layout::NodeRoleV(new_role)));
	}

	garage.system.update_cluster_layout(&layout).await?;

	let res = format_cluster_layout(&layout);
	Ok(json_ok_response(&res)?)
}

pub async fn handle_apply_cluster_layout(
	garage: &Arc<Garage>,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let param = parse_json_body::<ApplyRevertLayoutRequest>(req).await?;

	let layout = garage.system.cluster_layout().as_ref().clone();
	let (layout, msg) = layout.apply_staged_changes(Some(param.version))?;

	garage.system.update_cluster_layout(&layout).await?;

	let res = ApplyClusterLayoutResponse {
		message: msg,
		layout: format_cluster_layout(&layout),
	};
	Ok(json_ok_response(&res)?)
}

pub async fn handle_revert_cluster_layout(
	garage: &Arc<Garage>,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let param = parse_json_body::<ApplyRevertLayoutRequest>(req).await?;

	let layout = garage.system.cluster_layout().as_ref().clone();
	let layout = layout.revert_staged_changes(Some(param.version))?;
	garage.system.update_cluster_layout(&layout).await?;

	let res = format_cluster_layout(&layout);
	Ok(json_ok_response(&res)?)
}

// ----

type UpdateClusterLayoutRequest = Vec<NodeRoleChange>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApplyRevertLayoutRequest {
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
