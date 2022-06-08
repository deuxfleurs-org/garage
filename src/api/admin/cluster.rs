use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use hyper::{Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use garage_util::crdt::*;
use garage_util::data::*;

use garage_rpc::layout::*;

use garage_model::garage::Garage;

use crate::admin::error::*;
use crate::helpers::{json_ok_response, parse_json_body};

pub async fn handle_get_cluster_status(garage: &Arc<Garage>) -> Result<Response<Body>, Error> {
	let res = GetClusterStatusResponse {
		node: hex::encode(garage.system.id),
		garage_version: garage.system.garage_version(),
		db_engine: garage.db.engine(),
		known_nodes: garage
			.system
			.get_known_nodes()
			.into_iter()
			.map(|i| {
				(
					hex::encode(i.id),
					KnownNodeResp {
						addr: i.addr,
						is_up: i.is_up,
						last_seen_secs_ago: i.last_seen_secs_ago,
						hostname: i.status.hostname,
					},
				)
			})
			.collect(),
		layout: get_cluster_layout(garage),
	};

	Ok(json_ok_response(&res)?)
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
	let res = get_cluster_layout(garage);

	Ok(json_ok_response(&res)?)
}

fn get_cluster_layout(garage: &Arc<Garage>) -> GetClusterLayoutResponse {
	let layout = garage.system.get_cluster_layout();

	GetClusterLayoutResponse {
		version: layout.version,
		roles: layout
			.roles
			.items()
			.iter()
			.filter(|(_, _, v)| v.0.is_some())
			.map(|(k, _, v)| (hex::encode(k), v.0.clone()))
			.collect(),
		staged_role_changes: layout
			.staging
			.items()
			.iter()
			.filter(|(k, _, v)| layout.roles.get(k) != Some(v))
			.map(|(k, _, v)| (hex::encode(k), v.0.clone()))
			.collect(),
	}
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetClusterStatusResponse {
	node: String,
	garage_version: &'static str,
	db_engine: String,
	known_nodes: HashMap<String, KnownNodeResp>,
	layout: GetClusterLayoutResponse,
}

#[derive(Serialize)]
struct ConnectClusterNodesResponse {
	success: bool,
	error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetClusterLayoutResponse {
	version: u64,
	roles: HashMap<String, Option<NodeRole>>,
	staged_role_changes: HashMap<String, Option<NodeRole>>,
}

#[derive(Serialize)]
struct KnownNodeResp {
	addr: SocketAddr,
	is_up: bool,
	last_seen_secs_ago: Option<u64>,
	hostname: String,
}

pub async fn handle_update_cluster_layout(
	garage: &Arc<Garage>,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let updates = parse_json_body::<UpdateClusterLayoutRequest>(req).await?;

	let mut layout = garage.system.get_cluster_layout();

	let mut roles = layout.roles.clone();
	roles.merge(&layout.staging);

	for (node, role) in updates {
		let node = hex::decode(node).ok_or_bad_request("Invalid node identifier")?;
		let node = Uuid::try_from(&node).ok_or_bad_request("Invalid node identifier")?;

		layout
			.staging
			.merge(&roles.update_mutator(node, NodeRoleV(role)));
	}

	garage.system.update_cluster_layout(&layout).await?;

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::empty())?)
}

pub async fn handle_apply_cluster_layout(
	garage: &Arc<Garage>,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let param = parse_json_body::<ApplyRevertLayoutRequest>(req).await?;

	let layout = garage.system.get_cluster_layout();
	let layout = layout.apply_staged_changes(Some(param.version))?;
	garage.system.update_cluster_layout(&layout).await?;

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::empty())?)
}

pub async fn handle_revert_cluster_layout(
	garage: &Arc<Garage>,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let param = parse_json_body::<ApplyRevertLayoutRequest>(req).await?;

	let layout = garage.system.get_cluster_layout();
	let layout = layout.revert_staged_changes(Some(param.version))?;
	garage.system.update_cluster_layout(&layout).await?;

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::empty())?)
}

type UpdateClusterLayoutRequest = HashMap<String, Option<NodeRole>>;

#[derive(Deserialize)]
struct ApplyRevertLayoutRequest {
	version: u64,
}
