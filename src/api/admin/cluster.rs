use std::sync::Arc;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use serde::{Serialize};

use hyper::{Body, Request, Response, StatusCode};

use garage_util::data::*;
use garage_util::error::Error as GarageError;
use garage_rpc::layout::*;
use garage_rpc::system::*;

use garage_model::garage::Garage;


use crate::error::*;


pub async fn handle_get_cluster_status(
	garage: &Arc<Garage>,
) -> Result<Response<Body>, Error> {
	let layout = garage.system.get_cluster_layout();

	let res = GetClusterStatusResponse {
		known_nodes: garage.system.get_known_nodes()
			.into_iter()
			.map(|i| (hex::encode(i.id), KnownNodeResp {
				addr: i.addr,
				is_up: i.is_up,
				last_seen_secs_ago: i.last_seen_secs_ago,
				hostname: i.status.hostname,
			}))
			.collect(),
		roles: layout.roles.items()
			.iter()
			.filter(|(_, _, v)| v.0.is_some())
			.map(|(k, _, v)| (hex::encode(k), v.0.clone()))
			.collect(),
		staged_role_changes: layout.staging.items()
			.iter()
			.filter(|(k, _, v)| layout.roles.get(k) != Some(v))
			.map(|(k, _, v)| (hex::encode(k), v.0.clone()))
			.collect(),
	};

	let resp_json = serde_json::to_string_pretty(&res).map_err(GarageError::from)?;
	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::from(resp_json))?)
}


#[derive(Serialize)]
struct GetClusterStatusResponse {
	#[serde(rename = "knownNodes")]
	known_nodes: HashMap<String, KnownNodeResp>,
	roles: HashMap<String, Option<NodeRole>>,
	#[serde(rename = "stagedRoleChanges")]
	staged_role_changes: HashMap<String, Option<NodeRole>>,
}

#[derive(Serialize)]
struct KnownNodeResp {
	addr: SocketAddr,
	is_up: bool,
	last_seen_secs_ago: Option<u64>,
	hostname: String,
}
