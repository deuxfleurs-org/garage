use std::collections::HashMap;
use std::sync::Arc;

use hyper::{Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use garage_util::error::Error as GarageError;

use garage_table::*;

use garage_model::garage::Garage;
use garage_model::key_table::*;

use crate::admin::error::*;
use crate::helpers::parse_json_body;

pub async fn handle_list_keys(garage: &Arc<Garage>) -> Result<Response<Body>, Error> {
	let res = garage
		.key_table
		.get_range(
			&EmptyKey,
			None,
			Some(KeyFilter::Deleted(DeletedFilter::NotDeleted)),
			10000,
			EnumerationOrder::Forward,
		)
		.await?
		.iter()
		.map(|k| ListKeyResultItem {
			id: k.key_id.to_string(),
			name: k.params().unwrap().name.get().clone(),
		})
		.collect::<Vec<_>>();

	let resp_json = serde_json::to_string_pretty(&res).map_err(GarageError::from)?;
	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::from(resp_json))?)
}

#[derive(Serialize)]
struct ListKeyResultItem {
	id: String,
	name: String,
}

pub async fn handle_get_key_info(
	garage: &Arc<Garage>,
	id: Option<String>,
	search: Option<String>,
) -> Result<Response<Body>, Error> {
	let key = if let Some(id) = id {
		garage
			.key_table
			.get(&EmptyKey, &id)
			.await?
			.ok_or(Error::NoSuchAccessKey)?
	} else if let Some(search) = search {
		garage
			.key_helper()
			.get_existing_matching_key(&search)
			.await
			.map_err(|_| Error::NoSuchAccessKey)?
	} else {
		unreachable!();
	};

	key_info_results(garage, key).await
}

pub async fn handle_create_key(
	garage: &Arc<Garage>,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let req = parse_json_body::<CreateKeyRequest>(req).await?;

	let key = Key::new(&req.name);
	garage.key_table.insert(&key).await?;

	key_info_results(garage, key).await
}

#[derive(Deserialize)]
struct CreateKeyRequest {
	name: String,
}

pub async fn handle_update_key(
	garage: &Arc<Garage>,
	id: String,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let req = parse_json_body::<UpdateKeyRequest>(req).await?;

	let mut key = garage
		.key_table
		.get(&EmptyKey, &id)
		.await?
		.ok_or(Error::NoSuchAccessKey)?;

	let key_state = key.state.as_option_mut().ok_or(Error::NoSuchAccessKey)?;

	if let Some(new_name) = req.name {
		key_state.name.update(new_name);
	}
	if let Some(allow) = req.allow {
		if allow.create_bucket {
			key_state.allow_create_bucket.update(true);
		}
	}
	if let Some(deny) = req.deny {
		if deny.create_bucket {
			key_state.allow_create_bucket.update(false);
		}
	}

	garage.key_table.insert(&key).await?;

	key_info_results(garage, key).await
}

#[derive(Deserialize)]
struct UpdateKeyRequest {
	name: Option<String>,
	allow: Option<KeyPerm>,
	deny: Option<KeyPerm>,
}

pub async fn handle_delete_key(garage: &Arc<Garage>, id: String) -> Result<Response<Body>, Error> {
	let mut key = garage
		.key_table
		.get(&EmptyKey, &id)
		.await?
		.ok_or(Error::NoSuchAccessKey)?;

	key.state.as_option().ok_or(Error::NoSuchAccessKey)?;

	garage.key_helper().delete_key(&mut key).await?;

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(Body::empty())?)
}

async fn key_info_results(garage: &Arc<Garage>, key: Key) -> Result<Response<Body>, Error> {
	let mut relevant_buckets = HashMap::new();

	let key_state = key.state.as_option().unwrap();

	for id in key_state
		.authorized_buckets
		.items()
		.iter()
		.map(|(id, _)| id)
		.chain(
			key_state
				.local_aliases
				.items()
				.iter()
				.filter_map(|(_, _, v)| v.as_ref()),
		) {
		if !relevant_buckets.contains_key(id) {
			if let Some(b) = garage.bucket_table.get(&EmptyKey, id).await? {
				if b.state.as_option().is_some() {
					relevant_buckets.insert(*id, b);
				}
			}
		}
	}

	let res = GetKeyInfoResult {
		name: key_state.name.get().clone(),
		access_key_id: key.key_id.clone(),
		secret_access_key: key_state.secret_key.clone(),
		permissions: KeyPerm {
			create_bucket: *key_state.allow_create_bucket.get(),
		},
		buckets: relevant_buckets
			.into_iter()
			.map(|(_, bucket)| {
				let state = bucket.state.as_option().unwrap();
				KeyInfoBucketResult {
					id: hex::encode(bucket.id),
					global_aliases: state
						.aliases
						.items()
						.iter()
						.filter(|(_, _, a)| *a)
						.map(|(n, _, _)| n.to_string())
						.collect::<Vec<_>>(),
					local_aliases: state
						.local_aliases
						.items()
						.iter()
						.filter(|((k, _), _, a)| *a && *k == key.key_id)
						.map(|((_, n), _, _)| n.to_string())
						.collect::<Vec<_>>(),
					permissions: key_state
						.authorized_buckets
						.get(&bucket.id)
						.map(|p| ApiBucketKeyPerm {
							read: p.allow_read,
							write: p.allow_write,
							owner: p.allow_owner,
						})
						.unwrap_or_default(),
				}
			})
			.collect::<Vec<_>>(),
	};

	let resp_json = serde_json::to_string_pretty(&res).map_err(GarageError::from)?;
	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::from(resp_json))?)
}

#[derive(Serialize)]
struct GetKeyInfoResult {
	name: String,
	#[serde(rename = "accessKeyId")]
	access_key_id: String,
	#[serde(rename = "secretAccessKey")]
	secret_access_key: String,
	permissions: KeyPerm,
	buckets: Vec<KeyInfoBucketResult>,
}

#[derive(Serialize, Deserialize)]
struct KeyPerm {
	#[serde(rename = "createBucket", default)]
	create_bucket: bool,
}

#[derive(Serialize)]
struct KeyInfoBucketResult {
	id: String,
	#[serde(rename = "globalAliases")]
	global_aliases: Vec<String>,
	#[serde(rename = "localAliases")]
	local_aliases: Vec<String>,
	permissions: ApiBucketKeyPerm,
}

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct ApiBucketKeyPerm {
	#[serde(default)]
	pub(crate) read: bool,
	#[serde(default)]
	pub(crate) write: bool,
	#[serde(default)]
	pub(crate) owner: bool,
}
