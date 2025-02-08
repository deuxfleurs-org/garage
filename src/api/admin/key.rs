use std::collections::HashMap;
use std::sync::Arc;

use hyper::{body::Incoming as IncomingBody, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use garage_table::*;

use garage_model::garage::Garage;
use garage_model::key_table::*;

use garage_api_common::helpers::*;

use crate::api_server::ResBody;
use crate::error::*;

pub async fn handle_list_keys(garage: &Arc<Garage>) -> Result<Response<ResBody>, Error> {
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

	Ok(json_ok_response(&res)?)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListKeyResultItem {
	id: String,
	name: String,
}

pub async fn handle_get_key_info(
	garage: &Arc<Garage>,
	id: Option<String>,
	search: Option<String>,
	show_secret_key: bool,
) -> Result<Response<ResBody>, Error> {
	let key = if let Some(id) = id {
		garage.key_helper().get_existing_key(&id).await?
	} else if let Some(search) = search {
		garage
			.key_helper()
			.get_existing_matching_key(&search)
			.await?
	} else {
		unreachable!();
	};

	key_info_results(garage, key, show_secret_key).await
}

pub async fn handle_create_key(
	garage: &Arc<Garage>,
	req: Request<IncomingBody>,
) -> Result<Response<ResBody>, Error> {
	let req = parse_json_body::<CreateKeyRequest, _, Error>(req).await?;

	let key = Key::new(req.name.as_deref().unwrap_or("Unnamed key"));
	garage.key_table.insert(&key).await?;

	key_info_results(garage, key, true).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateKeyRequest {
	name: Option<String>,
}

pub async fn handle_import_key(
	garage: &Arc<Garage>,
	req: Request<IncomingBody>,
) -> Result<Response<ResBody>, Error> {
	let req = parse_json_body::<ImportKeyRequest, _, Error>(req).await?;

	let prev_key = garage.key_table.get(&EmptyKey, &req.access_key_id).await?;
	if prev_key.is_some() {
		return Err(Error::KeyAlreadyExists(req.access_key_id.to_string()));
	}

	let imported_key = Key::import(
		&req.access_key_id,
		&req.secret_access_key,
		req.name.as_deref().unwrap_or("Imported key"),
	)
	.ok_or_bad_request("Invalid key format")?;
	garage.key_table.insert(&imported_key).await?;

	key_info_results(garage, imported_key, false).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ImportKeyRequest {
	access_key_id: String,
	secret_access_key: String,
	name: Option<String>,
}

pub async fn handle_update_key(
	garage: &Arc<Garage>,
	id: String,
	req: Request<IncomingBody>,
) -> Result<Response<ResBody>, Error> {
	let req = parse_json_body::<UpdateKeyRequest, _, Error>(req).await?;

	let mut key = garage.key_helper().get_existing_key(&id).await?;

	let key_state = key.state.as_option_mut().unwrap();

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

	key_info_results(garage, key, false).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateKeyRequest {
	name: Option<String>,
	allow: Option<KeyPerm>,
	deny: Option<KeyPerm>,
}

pub async fn handle_delete_key(
	garage: &Arc<Garage>,
	id: String,
) -> Result<Response<ResBody>, Error> {
	let helper = garage.locked_helper().await;

	let mut key = helper.key().get_existing_key(&id).await?;

	helper.delete_key(&mut key).await?;

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(empty_body())?)
}

async fn key_info_results(
	garage: &Arc<Garage>,
	key: Key,
	show_secret: bool,
) -> Result<Response<ResBody>, Error> {
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
		secret_access_key: if show_secret {
			Some(key_state.secret_key.clone())
		} else {
			None
		},
		permissions: KeyPerm {
			create_bucket: *key_state.allow_create_bucket.get(),
		},
		buckets: relevant_buckets
			.into_values()
			.map(|bucket| {
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

	Ok(json_ok_response(&res)?)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetKeyInfoResult {
	name: String,
	access_key_id: String,
	#[serde(skip_serializing_if = "is_default")]
	secret_access_key: Option<String>,
	permissions: KeyPerm,
	buckets: Vec<KeyInfoBucketResult>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KeyPerm {
	#[serde(default)]
	create_bucket: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct KeyInfoBucketResult {
	id: String,
	global_aliases: Vec<String>,
	local_aliases: Vec<String>,
	permissions: ApiBucketKeyPerm,
}

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApiBucketKeyPerm {
	#[serde(default)]
	pub(crate) read: bool,
	#[serde(default)]
	pub(crate) write: bool,
	#[serde(default)]
	pub(crate) owner: bool,
}
