use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use hyper::{Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::error::Error as GarageError;

use garage_rpc::layout::*;

use garage_table::*;

use garage_model::garage::Garage;
use garage_model::key_table::*;

use crate::error::*;
use crate::helpers::*;

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
			.ok_or(Error::NoSuchKey)?
	} else if let Some(search) = search {
		garage
			.bucket_helper()
			.get_existing_matching_key(&search)
			.await
			.map_err(|_| Error::NoSuchKey)?
	} else {
		unreachable!();
	};

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
		permissions: KeyPermResult {
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
						.map(|p| KeyBucketPermResult {
							read: p.allow_read,
							write: p.allow_write,
							owner: p.allow_owner,
						})
						.unwrap_or(KeyBucketPermResult {
							read: false,
							write: false,
							owner: false,
						}),
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
	permissions: KeyPermResult,
	buckets: Vec<KeyInfoBucketResult>,
}

#[derive(Serialize)]
struct KeyPermResult {
	#[serde(rename = "createBucket")]
	create_bucket: bool,
}

#[derive(Serialize)]
struct KeyInfoBucketResult {
	id: String,
	#[serde(rename = "globalAliases")]
	global_aliases: Vec<String>,
	#[serde(rename = "localAliases")]
	local_aliases: Vec<String>,
	permissions: KeyBucketPermResult,
}

#[derive(Serialize)]
struct KeyBucketPermResult {
	read: bool,
	write: bool,
	owner: bool,
}
