use std::collections::HashMap;
use std::sync::Arc;

use hyper::{Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::error::Error as GarageError;

use garage_table::*;

use garage_model::bucket_alias_table::*;
use garage_model::bucket_table::*;
use garage_model::garage::Garage;
use garage_model::permission::*;
use garage_model::s3::object_table::ObjectFilter;

use crate::admin::key::ApiBucketKeyPerm;
use crate::error::*;
use crate::helpers::*;

pub async fn handle_list_buckets(garage: &Arc<Garage>) -> Result<Response<Body>, Error> {
	let buckets = garage
		.bucket_table
		.get_range(
			&EmptyKey,
			None,
			Some(DeletedFilter::NotDeleted),
			10000,
			EnumerationOrder::Forward,
		)
		.await?;

	let res = buckets
		.into_iter()
		.map(|b| {
			let state = b.state.as_option().unwrap();
			ListBucketResultItem {
				id: hex::encode(b.id),
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
					.filter(|(_, _, a)| *a)
					.map(|((k, n), _, _)| BucketLocalAlias {
						access_key_id: k.to_string(),
						alias: n.to_string(),
					})
					.collect::<Vec<_>>(),
			}
		})
		.collect::<Vec<_>>();

	let resp_json = serde_json::to_string_pretty(&res).map_err(GarageError::from)?;
	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::from(resp_json))?)
}

#[derive(Serialize)]
struct ListBucketResultItem {
	id: String,
	#[serde(rename = "globalAliases")]
	global_aliases: Vec<String>,
	#[serde(rename = "localAliases")]
	local_aliases: Vec<BucketLocalAlias>,
}

#[derive(Serialize)]
struct BucketLocalAlias {
	#[serde(rename = "accessKeyId")]
	access_key_id: String,
	alias: String,
}

pub async fn handle_get_bucket_info(
	garage: &Arc<Garage>,
	id: Option<String>,
	global_alias: Option<String>,
) -> Result<Response<Body>, Error> {
	let bucket_id = match (id, global_alias) {
		(Some(id), None) => {
			let id_hex = hex::decode(&id).ok_or_bad_request("Invalid bucket id")?;
			Uuid::try_from(&id_hex).ok_or_bad_request("Invalid bucket id")?
		}
		(None, Some(ga)) => garage
			.bucket_helper()
			.resolve_global_bucket_name(&ga)
			.await?
			.ok_or_bad_request("Bucket not found")?,
		_ => {
			return Err(Error::BadRequest(
				"Either id or globalAlias must be provided (but not both)".into(),
			))
		}
	};

	let bucket = garage
		.bucket_helper()
		.get_existing_bucket(bucket_id)
		.await?;

	bucket_info_results(garage, bucket).await
}

async fn bucket_info_results(
	garage: &Arc<Garage>,
	bucket: Bucket,
) -> Result<Response<Body>, Error> {
	let mut relevant_keys = HashMap::new();
	for (k, _) in bucket
		.state
		.as_option()
		.unwrap()
		.authorized_keys
		.items()
		.iter()
	{
		if let Some(key) = garage
			.key_table
			.get(&EmptyKey, k)
			.await?
			.filter(|k| !k.is_deleted())
		{
			if !key.state.is_deleted() {
				relevant_keys.insert(k.clone(), key);
			}
		}
	}
	for ((k, _), _, _) in bucket
		.state
		.as_option()
		.unwrap()
		.local_aliases
		.items()
		.iter()
	{
		if relevant_keys.contains_key(k) {
			continue;
		}
		if let Some(key) = garage.key_table.get(&EmptyKey, k).await? {
			if !key.state.is_deleted() {
				relevant_keys.insert(k.clone(), key);
			}
		}
	}

	let state = bucket.state.as_option().unwrap();

	let res = GetBucketInfoResult {
		id: hex::encode(&bucket.id),
		global_aliases: state
			.aliases
			.items()
			.iter()
			.filter(|(_, _, a)| *a)
			.map(|(n, _, _)| n.to_string())
			.collect::<Vec<_>>(),
		keys: relevant_keys
			.into_iter()
			.map(|(_, key)| {
				let p = key.state.as_option().unwrap();
				GetBucketInfoKey {
					access_key_id: key.key_id,
					name: p.name.get().to_string(),
					permissions: p
						.authorized_buckets
						.get(&bucket.id)
						.map(|p| ApiBucketKeyPerm {
							read: p.allow_read,
							write: p.allow_write,
							owner: p.allow_owner,
						})
						.unwrap_or_default(),
					bucket_local_aliases: p
						.local_aliases
						.items()
						.iter()
						.filter(|(_, _, b)| *b == Some(bucket.id))
						.map(|(n, _, _)| n.to_string())
						.collect::<Vec<_>>(),
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
struct GetBucketInfoResult {
	id: String,
	#[serde(rename = "globalAliases")]
	global_aliases: Vec<String>,
	keys: Vec<GetBucketInfoKey>,
}

#[derive(Serialize)]
struct GetBucketInfoKey {
	#[serde(rename = "accessKeyId")]
	access_key_id: String,
	#[serde(rename = "name")]
	name: String,
	permissions: ApiBucketKeyPerm,
	#[serde(rename = "bucketLocalAliases")]
	bucket_local_aliases: Vec<String>,
}

pub async fn handle_create_bucket(
	garage: &Arc<Garage>,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let req = parse_json_body::<CreateBucketRequest>(req).await?;

	if let Some(ga) = &req.global_alias {
		if !is_valid_bucket_name(ga) {
			return Err(Error::BadRequest(format!(
				"{}: {}",
				ga, INVALID_BUCKET_NAME_MESSAGE
			)));
		}

		if let Some(alias) = garage.bucket_alias_table.get(&EmptyKey, ga).await? {
			if alias.state.get().is_some() {
				return Err(Error::BucketAlreadyExists);
			}
		}
	}

	if let Some(la) = &req.local_alias {
		if !is_valid_bucket_name(&la.alias) {
			return Err(Error::BadRequest(format!(
				"{}: {}",
				la.alias, INVALID_BUCKET_NAME_MESSAGE
			)));
		}

		let key = garage
			.key_table
			.get(&EmptyKey, &la.access_key_id)
			.await?
			.ok_or(Error::NoSuchKey)?;
		let state = key.state.as_option().ok_or(Error::NoSuchKey)?;
		if matches!(state.local_aliases.get(&la.alias), Some(_)) {
			return Err(Error::BadRequest("Local alias already exists".into()));
		}
	}

	let bucket = Bucket::new();
	garage.bucket_table.insert(&bucket).await?;

	if let Some(ga) = &req.global_alias {
		garage
			.bucket_helper()
			.set_global_bucket_alias(bucket.id, ga)
			.await?;
	}

	if let Some(la) = &req.local_alias {
		garage
			.bucket_helper()
			.set_local_bucket_alias(bucket.id, &la.access_key_id, &la.alias)
			.await?;
		if la.all_permissions {
			garage
				.bucket_helper()
				.set_bucket_key_permissions(
					bucket.id,
					&la.access_key_id,
					BucketKeyPerm::ALL_PERMISSIONS,
				)
				.await?;
		}
	}

	let bucket = garage
		.bucket_table
		.get(&EmptyKey, &bucket.id)
		.await?
		.ok_or_internal_error("Bucket should now exist but doesn't")?;
	bucket_info_results(garage, bucket).await
}

#[derive(Deserialize)]
struct CreateBucketRequest {
	#[serde(rename = "globalAlias")]
	global_alias: Option<String>,
	#[serde(rename = "localAlias")]
	local_alias: Option<CreateBucketLocalAlias>,
}

#[derive(Deserialize)]
struct CreateBucketLocalAlias {
	#[serde(rename = "accessKeyId")]
	access_key_id: String,
	alias: String,
	#[serde(rename = "allPermissions", default)]
	all_permissions: bool,
}

pub async fn handle_delete_bucket(
	garage: &Arc<Garage>,
	id: String,
) -> Result<Response<Body>, Error> {
	let helper = garage.bucket_helper();

	let id_hex = hex::decode(&id).ok_or_bad_request("Invalid bucket id")?;
	let bucket_id = Uuid::try_from(&id_hex).ok_or_bad_request("Invalid bucket id")?;

	let mut bucket = helper.get_existing_bucket(bucket_id).await?;
	let state = bucket.state.as_option().unwrap();

	// Check bucket is empty
	let objects = garage
		.object_table
		.get_range(
			&bucket_id,
			None,
			Some(ObjectFilter::IsData),
			10,
			EnumerationOrder::Forward,
		)
		.await?;
	if !objects.is_empty() {
		return Err(Error::BadRequest("Bucket is not empty".into()));
	}

	// --- done checking, now commit ---
	// 1. delete authorization from keys that had access
	for (key_id, perm) in bucket.authorized_keys() {
		if perm.is_any() {
			helper
				.set_bucket_key_permissions(bucket.id, key_id, BucketKeyPerm::NO_PERMISSIONS)
				.await?;
		}
	}
	// 2. delete all local aliases
	for ((key_id, alias), _, active) in state.local_aliases.items().iter() {
		if *active {
			helper
				.unset_local_bucket_alias(bucket.id, &key_id, &alias)
				.await?;
		}
	}
	// 3. delete all global aliases
	for (alias, _, active) in state.aliases.items().iter() {
		if *active {
			helper.purge_global_bucket_alias(bucket.id, &alias).await?;
		}
	}

	// 4. delete bucket
	bucket.state = Deletable::delete();
	garage.bucket_table.insert(&bucket).await?;

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(Body::empty())?)
}

pub async fn handle_bucket_allow_key(
	garage: &Arc<Garage>,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	handle_bucket_change_key_perm(garage, req, true).await
}

pub async fn handle_bucket_deny_key(
	garage: &Arc<Garage>,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	handle_bucket_change_key_perm(garage, req, false).await
}

pub async fn handle_bucket_change_key_perm(
	garage: &Arc<Garage>,
	req: Request<Body>,
	new_perm_flag: bool,
) -> Result<Response<Body>, Error> {
	let req = parse_json_body::<BucketKeyPermChangeRequest>(req).await?;

	let id_hex = hex::decode(&req.bucket_id).ok_or_bad_request("Invalid bucket id")?;
	let bucket_id = Uuid::try_from(&id_hex).ok_or_bad_request("Invalid bucket id")?;

	let bucket = garage
		.bucket_helper()
		.get_existing_bucket(bucket_id)
		.await?;
	let state = bucket.state.as_option().unwrap();

	let key = garage
		.key_helper()
		.get_existing_key(&req.access_key_id)
		.await?;

	let mut perm = state
		.authorized_keys
		.get(&key.key_id)
		.cloned()
		.unwrap_or(BucketKeyPerm::NO_PERMISSIONS);

	if req.permissions.read {
		perm.allow_read = new_perm_flag;
	}
	if req.permissions.write {
		perm.allow_write = new_perm_flag;
	}
	if req.permissions.owner {
		perm.allow_owner = new_perm_flag;
	}

	garage
		.bucket_helper()
		.set_bucket_key_permissions(bucket.id, &key.key_id, perm)
		.await?;

	let bucket = garage
		.bucket_table
		.get(&EmptyKey, &bucket.id)
		.await?
		.ok_or_internal_error("Bucket should now exist but doesn't")?;
	bucket_info_results(garage, bucket).await
}

#[derive(Deserialize)]
struct BucketKeyPermChangeRequest {
	#[serde(rename = "bucketId")]
	bucket_id: String,
	#[serde(rename = "accessKeyId")]
	access_key_id: String,
	permissions: ApiBucketKeyPerm,
}
