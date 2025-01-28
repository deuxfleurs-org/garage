use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::time::*;

use garage_table::*;

use garage_model::bucket_alias_table::*;
use garage_model::bucket_table::*;
use garage_model::garage::Garage;
use garage_model::permission::*;
use garage_model::s3::mpu_table;
use garage_model::s3::object_table::*;

use crate::admin::api::ApiBucketKeyPerm;
use crate::admin::api::{
	ApiBucketQuotas, BucketAllowKeyRequest, BucketAllowKeyResponse, BucketDenyKeyRequest,
	BucketDenyKeyResponse, BucketKeyPermChangeRequest, BucketLocalAlias, CreateBucketRequest,
	CreateBucketResponse, DeleteBucketRequest, DeleteBucketResponse, GetBucketInfoKey,
	GetBucketInfoRequest, GetBucketInfoResponse, GetBucketInfoWebsiteResponse,
	AddGlobalBucketAliasRequest, AddGlobalBucketAliasResponse, RemoveGlobalBucketAliasRequest,
	RemoveGlobalBucketAliasResponse, ListBucketsRequest, ListBucketsResponse, ListBucketsResponseItem,
	AddLocalBucketAliasRequest, AddLocalBucketAliasResponse, RemoveLocalBucketAliasRequest,
	RemoveLocalBucketAliasResponse, UpdateBucketRequest, UpdateBucketResponse,
};
use crate::admin::error::*;
use crate::admin::EndpointHandler;
use crate::common_error::CommonError;

#[async_trait]
impl EndpointHandler for ListBucketsRequest {
	type Response = ListBucketsResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<ListBucketsResponse, Error> {
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
				ListBucketsResponseItem {
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

		Ok(ListBucketsResponse(res))
	}
}

#[async_trait]
impl EndpointHandler for GetBucketInfoRequest {
	type Response = GetBucketInfoResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<GetBucketInfoResponse, Error> {
		let bucket_id = match (self.id, self.global_alias) {
			(Some(id), None) => parse_bucket_id(&id)?,
			(None, Some(ga)) => garage
				.bucket_helper()
				.resolve_global_bucket_name(&ga)
				.await?
				.ok_or_else(|| HelperError::NoSuchBucket(ga.to_string()))?,
			_ => {
				return Err(Error::bad_request(
					"Either id or globalAlias must be provided (but not both)",
				));
			}
		};

		bucket_info_results(garage, bucket_id).await
	}
}

async fn bucket_info_results(
	garage: &Arc<Garage>,
	bucket_id: Uuid,
) -> Result<GetBucketInfoResponse, Error> {
	let bucket = garage
		.bucket_helper()
		.get_existing_bucket(bucket_id)
		.await?;

	let counters = garage
		.object_counter_table
		.table
		.get(&bucket_id, &EmptyKey)
		.await?
		.map(|x| x.filtered_values(&garage.system.cluster_layout()))
		.unwrap_or_default();

	let mpu_counters = garage
		.mpu_counter_table
		.table
		.get(&bucket_id, &EmptyKey)
		.await?
		.map(|x| x.filtered_values(&garage.system.cluster_layout()))
		.unwrap_or_default();

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

	let quotas = state.quotas.get();
	let res = GetBucketInfoResponse {
		id: hex::encode(bucket.id),
		global_aliases: state
			.aliases
			.items()
			.iter()
			.filter(|(_, _, a)| *a)
			.map(|(n, _, _)| n.to_string())
			.collect::<Vec<_>>(),
		website_access: state.website_config.get().is_some(),
		website_config: state.website_config.get().clone().map(|wsc| {
			GetBucketInfoWebsiteResponse {
				index_document: wsc.index_document,
				error_document: wsc.error_document,
			}
		}),
		keys: relevant_keys
			.into_values()
			.map(|key| {
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
		objects: *counters.get(OBJECTS).unwrap_or(&0),
		bytes: *counters.get(BYTES).unwrap_or(&0),
		unfinished_uploads: *counters.get(UNFINISHED_UPLOADS).unwrap_or(&0),
		unfinished_multipart_uploads: *mpu_counters.get(mpu_table::UPLOADS).unwrap_or(&0),
		unfinished_multipart_upload_parts: *mpu_counters.get(mpu_table::PARTS).unwrap_or(&0),
		unfinished_multipart_upload_bytes: *mpu_counters.get(mpu_table::BYTES).unwrap_or(&0),
		quotas: ApiBucketQuotas {
			max_size: quotas.max_size,
			max_objects: quotas.max_objects,
		},
	};

	Ok(res)
}

#[async_trait]
impl EndpointHandler for CreateBucketRequest {
	type Response = CreateBucketResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<CreateBucketResponse, Error> {
		let helper = garage.locked_helper().await;

		if let Some(ga) = &self.global_alias {
			if !is_valid_bucket_name(ga) {
				return Err(Error::bad_request(format!(
					"{}: {}",
					ga, INVALID_BUCKET_NAME_MESSAGE
				)));
			}

			if let Some(alias) = garage.bucket_alias_table.get(&EmptyKey, ga).await? {
				if alias.state.get().is_some() {
					return Err(CommonError::BucketAlreadyExists.into());
				}
			}
		}

		if let Some(la) = &self.local_alias {
			if !is_valid_bucket_name(&la.alias) {
				return Err(Error::bad_request(format!(
					"{}: {}",
					la.alias, INVALID_BUCKET_NAME_MESSAGE
				)));
			}

			let key = helper.key().get_existing_key(&la.access_key_id).await?;
			let state = key.state.as_option().unwrap();
			if matches!(state.local_aliases.get(&la.alias), Some(_)) {
				return Err(Error::bad_request("Local alias already exists"));
			}
		}

		let bucket = Bucket::new();
		garage.bucket_table.insert(&bucket).await?;

		if let Some(ga) = &self.global_alias {
			helper.set_global_bucket_alias(bucket.id, ga).await?;
		}

		if let Some(la) = &self.local_alias {
			helper
				.set_local_bucket_alias(bucket.id, &la.access_key_id, &la.alias)
				.await?;

			if la.allow.read || la.allow.write || la.allow.owner {
				helper
					.set_bucket_key_permissions(
						bucket.id,
						&la.access_key_id,
						BucketKeyPerm {
							timestamp: now_msec(),
							allow_read: la.allow.read,
							allow_write: la.allow.write,
							allow_owner: la.allow.owner,
						},
					)
					.await?;
			}
		}

		Ok(CreateBucketResponse(
			bucket_info_results(garage, bucket.id).await?,
		))
	}
}

#[async_trait]
impl EndpointHandler for DeleteBucketRequest {
	type Response = DeleteBucketResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<DeleteBucketResponse, Error> {
		let helper = garage.locked_helper().await;

		let bucket_id = parse_bucket_id(&self.id)?;

		let mut bucket = helper.bucket().get_existing_bucket(bucket_id).await?;
		let state = bucket.state.as_option().unwrap();

		// Check bucket is empty
		if !helper.bucket().is_bucket_empty(bucket_id).await? {
			return Err(CommonError::BucketNotEmpty.into());
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
					.unset_local_bucket_alias(bucket.id, key_id, alias)
					.await?;
			}
		}
		// 3. delete all global aliases
		for (alias, _, active) in state.aliases.items().iter() {
			if *active {
				helper.purge_global_bucket_alias(bucket.id, alias).await?;
			}
		}

		// 4. delete bucket
		bucket.state = Deletable::delete();
		garage.bucket_table.insert(&bucket).await?;

		Ok(DeleteBucketResponse)
	}
}

#[async_trait]
impl EndpointHandler for UpdateBucketRequest {
	type Response = UpdateBucketResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<UpdateBucketResponse, Error> {
		let bucket_id = parse_bucket_id(&self.id)?;

		let mut bucket = garage
			.bucket_helper()
			.get_existing_bucket(bucket_id)
			.await?;

		let state = bucket.state.as_option_mut().unwrap();

		if let Some(wa) = self.body.website_access {
			if wa.enabled {
				state.website_config.update(Some(WebsiteConfig {
					index_document: wa.index_document.ok_or_bad_request(
						"Please specify indexDocument when enabling website access.",
					)?,
					error_document: wa.error_document,
				}));
			} else {
				if wa.index_document.is_some() || wa.error_document.is_some() {
					return Err(Error::bad_request(
                        "Cannot specify indexDocument or errorDocument when disabling website access.",
                    ));
				}
				state.website_config.update(None);
			}
		}

		if let Some(q) = self.body.quotas {
			state.quotas.update(BucketQuotas {
				max_size: q.max_size,
				max_objects: q.max_objects,
			});
		}

		garage.bucket_table.insert(&bucket).await?;

		Ok(UpdateBucketResponse(
			bucket_info_results(garage, bucket_id).await?,
		))
	}
}

// ---- BUCKET/KEY PERMISSIONS ----

#[async_trait]
impl EndpointHandler for BucketAllowKeyRequest {
	type Response = BucketAllowKeyResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<BucketAllowKeyResponse, Error> {
		let res = handle_bucket_change_key_perm(garage, self.0, true).await?;
		Ok(BucketAllowKeyResponse(res))
	}
}

#[async_trait]
impl EndpointHandler for BucketDenyKeyRequest {
	type Response = BucketDenyKeyResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<BucketDenyKeyResponse, Error> {
		let res = handle_bucket_change_key_perm(garage, self.0, false).await?;
		Ok(BucketDenyKeyResponse(res))
	}
}

pub async fn handle_bucket_change_key_perm(
	garage: &Arc<Garage>,
	req: BucketKeyPermChangeRequest,
	new_perm_flag: bool,
) -> Result<GetBucketInfoResponse, Error> {
	let helper = garage.locked_helper().await;

	let bucket_id = parse_bucket_id(&req.bucket_id)?;

	let bucket = helper.bucket().get_existing_bucket(bucket_id).await?;
	let state = bucket.state.as_option().unwrap();

	let key = helper.key().get_existing_key(&req.access_key_id).await?;

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

	helper
		.set_bucket_key_permissions(bucket.id, &key.key_id, perm)
		.await?;

	bucket_info_results(garage, bucket.id).await
}

// ---- BUCKET ALIASES ----

#[async_trait]
impl EndpointHandler for AddGlobalBucketAliasRequest {
	type Response = AddGlobalBucketAliasResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<AddGlobalBucketAliasResponse, Error> {
		let bucket_id = parse_bucket_id(&self.bucket_id)?;

		let helper = garage.locked_helper().await;

		helper
			.set_global_bucket_alias(bucket_id, &self.alias)
			.await?;

		Ok(AddGlobalBucketAliasResponse(
			bucket_info_results(garage, bucket_id).await?,
		))
	}
}

#[async_trait]
impl EndpointHandler for RemoveGlobalBucketAliasRequest {
	type Response = RemoveGlobalBucketAliasResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<RemoveGlobalBucketAliasResponse, Error> {
		let bucket_id = parse_bucket_id(&self.bucket_id)?;

		let helper = garage.locked_helper().await;

		helper
			.unset_global_bucket_alias(bucket_id, &self.alias)
			.await?;

		Ok(RemoveGlobalBucketAliasResponse(
			bucket_info_results(garage, bucket_id).await?,
		))
	}
}

#[async_trait]
impl EndpointHandler for AddLocalBucketAliasRequest {
	type Response = AddLocalBucketAliasResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<AddLocalBucketAliasResponse, Error> {
		let bucket_id = parse_bucket_id(&self.bucket_id)?;

		let helper = garage.locked_helper().await;

		helper
			.set_local_bucket_alias(bucket_id, &self.access_key_id, &self.alias)
			.await?;

		Ok(AddLocalBucketAliasResponse(
			bucket_info_results(garage, bucket_id).await?,
		))
	}
}

#[async_trait]
impl EndpointHandler for RemoveLocalBucketAliasRequest {
	type Response = RemoveLocalBucketAliasResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<RemoveLocalBucketAliasResponse, Error> {
		let bucket_id = parse_bucket_id(&self.bucket_id)?;

		let helper = garage.locked_helper().await;

		helper
			.unset_local_bucket_alias(bucket_id, &self.access_key_id, &self.alias)
			.await?;

		Ok(RemoveLocalBucketAliasResponse(
			bucket_info_results(garage, bucket_id).await?,
		))
	}
}

// ---- HELPER ----

fn parse_bucket_id(id: &str) -> Result<Uuid, Error> {
	let id_hex = hex::decode(id).ok_or_bad_request("Invalid bucket id")?;
	Ok(Uuid::try_from(&id_hex).ok_or_bad_request("Invalid bucket id")?)
}
