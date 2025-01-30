use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use garage_table::*;

use garage_model::garage::Garage;
use garage_model::key_table::*;

use crate::api::*;
use crate::error::*;
use crate::{Admin, RequestHandler};

#[async_trait]
impl RequestHandler for ListKeysRequest {
	type Response = ListKeysResponse;

	async fn handle(self, garage: &Arc<Garage>, _admin: &Admin) -> Result<ListKeysResponse, Error> {
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
			.map(|k| ListKeysResponseItem {
				id: k.key_id.to_string(),
				name: k.params().unwrap().name.get().clone(),
			})
			.collect::<Vec<_>>();

		Ok(ListKeysResponse(res))
	}
}

#[async_trait]
impl RequestHandler for GetKeyInfoRequest {
	type Response = GetKeyInfoResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<GetKeyInfoResponse, Error> {
		let key = match (self.id, self.search) {
			(Some(id), None) => garage.key_helper().get_existing_key(&id).await?,
			(None, Some(search)) => {
				garage
					.key_helper()
					.get_existing_matching_key(&search)
					.await?
			}
			_ => {
				return Err(Error::bad_request(
					"Either id or search must be provided (but not both)",
				));
			}
		};

		Ok(key_info_results(garage, key, self.show_secret_key).await?)
	}
}

#[async_trait]
impl RequestHandler for CreateKeyRequest {
	type Response = CreateKeyResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<CreateKeyResponse, Error> {
		let key = Key::new(self.name.as_deref().unwrap_or("Unnamed key"));
		garage.key_table.insert(&key).await?;

		Ok(CreateKeyResponse(
			key_info_results(garage, key, true).await?,
		))
	}
}

#[async_trait]
impl RequestHandler for ImportKeyRequest {
	type Response = ImportKeyResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<ImportKeyResponse, Error> {
		let prev_key = garage.key_table.get(&EmptyKey, &self.access_key_id).await?;
		if prev_key.is_some() {
			return Err(Error::KeyAlreadyExists(self.access_key_id.to_string()));
		}

		let imported_key = Key::import(
			&self.access_key_id,
			&self.secret_access_key,
			self.name.as_deref().unwrap_or("Imported key"),
		)
		.ok_or_bad_request("Invalid key format")?;
		garage.key_table.insert(&imported_key).await?;

		Ok(ImportKeyResponse(
			key_info_results(garage, imported_key, false).await?,
		))
	}
}

#[async_trait]
impl RequestHandler for UpdateKeyRequest {
	type Response = UpdateKeyResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<UpdateKeyResponse, Error> {
		let mut key = garage.key_helper().get_existing_key(&self.id).await?;

		let key_state = key.state.as_option_mut().unwrap();

		if let Some(new_name) = self.body.name {
			key_state.name.update(new_name);
		}
		if let Some(allow) = self.body.allow {
			if allow.create_bucket {
				key_state.allow_create_bucket.update(true);
			}
		}
		if let Some(deny) = self.body.deny {
			if deny.create_bucket {
				key_state.allow_create_bucket.update(false);
			}
		}

		garage.key_table.insert(&key).await?;

		Ok(UpdateKeyResponse(
			key_info_results(garage, key, false).await?,
		))
	}
}

#[async_trait]
impl RequestHandler for DeleteKeyRequest {
	type Response = DeleteKeyResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<DeleteKeyResponse, Error> {
		let helper = garage.locked_helper().await;

		let mut key = helper.key().get_existing_key(&self.id).await?;

		helper.delete_key(&mut key).await?;

		Ok(DeleteKeyResponse)
	}
}

async fn key_info_results(
	garage: &Arc<Garage>,
	key: Key,
	show_secret: bool,
) -> Result<GetKeyInfoResponse, Error> {
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

	let res = GetKeyInfoResponse {
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
				KeyInfoBucketResponse {
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

	Ok(res)
}
