use std::collections::HashMap;
use std::sync::Arc;

use chrono::DateTime;
use futures::StreamExt;

use garage_table::*;
use garage_util::time::now_msec;

use garage_model::garage::Garage;
use garage_model::key_table::*;
use garage_model::permission::ExpirationTime;

use crate::api::*;
use crate::error::*;
use crate::{Admin, RequestHandler};

impl RequestHandler for ListKeysRequest {
	type Response = ListKeysResponse;

	async fn handle(self, garage: &Arc<Garage>, _admin: &Admin) -> Result<ListKeysResponse, Error> {
		let now = now_msec();

		let limit = self
			.limit
			.unwrap_or_else(|| if self.details { 1000 } else { 10_000 });

		let keys = garage
			.key_table
			.get_range(
				&EmptyKey,
				self.offset,
				Some(KeyFilter::Deleted(DeletedFilter::NotDeleted)),
				limit,
				EnumerationOrder::Forward,
			)
			.await?;

		if self.details {
			let mut stream = keys
				.into_iter()
				.map(|k| key_info_results(garage, k, false))
				.collect::<futures::stream::FuturesOrdered<_>>();

			let mut res = vec![];
			while let Some(next) = stream.next().await {
				res.push(next?);
			}

			Ok(ListKeysResponse::WithDetails(res))
		} else {
			let res = keys
				.iter()
				.map(|k| {
					let p = k.params().unwrap();

					ListKeysResponseItem {
						id: k.key_id.to_string(),
						name: p.name.get().clone(),
						created: p.created.map(|x| {
							DateTime::from_timestamp_millis(x as i64)
								.expect("invalid timestamp stored in db")
						}),
						expiration: p.expiration.get().inner().map(|x| {
							DateTime::from_timestamp_millis(x.0 as i64)
								.expect("invalid timestamp stored in db")
						}),
						expired: p.is_expired(now),
					}
				})
				.collect::<Vec<_>>();

			Ok(ListKeysResponse::WithoutDetails(res))
		}
	}
}

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
				let candidates = garage
					.key_table
					.get_range(
						&EmptyKey,
						None,
						Some(KeyFilter::MatchesAndNotDeleted(search.to_string())),
						10,
						EnumerationOrder::Forward,
					)
					.await?
					.into_iter()
					.collect::<Vec<_>>();
				if candidates.is_empty() {
					return Err(Error::NoSuchAccessKey(search.clone()));
				} else if candidates.len() != 1 {
					return Err(Error::bad_request(format!(
						"{} matching keys",
						candidates.len()
					)));
				}
				candidates.into_iter().next().unwrap()
			}
			_ => {
				return Err(Error::bad_request(
					"Either id or search must be provided (but not both)",
				));
			}
		};

		key_info_results(garage, key, self.show_secret_key).await
	}
}

impl RequestHandler for CreateKeyRequest {
	type Response = CreateKeyResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<CreateKeyResponse, Error> {
		let mut key = Key::new("Unnamed key");

		apply_key_updates(&mut key, self.0)?;

		garage.key_table.insert(&key).await?;

		Ok(CreateKeyResponse(
			key_info_results(garage, key, true).await?,
		))
	}
}

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

impl RequestHandler for UpdateKeyRequest {
	type Response = UpdateKeyResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<UpdateKeyResponse, Error> {
		let mut key = garage.key_helper().get_existing_key(&self.id).await?;

		apply_key_updates(&mut key, self.body)?;

		garage.key_table.insert(&key).await?;

		Ok(UpdateKeyResponse(
			key_info_results(garage, key, false).await?,
		))
	}
}

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
	let key_state = key.state.as_option().unwrap();

	let buckets1 = key_state
		.authorized_buckets
		.items()
		.iter()
		.filter(|(_, p)| p.is_any())
		.map(|(id, _)| id);
	let buckets2 = key_state
		.local_aliases
		.items()
		.iter()
		.filter_map(|(_, _, v)| v.inner());

	let mut relevant_buckets = HashMap::new();
	for bucket_id in buckets1.chain(buckets2) {
		if !relevant_buckets.contains_key(bucket_id) {
			if let Some(b) = garage.bucket_table.get(&EmptyKey, bucket_id).await? {
				relevant_buckets.insert(*bucket_id, b);
			} else {
				warn!(
					"Key {} references non-existent bucket {:?}",
					key.key_id, bucket_id
				);
			}
		}
	}
	relevant_buckets.retain(|_, b| !b.is_deleted());

	let res = GetKeyInfoResponse {
		name: key_state.name.get().clone(),
		created: key_state.created.map(|x| {
			DateTime::from_timestamp_millis(x as i64).expect("invalid timestamp stored in db")
		}),
		expiration: key_state.expiration.get().inner().map(|x| {
			DateTime::from_timestamp_millis(x.0 as i64).expect("invalid timestamp stored in db")
		}),
		expired: key_state.is_expired(now_msec()),
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
				let permissions = key_state
					.authorized_buckets
					.get(&bucket.id)
					.filter(|p| p.is_any())
					.map(|p| ApiBucketKeyPerm {
						read: p.allow_read,
						write: p.allow_write,
						owner: p.allow_owner,
					})
					.unwrap_or_default();
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
					permissions,
				}
			})
			.collect::<Vec<_>>(),
	};

	Ok(res)
}

fn apply_key_updates(key: &mut Key, updates: UpdateKeyRequestBody) -> Result<(), Error> {
	if updates.never_expires && updates.expiration.is_some() {
		return Err(Error::bad_request(
			"cannot specify `expiration` and `never_expires`",
		));
	}

	let key_state = key.state.as_option_mut().unwrap();

	if let Some(new_name) = updates.name {
		key_state.name.update(new_name);
	}
	if let Some(expiration) = updates.expiration {
		key_state
			.expiration
			.update(Some(ExpirationTime(expiration.timestamp_millis() as u64)).into());
	}
	if updates.never_expires {
		key_state.expiration.update(None.into());
	}
	if let Some(allow) = updates.allow {
		if allow.create_bucket {
			key_state.allow_create_bucket.update(true);
		}
	}
	if let Some(deny) = updates.deny {
		if deny.create_bucket {
			key_state.allow_create_bucket.update(false);
		}
	}

	Ok(())
}
