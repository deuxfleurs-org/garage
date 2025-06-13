use std::collections::HashMap;

use hyper::{Request, Response, StatusCode};

use garage_model::bucket_alias_table::*;
use garage_model::bucket_table::Bucket;
use garage_model::garage::Garage;
use garage_model::key_table::{Key, KeyParams};
use garage_model::permission::BucketKeyPerm;
use garage_table::util::*;
use garage_util::crdt::*;
use garage_util::time::*;

use garage_api_common::common_error::CommonError;
use garage_api_common::helpers::*;

use crate::api_server::{ReqBody, ResBody};
use crate::error::*;
use crate::xml as s3_xml;

pub fn handle_get_bucket_location(ctx: ReqCtx) -> Result<Response<ResBody>, Error> {
	let ReqCtx { garage, .. } = ctx;
	let loc = s3_xml::LocationConstraint {
		xmlns: (),
		region: garage.config.s3_api.s3_region.to_string(),
	};
	let xml = s3_xml::to_xml_with_header(&loc)?;

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(string_body(xml))?)
}

pub fn handle_get_bucket_versioning() -> Result<Response<ResBody>, Error> {
	let versioning = s3_xml::VersioningConfiguration {
		xmlns: (),
		status: None,
	};

	let xml = s3_xml::to_xml_with_header(&versioning)?;

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(string_body(xml))?)
}

pub fn handle_get_bucket_acl(ctx: ReqCtx) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		bucket_id, api_key, ..
	} = ctx;
	let key_p = api_key.params().ok_or_internal_error(
		"Key should not be in deleted state at this point (in handle_get_bucket_acl)",
	)?;

	let mut grants: Vec<s3_xml::Grant> = vec![];
	let kp = api_key.bucket_permissions(&bucket_id);

	if kp.allow_owner {
		grants.push(s3_xml::Grant {
			grantee: create_grantee(&key_p, &api_key),
			permission: s3_xml::Value("FULL_CONTROL".to_string()),
		});
	} else {
		if kp.allow_read {
			grants.push(s3_xml::Grant {
				grantee: create_grantee(&key_p, &api_key),
				permission: s3_xml::Value("READ".to_string()),
			});
			grants.push(s3_xml::Grant {
				grantee: create_grantee(&key_p, &api_key),
				permission: s3_xml::Value("READ_ACP".to_string()),
			});
		}
		if kp.allow_write {
			grants.push(s3_xml::Grant {
				grantee: create_grantee(&key_p, &api_key),
				permission: s3_xml::Value("WRITE".to_string()),
			});
		}
	}

	let access_control_policy = s3_xml::AccessControlPolicy {
		xmlns: (),
		owner: None,
		acl: s3_xml::AccessControlList { entries: grants },
	};

	let xml = s3_xml::to_xml_with_header(&access_control_policy)?;
	trace!("xml: {}", xml);

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(string_body(xml))?)
}

pub async fn handle_list_buckets(
	garage: &Garage,
	api_key: &Key,
) -> Result<Response<ResBody>, Error> {
	let key_p = api_key.params().ok_or_internal_error(
		"Key should not be in deleted state at this point (in handle_list_buckets)",
	)?;

	// Collect buckets user has access to
	let ids = api_key
		.state
		.as_option()
		.unwrap()
		.authorized_buckets
		.items()
		.iter()
		.filter(|(_, perms)| perms.is_any())
		.map(|(id, _)| *id)
		.collect::<Vec<_>>();

	let mut buckets_by_id = HashMap::new();
	let mut aliases = HashMap::new();

	for bucket_id in ids.iter() {
		let bucket = garage.bucket_table.get(&EmptyKey, bucket_id).await?;
		if let Some(bucket) = bucket {
			for (alias, _, _active) in bucket.aliases().iter().filter(|(_, _, active)| *active) {
				let alias_opt = garage.bucket_alias_table.get(&EmptyKey, alias).await?;
				if let Some(alias_ent) = alias_opt {
					if *alias_ent.state.get() == Some(*bucket_id) {
						aliases.insert(alias_ent.name().to_string(), *bucket_id);
					}
				}
			}
			if let Deletable::Present(param) = bucket.state {
				buckets_by_id.insert(bucket_id, param);
			}
		}
	}

	for (alias, _, id_opt) in key_p.local_aliases.items() {
		if let Some(id) = id_opt {
			aliases.insert(alias.clone(), *id);
		}
	}

	// Generate response
	let list_buckets = s3_xml::ListAllMyBucketsResult {
		owner: s3_xml::Owner {
			display_name: s3_xml::Value(key_p.name.get().to_string()),
			id: s3_xml::Value(api_key.key_id.to_string()),
		},
		buckets: s3_xml::BucketList {
			entries: aliases
				.iter()
				.filter_map(|(name, id)| buckets_by_id.get(id).map(|p| (name, id, p)))
				.map(|(name, _id, param)| s3_xml::Bucket {
					creation_date: s3_xml::Value(msec_to_rfc3339(param.creation_date)),
					name: s3_xml::Value(name.to_string()),
				})
				.collect(),
		},
	};

	let xml = s3_xml::to_xml_with_header(&list_buckets)?;
	trace!("xml: {}", xml);

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(string_body(xml))?)
}

pub async fn handle_create_bucket(
	garage: &Garage,
	req: Request<ReqBody>,
	api_key_id: &String,
	bucket_name: String,
) -> Result<Response<ResBody>, Error> {
	let body = req.into_body().collect().await?;

	let cmd =
		parse_create_bucket_xml(&body[..]).ok_or_bad_request("Invalid create bucket XML query")?;

	if let Some(location_constraint) = cmd {
		if location_constraint != garage.config.s3_api.s3_region {
			return Err(Error::bad_request(format!(
				"Cannot satisfy location constraint `{}`: buckets can only be created in region `{}`",
				location_constraint,
				garage.config.s3_api.s3_region
			)));
		}
	}

	let helper = garage.locked_helper().await;

	// refetch API key after taking lock to ensure up-to-date data
	let api_key = helper.key().get_existing_key(api_key_id).await?;
	let key_params = api_key.params().unwrap();

	let existing_bucket = helper
		.bucket()
		.resolve_bucket(&bucket_name, &api_key.key_id)
		.await?;

	if let Some(bucket) = existing_bucket {
		// Check we have write or owner permission on the bucket,
		// in that case it's fine, return 200 OK, bucket exists;
		// otherwise return a forbidden error.
		let kp = api_key.bucket_permissions(&bucket.id);
		if !(kp.allow_write || kp.allow_owner) {
			return Err(CommonError::BucketAlreadyExists.into());
		}
	} else {
		// Check user is allowed to create bucket
		if !key_params.allow_create_bucket.get() {
			return Err(CommonError::Forbidden(format!(
				"Access key {} is not allowed to create buckets",
				api_key.key_id
			))
			.into());
		}

		// Create the bucket!
		if !is_valid_bucket_name(&bucket_name, garage.config.allow_punycode) {
			return Err(Error::bad_request(format!(
				"{}: {}",
				bucket_name, INVALID_BUCKET_NAME_MESSAGE
			)));
		}

		let bucket = Bucket::new();
		garage.bucket_table.insert(&bucket).await?;

		helper
			.set_bucket_key_permissions(bucket.id, &api_key.key_id, BucketKeyPerm::ALL_PERMISSIONS)
			.await?;

		helper
			.set_local_bucket_alias(bucket.id, &api_key.key_id, &bucket_name)
			.await?;
	}

	Ok(Response::builder()
		.header("Location", format!("/{}", bucket_name))
		.body(empty_body())
		.unwrap())
}

pub async fn handle_delete_bucket(ctx: ReqCtx) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage,
		bucket_id,
		bucket_name,
		bucket_params: bucket_state,
		api_key,
		..
	} = &ctx;
	let helper = garage.locked_helper().await;

	let key_params = api_key.params().unwrap();

	let is_local_alias = matches!(key_params.local_aliases.get(bucket_name), Some(Some(_)));

	// If the bucket has no other aliases, this is a true deletion.
	// Otherwise, it is just an alias removal.

	let has_other_global_aliases = bucket_state
		.aliases
		.items()
		.iter()
		.filter(|(_, _, active)| *active)
		.any(|(n, _, _)| is_local_alias || (*n != *bucket_name));

	let has_other_local_aliases = bucket_state
		.local_aliases
		.items()
		.iter()
		.filter(|(_, _, active)| *active)
		.any(|((k, n), _, _)| !is_local_alias || *n != *bucket_name || *k != api_key.key_id);

	if !has_other_global_aliases && !has_other_local_aliases {
		// Delete bucket

		// Check bucket is empty
		if !helper.bucket().is_bucket_empty(*bucket_id).await? {
			return Err(CommonError::BucketNotEmpty.into());
		}

		// --- done checking, now commit ---
		// 1. delete bucket alias
		if is_local_alias {
			helper
				.purge_local_bucket_alias(*bucket_id, &api_key.key_id, bucket_name)
				.await?;
		} else {
			helper
				.purge_global_bucket_alias(*bucket_id, bucket_name)
				.await?;
		}

		// 2. delete authorization from keys that had access
		for (key_id, _) in bucket_state.authorized_keys.items() {
			helper
				.set_bucket_key_permissions(*bucket_id, key_id, BucketKeyPerm::NO_PERMISSIONS)
				.await?;
		}

		let bucket = Bucket {
			id: *bucket_id,
			state: Deletable::delete(),
		};
		// 3. delete bucket
		garage.bucket_table.insert(&bucket).await?;
	} else if is_local_alias {
		// Just unalias
		helper
			.unset_local_bucket_alias(*bucket_id, &api_key.key_id, bucket_name)
			.await?;
	} else {
		// Just unalias (but from global namespace)
		helper
			.unset_global_bucket_alias(*bucket_id, bucket_name)
			.await?;
	}

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(empty_body())?)
}

fn parse_create_bucket_xml(xml_bytes: &[u8]) -> Option<Option<String>> {
	// Returns None if invalid data
	// Returns Some(None) if no location constraint is given
	// Returns Some(Some("xxxx")) where xxxx is the given location constraint

	let xml_str = std::str::from_utf8(xml_bytes).ok()?;
	if xml_str.trim_matches(char::is_whitespace).is_empty() {
		return Some(None);
	}

	let xml = roxmltree::Document::parse(xml_str).ok()?;

	let cbc = xml.root().first_child()?;
	if !cbc.has_tag_name("CreateBucketConfiguration") {
		return None;
	}

	let mut ret = None;
	for item in cbc.children() {
		if item.has_tag_name("LocationConstraint") {
			if ret.is_some() {
				return None;
			}
			ret = Some(item.text()?.to_string());
		} else if !item.is_text() {
			return None;
		}
	}

	Some(ret)
}

fn create_grantee(key_params: &KeyParams, api_key: &Key) -> s3_xml::Grantee {
	s3_xml::Grantee {
		xmlns_xsi: (),
		typ: "CanonicalUser".to_string(),
		display_name: Some(s3_xml::Value(key_params.name.get().to_string())),
		id: Some(s3_xml::Value(api_key.key_id.to_string())),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn create_bucket() {
		assert_eq!(parse_create_bucket_xml(br#""#), Some(None));
		assert_eq!(
			parse_create_bucket_xml(
				br#"
            <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            </CreateBucketConfiguration >
		"#
			),
			Some(None)
		);
		assert_eq!(
			parse_create_bucket_xml(
				br#"
            <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
             <LocationConstraint>Europe</LocationConstraint>
            </CreateBucketConfiguration >
		"#
			),
			Some(Some("Europe".into()))
		);
		assert_eq!(
			parse_create_bucket_xml(
				br#"
            <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            </Crea >
		"#
			),
			None
		);
	}
}
