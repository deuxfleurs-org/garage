use std::collections::HashMap;
use std::sync::Arc;

use hyper::{Body, Request, Response, StatusCode};

use garage_model::bucket_alias_table::*;
use garage_model::bucket_table::Bucket;
use garage_model::garage::Garage;
use garage_model::key_table::Key;
use garage_model::object_table::ObjectFilter;
use garage_model::permission::BucketKeyPerm;
use garage_table::util::*;
use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::time::*;

use crate::error::*;
use crate::s3_xml;
use crate::signature::verify_signed_content;

pub fn handle_get_bucket_location(garage: Arc<Garage>) -> Result<Response<Body>, Error> {
	let loc = s3_xml::LocationConstraint {
		xmlns: (),
		region: garage.config.s3_api.s3_region.to_string(),
	};
	let xml = s3_xml::to_xml_with_header(&loc)?;

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(xml.into_bytes()))?)
}

pub fn handle_get_bucket_versioning() -> Result<Response<Body>, Error> {
	let versioning = s3_xml::VersioningConfiguration {
		xmlns: (),
		status: None,
	};

	let xml = s3_xml::to_xml_with_header(&versioning)?;

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(xml.into_bytes()))?)
}

pub async fn handle_list_buckets(garage: &Garage, api_key: &Key) -> Result<Response<Body>, Error> {
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
		.body(Body::from(xml))?)
}

pub async fn handle_create_bucket(
	garage: &Garage,
	req: Request<Body>,
	content_sha256: Option<Hash>,
	api_key: Key,
	bucket_name: String,
) -> Result<Response<Body>, Error> {
	let body = hyper::body::to_bytes(req.into_body()).await?;

	if let Some(content_sha256) = content_sha256 {
		verify_signed_content(content_sha256, &body[..])?;
	}

	let cmd =
		parse_create_bucket_xml(&body[..]).ok_or_bad_request("Invalid create bucket XML query")?;

	if let Some(location_constraint) = cmd {
		if location_constraint != garage.config.s3_api.s3_region {
			return Err(Error::BadRequest(format!(
				"Cannot satisfy location constraint `{}`: buckets can only be created in region `{}`",
				location_constraint,
				garage.config.s3_api.s3_region
			)));
		}
	}

	let key_params = api_key
		.params()
		.ok_or_internal_error("Key should not be deleted at this point")?;

	let existing_bucket = if let Some(Some(bucket_id)) = key_params.local_aliases.get(&bucket_name)
	{
		Some(*bucket_id)
	} else {
		garage
			.bucket_helper()
			.resolve_global_bucket_name(&bucket_name)
			.await?
	};

	if let Some(bucket_id) = existing_bucket {
		// Check we have write or owner permission on the bucket,
		// in that case it's fine, return 200 OK, bucket exists;
		// otherwise return a forbidden error.
		let kp = api_key.bucket_permissions(&bucket_id);
		if !(kp.allow_write || kp.allow_owner) {
			return Err(Error::BucketAlreadyExists);
		}
	} else {
		// Create the bucket!
		if !is_valid_bucket_name(&bucket_name) {
			return Err(Error::BadRequest(format!(
				"{}: {}",
				bucket_name, INVALID_BUCKET_NAME_MESSAGE
			)));
		}

		let bucket = Bucket::new();
		garage.bucket_table.insert(&bucket).await?;

		garage
			.bucket_helper()
			.set_bucket_key_permissions(bucket.id, &api_key.key_id, BucketKeyPerm::ALL_PERMISSIONS)
			.await?;

		garage
			.bucket_helper()
			.set_local_bucket_alias(bucket.id, &api_key.key_id, &bucket_name)
			.await?;
	}

	Ok(Response::builder()
		.header("Location", format!("/{}", bucket_name))
		.body(Body::empty())
		.unwrap())
}

pub async fn handle_delete_bucket(
	garage: &Garage,
	bucket_id: Uuid,
	bucket_name: String,
	api_key: Key,
) -> Result<Response<Body>, Error> {
	let key_params = api_key
		.params()
		.ok_or_internal_error("Key should not be deleted at this point")?;

	let is_local_alias = matches!(key_params.local_aliases.get(&bucket_name), Some(Some(_)));

	let mut bucket = garage
		.bucket_helper()
		.get_existing_bucket(bucket_id)
		.await?;
	let bucket_state = bucket.state.as_option().unwrap();

	// If the bucket has no other aliases, this is a true deletion.
	// Otherwise, it is just an alias removal.

	let has_other_global_aliases = bucket_state
		.aliases
		.items()
		.iter()
		.filter(|(_, _, active)| *active)
		.any(|(n, _, _)| is_local_alias || (*n != bucket_name));

	let has_other_local_aliases = bucket_state
		.local_aliases
		.items()
		.iter()
		.filter(|(_, _, active)| *active)
		.any(|((k, n), _, _)| !is_local_alias || *n != bucket_name || *k != api_key.key_id);

	if !has_other_global_aliases && !has_other_local_aliases {
		// Delete bucket

		// Check bucket is empty
		let objects = garage
			.object_table
			.get_range(&bucket_id, None, Some(ObjectFilter::IsData), 10)
			.await?;
		if !objects.is_empty() {
			return Err(Error::BucketNotEmpty);
		}

		// --- done checking, now commit ---
		// 1. delete bucket alias
		if is_local_alias {
			garage
				.bucket_helper()
				.unset_local_bucket_alias(bucket_id, &api_key.key_id, &bucket_name)
				.await?;
		} else {
			garage
				.bucket_helper()
				.unset_global_bucket_alias(bucket_id, &bucket_name)
				.await?;
		}

		// 2. delete authorization from keys that had access
		for (key_id, _) in bucket.authorized_keys() {
			garage
				.bucket_helper()
				.set_bucket_key_permissions(bucket.id, key_id, BucketKeyPerm::NO_PERMISSIONS)
				.await?;
		}

		// 3. delete bucket
		bucket.state = Deletable::delete();
		garage.bucket_table.insert(&bucket).await?;
	} else if is_local_alias {
		// Just unalias
		garage
			.bucket_helper()
			.unset_local_bucket_alias(bucket_id, &api_key.key_id, &bucket_name)
			.await?;
	} else {
		// Just unalias (but from global namespace)
		garage
			.bucket_helper()
			.unset_global_bucket_alias(bucket_id, &bucket_name)
			.await?;
	}

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(Body::empty())?)
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
		println!("{:?}", item);
		if item.has_tag_name("LocationConstraint") {
			if ret != None {
				return None;
			}
			ret = Some(item.text()?.to_string());
		} else if !item.is_text() {
			return None;
		}
	}

	Some(ret)
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
