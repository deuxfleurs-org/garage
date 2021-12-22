use std::collections::HashMap;
use std::sync::Arc;

use hyper::{Body, Response};

use garage_model::garage::Garage;
use garage_model::key_table::Key;
use garage_table::util::EmptyKey;
use garage_util::crdt::*;
use garage_util::time::*;

use crate::error::*;
use crate::s3_xml;

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
	let key_state = api_key.state.as_option().ok_or_internal_error(
		"Key should not be in deleted state at this point (internal error)",
	)?;

	// Collect buckets user has access to
	let ids = api_key
		.state
		.as_option()
		.unwrap()
		.authorized_buckets
		.items()
		.iter()
		.filter(|(_, perms)| perms.allow_read || perms.allow_write || perms.allow_owner)
		.map(|(id, _)| *id)
		.collect::<Vec<_>>();

	let mut buckets_by_id = HashMap::new();
	let mut aliases = HashMap::new();

	for bucket_id in ids.iter() {
		let bucket = garage.bucket_table.get(bucket_id, &EmptyKey).await?;
		if let Some(bucket) = bucket {
			if let Deletable::Present(param) = bucket.state {
				for (alias, _, active) in param.aliases.items() {
					if *active {
						let alias_ent = garage.bucket_alias_table.get(&EmptyKey, alias).await?;
						if let Some(alias_ent) = alias_ent {
							if let Some(alias_p) = alias_ent.state.get().as_option() {
								if alias_p.bucket_id == *bucket_id {
									aliases.insert(alias_ent.name().to_string(), *bucket_id);
								}
							}
						}
					}
				}
				buckets_by_id.insert(bucket_id, param);
			}
		}
	}

	for (alias, _, id) in key_state.local_aliases.items() {
		if let Some(id) = id.as_option() {
			aliases.insert(alias.clone(), *id);
		}
	}

	// Generate response
	let list_buckets = s3_xml::ListAllMyBucketsResult {
		owner: s3_xml::Owner {
			display_name: s3_xml::Value(api_key.name.get().to_string()),
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
