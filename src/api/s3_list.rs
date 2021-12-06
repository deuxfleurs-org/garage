use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use hyper::{Body, Response};

use garage_util::error::Error as GarageError;
use garage_util::time::*;

use garage_model::garage::Garage;
use garage_model::object_table::*;

use garage_table::DeletedFilter;

use crate::encoding::*;
use crate::error::*;
use crate::s3_xml;

#[derive(Debug)]
pub struct ListObjectsQuery {
	pub is_v2: bool,
	pub bucket: String,
	pub delimiter: Option<String>,
	pub max_keys: usize,
	pub prefix: String,
	pub marker: Option<String>,
	pub continuation_token: Option<String>,
	pub start_after: Option<String>,
	pub urlencode_resp: bool,
}

#[derive(Debug)]
struct ListResultInfo {
	last_modified: u64,
	size: u64,
	etag: String,
}

pub async fn handle_list(
	garage: Arc<Garage>,
	query: &ListObjectsQuery,
) -> Result<Response<Body>, Error> {
	let mut result_keys = BTreeMap::<String, ListResultInfo>::new();
	let mut result_common_prefixes = BTreeSet::<String>::new();

	// Determine the key from where we want to start fetch objects
	// from the database, and whether the object at this key must
	// be included or excluded from the response.
	// This key can be the prefix in the base case, or intermediate
	// points in the dataset if we are continuing a previous listing.
	#[allow(clippy::collapsible_else_if)]
	let (mut next_chunk_start, mut next_chunk_exclude_start) = if query.is_v2 {
		if let Some(ct) = &query.continuation_token {
			// In V2 mode, the continuation token is defined as an opaque
			// string in the spec, so we can do whatever we want with it.
			// In our case, it is defined as either [ or ] (for include
			// and exclude, respectively), followed by a base64 string
			// representing the key to start with.
			let exclude = match &ct[..1] {
				"[" => false,
				"]" => true,
				_ => return Err(Error::BadRequest("Invalid continuation token".to_string())),
			};
			(
				String::from_utf8(base64::decode(ct[1..].as_bytes())?)?,
				exclude,
			)
		} else if let Some(sa) = &query.start_after {
			// StartAfter has defined semantics in the spec:
			// start listing at the first key immediately after.
			(sa.clone(), true)
		} else {
			// In the case where neither is specified, we start
			// listing at the specified prefix. If an object has this
			// exact same key, we include it. (TODO is this correct?)
			(query.prefix.clone(), false)
		}
	} else {
		if let Some(mk) = &query.marker {
			// In V1 mode, the spec defines the Marker value to mean
			// the same thing as the StartAfter value in V2 mode.
			(mk.clone(), true)
		} else {
			// Base case, same as in V2 mode
			(query.prefix.clone(), false)
		}
	};

	debug!(
		"List request: `{:?}` {} `{}`, start from {}, exclude first {}",
		query.delimiter, query.max_keys, query.prefix, next_chunk_start, next_chunk_exclude_start
	);

	// `truncated` is a boolean that determines whether there are
	// more items to be added.
	let truncated;
	// `last_processed_item` is the key of the last item
	// that was included in the listing before truncating.
	let mut last_processed_item = None;

	'query_loop: loop {
		// Fetch objects
		let objects = garage
			.object_table
			.get_range(
				&query.bucket,
				Some(next_chunk_start.clone()),
				Some(DeletedFilter::NotDeleted),
				query.max_keys + 1,
			)
			.await?;
		debug!(
			"List: get range {} (max {}), results: {}",
			next_chunk_start,
			query.max_keys + 1,
			objects.len()
		);
		let current_chunk_start = next_chunk_start.clone();

		// Iterate on returned objects and add them to the response.
		// If a delimiter is specified, we take care of grouping objects
		// into CommonPrefixes.
		for object in objects.iter() {
			// If we have retrieved an object that doesn't start with
			// the prefix, we know we have finished listing our stuff.
			if !object.key.starts_with(&query.prefix) {
				truncated = false;
				break 'query_loop;
			}

			// Exclude the starting key if we have to.
			if object.key == next_chunk_start && next_chunk_exclude_start {
				continue;
			}

			// Find if this object has a currently valid (non-deleted,
			// non-still-uploading) version. If not, skip it.
			let version = match object.versions().iter().find(|x| x.is_data()) {
				Some(v) => v,
				None => continue,
			};

			// If we don't have space to add this object to our response,
			// we will need to stop here and mark the key of this object
			// as the marker from where
			// we want to start again in the next list call.
			let cannot_add = result_keys.len() + result_common_prefixes.len() >= query.max_keys;

			// Determine whether this object should be grouped inside
			// a CommonPrefix because it contains the delimiter,
			// or if it should be returned as an object.
			let common_prefix = match &query.delimiter {
				Some(delimiter) => object.key[query.prefix.len()..]
					.find(delimiter)
					.map(|i| &object.key[..query.prefix.len() + i + delimiter.len()]),
				None => None,
			};
			if let Some(pfx) = common_prefix {
				// In the case where this object must be grouped in a
				// common prefix, handle it here.
				if !result_common_prefixes.contains(pfx) {
					// Determine the first listing key that starts after
					// the common prefix, by finding the next possible
					// string by alphabetical order.
					let mut first_key_after_prefix = pfx.to_string();
					let tail = first_key_after_prefix.pop().unwrap();
					first_key_after_prefix.push(((tail as u8) + 1) as char);

					// If this were the end of the chunk,
					// the next chunk should start after this prefix
					next_chunk_start = first_key_after_prefix;
					next_chunk_exclude_start = false;

					if cannot_add {
						truncated = true;
						break 'query_loop;
					}
					result_common_prefixes.insert(pfx.to_string());
				}
				last_processed_item = Some(object.key.clone());
				continue;
			};

			// This is not a common prefix, we want to add it to our
			// response directly.
			next_chunk_start = object.key.clone();

			if cannot_add {
				truncated = true;
				next_chunk_exclude_start = false;
				break 'query_loop;
			}

			let meta = match &version.state {
				ObjectVersionState::Complete(ObjectVersionData::Inline(meta, _)) => meta,
				ObjectVersionState::Complete(ObjectVersionData::FirstBlock(meta, _)) => meta,
				_ => unreachable!(),
			};
			let info = match result_keys.get(&object.key) {
				None => ListResultInfo {
					last_modified: version.timestamp,
					size: meta.size,
					etag: meta.etag.to_string(),
				},
				Some(_lri) => {
					return Err(Error::InternalError(GarageError::Message(format!(
						"Duplicate key?? {} (this is a bug, please report it)",
						object.key
					))))
				}
			};
			result_keys.insert(object.key.clone(), info);
			last_processed_item = Some(object.key.clone());
			next_chunk_exclude_start = true;
		}

		// If our database returned less objects than what we were asking for,
		// it means that no more objects are in the bucket. So we stop here.
		if objects.len() < query.max_keys + 1 {
			truncated = false;
			break 'query_loop;
		}

		// Sanity check: we should have added at least an object
		// or a prefix to our returned result.
		if next_chunk_start == current_chunk_start || last_processed_item.is_none() {
			return Err(Error::InternalError(GarageError::Message(format!(
							"S3 ListObject: made no progress, still starting at {} (this is a bug, please report it)", next_chunk_start))));
		}

		// Loop and fetch more objects
	}

	let mut result = s3_xml::ListBucketResult {
		xmlns: (),
		name: s3_xml::Value(query.bucket.to_string()),
		prefix: uriencode_maybe(&query.prefix, query.urlencode_resp),
		marker: None,
		next_marker: None,
		start_after: None,
		continuation_token: None,
		next_continuation_token: None,
		max_keys: s3_xml::IntValue(query.max_keys as i64),
		delimiter: query
			.delimiter
			.as_ref()
			.map(|x| uriencode_maybe(x, query.urlencode_resp)),
		encoding_type: match query.urlencode_resp {
			true => Some(s3_xml::Value("url".to_string())),
			false => None,
		},
		key_count: Some(s3_xml::IntValue(
			result_keys.len() as i64 + result_common_prefixes.len() as i64,
		)),
		is_truncated: s3_xml::Value(format!("{}", truncated)),
		contents: vec![],
		common_prefixes: vec![],
	};

	if query.is_v2 {
		if let Some(ct) = &query.continuation_token {
			result.continuation_token = Some(s3_xml::Value(ct.to_string()));
		}
		if let Some(sa) = &query.start_after {
			result.start_after = Some(uriencode_maybe(sa, query.urlencode_resp));
		}
		if truncated {
			let b64 = base64::encode(next_chunk_start.as_bytes());
			let nct = if next_chunk_exclude_start {
				format!("]{}", b64)
			} else {
				format!("[{}", b64)
			};
			result.next_continuation_token = Some(s3_xml::Value(nct));
		}
	} else {
		// TODO: are these supposed to be urlencoded when encoding-type is URL??
		if let Some(mkr) = &query.marker {
			result.marker = Some(uriencode_maybe(mkr, query.urlencode_resp));
		}
		if truncated {
			if let Some(lpi) = last_processed_item {
				result.next_marker = Some(uriencode_maybe(&lpi, query.urlencode_resp));
			} else {
				return Err(Error::InternalError(GarageError::Message(
								"S3 ListObject: last_processed_item is None but the response was truncated, indicating that many items were processed (this is a bug, please report it)".to_string())));
			}
		}
	}

	for (key, info) in result_keys.iter() {
		result.contents.push(s3_xml::ListBucketItem {
			key: uriencode_maybe(key, query.urlencode_resp),
			last_modified: s3_xml::Value(msec_to_rfc3339(info.last_modified)),
			size: s3_xml::IntValue(info.size as i64),
			etag: s3_xml::Value(info.etag.to_string()),
			storage_class: s3_xml::Value("STANDARD".to_string()),
		});
	}

	for pfx in result_common_prefixes.iter() {
		result.common_prefixes.push(s3_xml::CommonPrefix {
			prefix: uriencode_maybe(pfx, query.urlencode_resp),
		});
	}

	let xml = s3_xml::to_xml_with_header(&result)?;

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(xml.into_bytes()))?)
}

fn uriencode_maybe(s: &str, yes: bool) -> s3_xml::Value {
	if yes {
		s3_xml::Value(uri_encode(s, true))
	} else {
		s3_xml::Value(s.to_string())
	}
}
