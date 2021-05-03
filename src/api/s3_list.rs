use std::collections::{BTreeMap, BTreeSet, HashMap};
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

pub fn parse_list_objects_query(
	bucket: &str,
	params: &HashMap<String, String>,
) -> Result<ListObjectsQuery, Error> {
	Ok(ListObjectsQuery {
		is_v2: params.get("list-type").map(|x| x == "2").unwrap_or(false),
		bucket: bucket.to_string(),
		delimiter: params.get("delimiter").filter(|x| !x.is_empty()).cloned(),
		max_keys: params
			.get("max-keys")
			.map(|x| {
				x.parse::<usize>()
					.ok_or_bad_request("Invalid value for max-keys")
			})
			.unwrap_or(Ok(1000))?,
		prefix: params.get("prefix").cloned().unwrap_or_default(),
		marker: params.get("marker").cloned(),
		continuation_token: params.get("continuation-token").cloned(),
		start_after: params.get("start-after").cloned(),
		urlencode_resp: params
			.get("encoding-type")
			.map(|x| x == "url")
			.unwrap_or(false),
	})
}

pub async fn handle_list(
	garage: Arc<Garage>,
	query: &ListObjectsQuery,
) -> Result<Response<Body>, Error> {
	let mut result_keys = BTreeMap::<String, ListResultInfo>::new();
	let mut result_common_prefixes = BTreeSet::<String>::new();

	let mut next_chunk_start = if query.is_v2 {
		if let Some(ct) = &query.continuation_token {
			String::from_utf8(base64::decode(ct.as_bytes())?)?
		} else {
			query
				.start_after
				.clone()
				.unwrap_or_else(|| query.prefix.clone())
		}
	} else {
		query.marker.clone().unwrap_or_else(|| query.prefix.clone())
	};

	debug!(
		"List request: `{:?}` {} `{}`",
		query.delimiter, query.max_keys, query.prefix
	);

	let truncated;
	'query_loop: loop {
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

		for object in objects.iter() {
			if !object.key.starts_with(&query.prefix) {
				truncated = None;
				break 'query_loop;
			}

			if query.is_v2 && query.start_after.as_ref() == Some(&object.key) {
				continue;
			}

			if let Some(version) = object.versions().iter().find(|x| x.is_data()) {
				if result_keys.len() + result_common_prefixes.len() >= query.max_keys {
					truncated = Some(object.key.to_string());
					break 'query_loop;
				}
				let common_prefix = if let Some(delimiter) = &query.delimiter {
					let relative_key = &object.key[query.prefix.len()..];
					relative_key
						.find(delimiter)
						.map(|i| &object.key[..query.prefix.len() + i + delimiter.len()])
				} else {
					None
				};
				if let Some(pfx) = common_prefix {
					result_common_prefixes.insert(pfx.to_string());
				} else {
					let meta = match &version.state {
						ObjectVersionState::Complete(ObjectVersionData::Inline(meta, _)) => meta,
						ObjectVersionState::Complete(ObjectVersionData::FirstBlock(meta, _)) => {
							meta
						}
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
								"Duplicate key?? {}",
								object.key
							))))
						}
					};
					result_keys.insert(object.key.clone(), info);
				};
			}
		}
		if objects.len() < query.max_keys + 1 {
			truncated = None;
			break 'query_loop;
		}
		if !objects.is_empty() {
			next_chunk_start = objects[objects.len() - 1].key.clone();
		}
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
		is_truncated: s3_xml::Value(format!("{}", truncated.is_some())),
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
		if let Some(nct) = truncated {
			result.next_continuation_token = Some(s3_xml::Value(base64::encode(nct.as_bytes())));
		}
	} else {
		// TODO: are these supposed to be urlencoded when encoding-type is URL??
		if let Some(mkr) = &query.marker {
			result.marker = Some(uriencode_maybe(mkr, query.urlencode_resp));
		}
		if let Some(next_marker) = truncated {
			result.next_marker = Some(uriencode_maybe(&next_marker, query.urlencode_resp));
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
		//TODO: in V1, are these urlencoded when urlencode_resp is true ?? (proably)
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
