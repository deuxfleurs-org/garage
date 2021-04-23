use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write;
use std::sync::Arc;

use hyper::{Body, Response};

use garage_util::error::Error as GarageError;
use garage_util::time::*;

use garage_model::garage::Garage;
use garage_model::object_table::*;

use garage_table::DeletedFilter;

use crate::encoding::*;
use crate::error::*;

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

	let mut xml = String::new();
	writeln!(&mut xml, r#"<?xml version="1.0" encoding="UTF-8"?>"#).unwrap();
	writeln!(
		&mut xml,
		r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#
	)
	.unwrap();

	writeln!(&mut xml, "\t<Name>{}</Name>", query.bucket).unwrap();

	// TODO: in V1, is this supposed to be urlencoded when encoding-type is URL??
	writeln!(
		&mut xml,
		"\t<Prefix>{}</Prefix>",
		xml_encode_key(&query.prefix, query.urlencode_resp),
	)
	.unwrap();

	if let Some(delim) = &query.delimiter {
		// TODO: in V1, is this supposed to be urlencoded when encoding-type is URL??
		writeln!(
			&mut xml,
			"\t<Delimiter>{}</Delimiter>",
			xml_encode_key(delim, query.urlencode_resp),
		)
		.unwrap();
	}

	writeln!(&mut xml, "\t<MaxKeys>{}</MaxKeys>", query.max_keys).unwrap();
	if query.urlencode_resp {
		writeln!(&mut xml, "\t<EncodingType>url</EncodingType>").unwrap();
	}

	writeln!(
		&mut xml,
		"\t<KeyCount>{}</KeyCount>",
		result_keys.len() + result_common_prefixes.len()
	)
	.unwrap();
	writeln!(
		&mut xml,
		"\t<IsTruncated>{}</IsTruncated>",
		truncated.is_some()
	)
	.unwrap();

	if query.is_v2 {
		if let Some(ct) = &query.continuation_token {
			writeln!(&mut xml, "\t<ContinuationToken>{}</ContinuationToken>", ct).unwrap();
		}
		if let Some(sa) = &query.start_after {
			writeln!(
				&mut xml,
				"\t<StartAfter>{}</StartAfter>",
				xml_encode_key(sa, query.urlencode_resp)
			)
			.unwrap();
		}
		if let Some(nct) = truncated {
			writeln!(
				&mut xml,
				"\t<NextContinuationToken>{}</NextContinuationToken>",
				base64::encode(nct.as_bytes())
			)
			.unwrap();
		}
	} else {
		// TODO: are these supposed to be urlencoded when encoding-type is URL??
		if let Some(mkr) = &query.marker {
			writeln!(
				&mut xml,
				"\t<Marker>{}</Marker>",
				xml_encode_key(mkr, query.urlencode_resp)
			)
			.unwrap();
		}
		if let Some(next_marker) = truncated {
			writeln!(
				&mut xml,
				"\t<NextMarker>{}</NextMarker>",
				xml_encode_key(&next_marker, query.urlencode_resp)
			)
			.unwrap();
		}
	}

	for (key, info) in result_keys.iter() {
		let last_modif = msec_to_rfc3339(info.last_modified);
		writeln!(&mut xml, "\t<Contents>").unwrap();
		writeln!(
			&mut xml,
			"\t\t<Key>{}</Key>",
			xml_encode_key(key, query.urlencode_resp),
		)
		.unwrap();
		writeln!(&mut xml, "\t\t<LastModified>{}</LastModified>", last_modif).unwrap();
		writeln!(&mut xml, "\t\t<Size>{}</Size>", info.size).unwrap();
		if !info.etag.is_empty() {
			writeln!(&mut xml, "\t\t<ETag>\"{}\"</ETag>", info.etag).unwrap();
		}
		writeln!(&mut xml, "\t\t<StorageClass>STANDARD</StorageClass>").unwrap();
		writeln!(&mut xml, "\t</Contents>").unwrap();
	}

	for pfx in result_common_prefixes.iter() {
		writeln!(&mut xml, "\t<CommonPrefixes>").unwrap();
		//TODO: in V1, are these urlencoded when urlencode_resp is true ?? (proably)
		writeln!(
			&mut xml,
			"\t\t<Prefix>{}</Prefix>",
			xml_encode_key(pfx, query.urlencode_resp),
		)
		.unwrap();
		writeln!(&mut xml, "\t</CommonPrefixes>").unwrap();
	}

	writeln!(&mut xml, "</ListBucketResult>").unwrap();
	debug!("{}", xml);

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(xml.into_bytes()))?)
}
