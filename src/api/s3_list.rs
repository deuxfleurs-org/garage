use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use hyper::{Body, Response};

use garage_util::error::Error;

use garage_model::garage::Garage;
use garage_model::object_table::*;

use garage_table::DeletedFilter;

use crate::encoding::*;

#[derive(Debug)]
struct ListResultInfo {
	last_modified: u64,
	size: u64,
	etag: String,
}

pub async fn handle_list(
	garage: Arc<Garage>,
	bucket: &str,
	delimiter: &str,
	max_keys: usize,
	prefix: &str,
	marker: Option<&str>,
	urlencode_resp: bool,
) -> Result<Response<Body>, Error> {
	let mut result_keys = BTreeMap::<String, ListResultInfo>::new();
	let mut result_common_prefixes = BTreeSet::<String>::new();

	let mut next_chunk_start = marker.unwrap_or(prefix).to_string();

	debug!("List request: `{}` {} `{}`", delimiter, max_keys, prefix);

	let truncated;
	'query_loop: loop {
		let objects = garage
			.object_table
			.get_range(
				&bucket.to_string(),
				Some(next_chunk_start.clone()),
				Some(DeletedFilter::NotDeleted),
				max_keys + 1,
			)
			.await?;
		debug!(
			"List: get range {} (max {}), results: {}",
			next_chunk_start,
			max_keys + 1,
			objects.len()
		);

		for object in objects.iter() {
			if !object.key.starts_with(prefix) {
				truncated = None;
				break 'query_loop;
			}
			if let Some(version) = object.versions().iter().find(|x| x.is_data()) {
				if result_keys.len() + result_common_prefixes.len() >= max_keys {
					truncated = Some(object.key.to_string());
					break 'query_loop;
				}
				let common_prefix = if delimiter.len() > 0 {
					let relative_key = &object.key[prefix.len()..];
					relative_key
						.find(delimiter)
						.map(|i| &object.key[..prefix.len() + i + delimiter.len()])
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
							return Err(Error::Message(format!("Duplicate key?? {}", object.key)))
						}
					};
					result_keys.insert(object.key.clone(), info);
				};
			}
		}
		if objects.len() < max_keys + 1 {
			truncated = None;
			break 'query_loop;
		}
		if objects.len() > 0 {
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
	writeln!(&mut xml, "\t<Name>{}</Name>", bucket).unwrap();
	writeln!(&mut xml, "\t<Prefix>{}</Prefix>", prefix).unwrap();
	if let Some(mkr) = marker {
		writeln!(&mut xml, "\t<Marker>{}</Marker>", mkr).unwrap();
	}
	writeln!(&mut xml, "\t<KeyCount>{}</KeyCount>", result_keys.len()).unwrap();
	writeln!(&mut xml, "\t<MaxKeys>{}</MaxKeys>", max_keys).unwrap();
	writeln!(
		&mut xml,
		"\t<IsTruncated>{}</IsTruncated>",
		truncated.is_some()
	)
	.unwrap();
	if let Some(next_marker) = truncated {
		writeln!(&mut xml, "\t<NextMarker>{}</NextMarker>", next_marker).unwrap();
	}
	for (key, info) in result_keys.iter() {
		let last_modif = NaiveDateTime::from_timestamp(info.last_modified as i64 / 1000, 0);
		let last_modif = DateTime::<Utc>::from_utc(last_modif, Utc);
		let last_modif = last_modif.to_rfc3339_opts(SecondsFormat::Millis, true);
		writeln!(&mut xml, "\t<Contents>").unwrap();
		writeln!(
			&mut xml,
			"\t\t<Key>{}</Key>",
			xml_escape(key),
			//xml_encode_key(key, urlencode_resp)       // doesn't work with nextcloud, wtf
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
	if result_common_prefixes.len() > 0 {
		writeln!(&mut xml, "\t<CommonPrefixes>").unwrap();
		for pfx in result_common_prefixes.iter() {
			writeln!(
				&mut xml,
				"\t\t<Prefix>{}</Prefix>",
				xml_escape(pfx),
				//xml_encode_key(pfx, urlencode_resp)
			)
			.unwrap();
		}
		writeln!(&mut xml, "\t</CommonPrefixes>").unwrap();
	}
	writeln!(&mut xml, "</ListBucketResult>").unwrap();
	println!("{}", xml);

	Ok(Response::new(Body::from(xml.into_bytes())))
}
