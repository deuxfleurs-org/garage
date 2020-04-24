use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use hyper::Response;

use garage_util::error::Error;

use garage_core::garage::Garage;
use garage_core::object_table::*;

use crate::api_server::BodyType;
use crate::http_util::*;

#[derive(Debug)]
struct ListResultInfo {
	last_modified: u64,
	size: u64,
}

pub async fn handle_list(
	garage: Arc<Garage>,
	bucket: &str,
	delimiter: &str,
	max_keys: usize,
	prefix: &str,
) -> Result<Response<BodyType>, Error> {
	let mut result = BTreeMap::<String, ListResultInfo>::new();
	let mut truncated = true;
	let mut next_chunk_start = prefix.to_string();

	debug!("List request: `{}` {} `{}`", delimiter, max_keys, prefix);

	while result.len() < max_keys && truncated {
		let objects = garage
			.object_table
			.get_range(
				&bucket.to_string(),
				Some(next_chunk_start.clone()),
				Some(()),
				max_keys,
			)
			.await?;
		for object in objects.iter() {
			if let Some(version) = object
				.versions()
				.iter()
				.find(|x| x.is_complete && x.data != ObjectVersionData::DeleteMarker)
			{
				let relative_key = match object.key.starts_with(prefix) {
					true => &object.key[prefix.len()..],
					false => {
						truncated = false;
						break;
					}
				};
				let delimited_key = match relative_key.find(delimiter) {
					Some(i) => relative_key.split_at(i).1,
					None => &relative_key,
				};
				let delimited_key = delimited_key.to_string();
				let new_info = match result.get(&delimited_key) {
					None => ListResultInfo {
						last_modified: version.timestamp,
						size: version.size,
					},
					Some(lri) => ListResultInfo {
						last_modified: std::cmp::max(version.timestamp, lri.last_modified),
						size: 0,
					},
				};
				result.insert(delimited_key, new_info);
			}
		}
		if objects.len() < max_keys {
			truncated = false;
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
	writeln!(&mut xml, "\t<Bucket>{}</Bucket>", bucket).unwrap();
	writeln!(&mut xml, "\t<Prefix>{}</Prefix>", prefix).unwrap();
	writeln!(&mut xml, "\t<KeyCount>{}</KeyCount>", result.len()).unwrap();
	writeln!(&mut xml, "\t<MaxKeys>{}</MaxKeys>", max_keys).unwrap();
	writeln!(&mut xml, "\t<IsTruncated>{}</IsTruncated>", truncated).unwrap();
	for (key, info) in result.iter() {
		let last_modif = NaiveDateTime::from_timestamp(info.last_modified as i64 / 1000, 0);
		let last_modif = DateTime::<Utc>::from_utc(last_modif, Utc);
		let last_modif = last_modif.to_rfc3339_opts(SecondsFormat::Millis, true);
		writeln!(&mut xml, "\t<Contents>").unwrap();
		writeln!(&mut xml, "\t\t<Key>{}</Key>", xml_escape(key)).unwrap();
		writeln!(&mut xml, "\t\t<LastModified>{}</LastModified>", last_modif).unwrap();
		writeln!(&mut xml, "\t\t<Size>{}</Size>", info.size).unwrap();
		writeln!(&mut xml, "\t\t<StorageClass>STANDARD</StorageClass>").unwrap();
		writeln!(&mut xml, "\t</Contents>").unwrap();
	}
	writeln!(&mut xml, "</ListBucketResult>").unwrap();

	Ok(Response::new(Box::new(BytesBody::from(xml.into_bytes()))))
}

fn xml_escape(s: &str) -> String {
	s.replace("<", "&lt;")
		.replace(">", "&gt;")
		.replace("\"", "&quot;")
}
