use std::fmt::Write;
use std::sync::Arc;

use hyper::{Body, Request, Response};

use garage_util::data::*;
use garage_util::time::*;

use garage_model::garage::Garage;
use garage_model::object_table::*;

use crate::encoding::*;
use crate::error::*;
use crate::signature::verify_signed_content;

async fn handle_delete_internal(
	garage: &Garage,
	bucket: &str,
	key: &str,
) -> Result<(UUID, UUID), Error> {
	let object = garage
		.object_table
		.get(&bucket.to_string(), &key.to_string())
		.await?
		.ok_or(Error::NotFound)?; // No need to delete

	let interesting_versions = object.versions().iter().filter(|v| match v.state {
		ObjectVersionState::Aborted => false,
		ObjectVersionState::Complete(ObjectVersionData::DeleteMarker) => false,
		_ => true,
	});

	let mut version_to_delete = None;
	let mut timestamp = now_msec();
	for v in interesting_versions {
		if v.timestamp + 1 > timestamp || version_to_delete.is_none() {
			version_to_delete = Some(v.uuid);
		}
		timestamp = std::cmp::max(timestamp, v.timestamp + 1);
	}

	let deleted_version = version_to_delete.ok_or(Error::NotFound)?;

	let version_uuid = gen_uuid();

	let object = Object::new(
		bucket.into(),
		key.into(),
		vec![ObjectVersion {
			uuid: version_uuid,
			timestamp,
			state: ObjectVersionState::Complete(ObjectVersionData::DeleteMarker),
		}],
	);

	garage.object_table.insert(&object).await?;
	return Ok((deleted_version, version_uuid));
}

pub async fn handle_delete(
	garage: Arc<Garage>,
	bucket: &str,
	key: &str,
) -> Result<Response<Body>, Error> {
	let (_deleted_version, delete_marker_version) =
		handle_delete_internal(&garage, bucket, key).await?;

	Ok(Response::builder()
		.header("x-amz-version-id", hex::encode(delete_marker_version))
		.body(Body::from(vec![]))
		.unwrap())
}

pub async fn handle_delete_objects(
	garage: Arc<Garage>,
	bucket: &str,
	req: Request<Body>,
	content_sha256: Option<Hash>,
) -> Result<Response<Body>, Error> {
	let body = hyper::body::to_bytes(req.into_body()).await?;
	verify_signed_content(content_sha256, &body[..])?;

	let cmd_xml = roxmltree::Document::parse(&std::str::from_utf8(&body)?)?;
	let cmd = parse_delete_objects_xml(&cmd_xml).ok_or_bad_request("Invalid delete XML query")?;

	let mut retxml = String::new();
	writeln!(&mut retxml, r#"<?xml version="1.0" encoding="UTF-8"?>"#).unwrap();
	writeln!(
		&mut retxml,
		r#"<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#
	)
	.unwrap();

	for obj in cmd.objects.iter() {
		match handle_delete_internal(&garage, bucket, &obj.key).await {
			Ok((deleted_version, delete_marker_version)) => {
				if cmd.quiet {
					continue;
				}
				writeln!(&mut retxml, "\t<Deleted>").unwrap();
				writeln!(&mut retxml, "\t\t<Key>{}</Key>", xml_escape(&obj.key)).unwrap();
				writeln!(
					&mut retxml,
					"\t\t<VersionId>{}</VersionId>",
					hex::encode(deleted_version)
				)
				.unwrap();
				writeln!(
					&mut retxml,
					"\t\t<DeleteMarkerVersionId>{}</DeleteMarkerVersionId>",
					hex::encode(delete_marker_version)
				)
				.unwrap();
				writeln!(&mut retxml, "\t</Deleted>").unwrap();
			}
			Err(e) => {
				writeln!(&mut retxml, "\t<Error>").unwrap();
				writeln!(&mut retxml, "\t\t<Code>{}</Code>", e.http_status_code()).unwrap();
				writeln!(&mut retxml, "\t\t<Key>{}</Key>", xml_escape(&obj.key)).unwrap();
				writeln!(
					&mut retxml,
					"\t\t<Message>{}</Message>",
					xml_escape(&format!("{}", e))
				)
				.unwrap();
				writeln!(&mut retxml, "\t</Error>").unwrap();
			}
		}
	}

	writeln!(&mut retxml, "</DeleteResult>").unwrap();

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(retxml.into_bytes()))?)
}

struct DeleteRequest {
	quiet: bool,
	objects: Vec<DeleteObject>,
}

struct DeleteObject {
	key: String,
}

fn parse_delete_objects_xml(xml: &roxmltree::Document) -> Option<DeleteRequest> {
	let mut ret = DeleteRequest {
		quiet: false,
		objects: vec![],
	};

	let root = xml.root();
	let delete = root.first_child()?;

	if !delete.has_tag_name("Delete") {
		return None;
	}

	for item in delete.children() {
		if item.has_tag_name("Object") {
			let key = item.children().find(|e| e.has_tag_name("Key"))?;
			let key_str = key.text()?;
			ret.objects.push(DeleteObject {
				key: key_str.to_string(),
			});
		} else if item.has_tag_name("Quiet") {
			if item.text()? == "true" {
				ret.quiet = true;
			} else {
				ret.quiet = false;
			}
		} else {
			return None;
		}
	}

	Some(ret)
}
