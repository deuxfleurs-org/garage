use std::sync::Arc;

use hyper::{Body, Request, Response, StatusCode};

use garage_util::data::*;
use garage_util::time::*;

use garage_model::garage::Garage;
use garage_model::s3::object_table::*;

use crate::error::*;
use crate::s3::xml as s3_xml;
use crate::signature::verify_signed_content;

async fn handle_delete_internal(
	garage: &Garage,
	bucket_id: Uuid,
	key: &str,
) -> Result<(Uuid, Uuid), Error> {
	let object = garage
		.object_table
		.get(&bucket_id, &key.to_string())
		.await?
		.ok_or(Error::NoSuchKey)?; // No need to delete

	let interesting_versions = object.versions().iter().filter(|v| {
		!matches!(
			v.state,
			ObjectVersionState::Aborted
				| ObjectVersionState::Complete(ObjectVersionData::DeleteMarker)
		)
	});

	let mut version_to_delete = None;
	let mut timestamp = now_msec();
	for v in interesting_versions {
		if v.timestamp + 1 > timestamp || version_to_delete.is_none() {
			version_to_delete = Some(v.uuid);
		}
		timestamp = std::cmp::max(timestamp, v.timestamp + 1);
	}

	let deleted_version = version_to_delete.ok_or(Error::NoSuchKey)?;

	let version_uuid = gen_uuid();

	let object = Object::new(
		bucket_id,
		key.into(),
		vec![ObjectVersion {
			uuid: version_uuid,
			timestamp,
			state: ObjectVersionState::Complete(ObjectVersionData::DeleteMarker),
		}],
	);

	garage.object_table.insert(&object).await?;

	Ok((deleted_version, version_uuid))
}

pub async fn handle_delete(
	garage: Arc<Garage>,
	bucket_id: Uuid,
	key: &str,
) -> Result<Response<Body>, Error> {
	let (_deleted_version, delete_marker_version) =
		handle_delete_internal(&garage, bucket_id, key).await?;

	Ok(Response::builder()
		.header("x-amz-version-id", hex::encode(delete_marker_version))
		.status(StatusCode::NO_CONTENT)
		.body(Body::from(vec![]))
		.unwrap())
}

pub async fn handle_delete_objects(
	garage: Arc<Garage>,
	bucket_id: Uuid,
	req: Request<Body>,
	content_sha256: Option<Hash>,
) -> Result<Response<Body>, Error> {
	let body = hyper::body::to_bytes(req.into_body()).await?;

	if let Some(content_sha256) = content_sha256 {
		verify_signed_content(content_sha256, &body[..])?;
	}

	let cmd_xml = roxmltree::Document::parse(std::str::from_utf8(&body)?)?;
	let cmd = parse_delete_objects_xml(&cmd_xml).ok_or_bad_request("Invalid delete XML query")?;

	let mut ret_deleted = Vec::new();
	let mut ret_errors = Vec::new();

	for obj in cmd.objects.iter() {
		match handle_delete_internal(&garage, bucket_id, &obj.key).await {
			Ok((deleted_version, delete_marker_version)) => {
				if cmd.quiet {
					continue;
				}
				ret_deleted.push(s3_xml::Deleted {
					key: s3_xml::Value(obj.key.clone()),
					version_id: s3_xml::Value(hex::encode(deleted_version)),
					delete_marker_version_id: s3_xml::Value(hex::encode(delete_marker_version)),
				});
			}
			Err(e) => {
				ret_errors.push(s3_xml::DeleteError {
					code: s3_xml::Value(e.aws_code().to_string()),
					key: Some(s3_xml::Value(obj.key.clone())),
					message: s3_xml::Value(format!("{}", e)),
					version_id: None,
				});
			}
		}
	}

	let xml = s3_xml::to_xml_with_header(&s3_xml::DeleteResult {
		xmlns: (),
		deleted: ret_deleted,
		errors: ret_errors,
	})?;

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(xml))?)
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
