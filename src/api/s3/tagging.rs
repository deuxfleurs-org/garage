use quick_xml::de::from_reader;

use hyper::{Request, Response, StatusCode};

use garage_model::bucket_table::Bucket;
use garage_model::s3::object_table::{Object, ObjectVersionState};

use garage_api_common::helpers::*;
use garage_api_common::xml::tagging::*;

use crate::api_server::{ReqBody, ResBody};
use crate::error::*;
use crate::xml::to_xml_with_header;

const BUCKET_MAX_TAGS: usize = 10;
const OBJECT_MAX_TAGS: usize = 10;

pub async fn handle_get_bucket_tagging(ctx: ReqCtx) -> Result<Response<ResBody>, Error> {
	let ReqCtx { bucket_params, .. } = ctx;
	if let Some(tagging) = bucket_params.tags.get().inner() {
		let tagging = Tagging::from_map(tagging);
		let xml = to_xml_with_header(&tagging)?;
		Ok(Response::builder()
			.status(StatusCode::OK)
			.header(http::header::CONTENT_TYPE, "application/xml")
			.body(string_body(xml))?)
	} else {
		Ok(Response::builder()
			.status(StatusCode::NO_CONTENT)
			.body(empty_body())?)
	}
}

pub async fn handle_delete_bucket_tagging(ctx: ReqCtx) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage,
		bucket_id,
		mut bucket_params,
		..
	} = ctx;

	bucket_params.tags.update(None.into());
	garage
		.bucket_table
		.insert(&Bucket::present(bucket_id, bucket_params))
		.await?;

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(empty_body())?)
}

pub async fn handle_put_bucket_tagging(
	ctx: ReqCtx,
	req: Request<ReqBody>,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage,
		bucket_id,
		mut bucket_params,
		..
	} = ctx;

	let body = req.into_body().collect().await?;
	let tagging: Tagging = from_reader(&body as &[u8])?;

	bucket_params
		.tags
		.update(Some(tagging.into_map(BUCKET_MAX_TAGS)?).into());
	garage
		.bucket_table
		.insert(&Bucket::present(bucket_id, bucket_params))
		.await?;

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(empty_body())?)
}

// ----

pub async fn handle_get_object_tagging(
	ctx: AnonReqCtx,
	key: &String,
) -> Result<Response<ResBody>, Error> {
	let AnonReqCtx {
		garage, bucket_id, ..
	} = ctx;

	let object = garage
		.object_table
		.get(&bucket_id, &key.to_string())
		.await?
		.ok_or(Error::NoSuchKey)?;

	let object_version = object
		.versions()
		.iter()
		.rev()
		.find(|v| v.is_data())
		.ok_or(Error::NoSuchKey)?;

	let version_tags = match &object_version.state {
		ObjectVersionState::Complete { tags, .. } => tags,
		_ => unreachable!(),
	};

	if let Some(tagging) = version_tags.get().inner() {
		let tagging = Tagging::from_map(tagging);
		let xml = to_xml_with_header(&tagging)?;
		Ok(Response::builder()
			.status(StatusCode::OK)
			.header(http::header::CONTENT_TYPE, "application/xml")
			.body(string_body(xml))?)
	} else {
		Ok(Response::builder()
			.status(StatusCode::NO_CONTENT)
			.body(empty_body())?)
	}
}

pub async fn handle_delete_object_tagging(
	ctx: ReqCtx,
	key: &String,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = ctx;

	let object = garage
		.object_table
		.get(&bucket_id, &key.to_string())
		.await?
		.ok_or(Error::NoSuchKey)?;

	let mut object_version = object
		.versions()
		.iter()
		.rev()
		.find(|v| v.is_data())
		.ok_or(Error::NoSuchKey)?
		.clone();

	match &mut object_version.state {
		ObjectVersionState::Complete { tags, .. } => {
			tags.update(None.into());
		}
		_ => unreachable!(),
	}

	let object = Object::new(bucket_id, key.into(), vec![object_version]);
	garage.object_table.insert(&object).await?;

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(empty_body())?)
}

pub async fn handle_put_object_tagging(
	ctx: ReqCtx,
	key: &String,
	req: Request<ReqBody>,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = ctx;

	let body = req.into_body().collect().await?;
	let tagging: Tagging = from_reader(&body as &[u8])?;

	let object = garage
		.object_table
		.get(&bucket_id, &key.to_string())
		.await?
		.ok_or(Error::NoSuchKey)?;

	let mut object_version = object
		.versions()
		.iter()
		.rev()
		.find(|v| v.is_data())
		.ok_or(Error::NoSuchKey)?
		.clone();

	match &mut object_version.state {
		ObjectVersionState::Complete { tags, .. } => {
			tags.update(Some(tagging.into_map(OBJECT_MAX_TAGS)?).into());
		}
		_ => unreachable!(),
	}

	let object = Object::new(bucket_id, key.into(), vec![object_version]);
	garage.object_table.insert(&object).await?;

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(empty_body())?)
}
