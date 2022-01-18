use std::cmp::{max, min};
use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::Future;
use hyper::header;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};

use garage_util::data::*;
use garage_util::error::Error as GarageError;

use garage_model::garage::Garage;
use garage_model::key_table::Key;

use crate::error::*;
use crate::signature::payload::check_payload_signature;

use crate::helpers::*;
use crate::s3_bucket::*;
use crate::s3_copy::*;
use crate::s3_delete::*;
use crate::s3_get::*;
use crate::s3_list::*;
use crate::s3_put::*;
use crate::s3_router::{Authorization, Endpoint};
use crate::s3_website::*;

/// Run the S3 API server
pub async fn run_api_server(
	garage: Arc<Garage>,
	shutdown_signal: impl Future<Output = ()>,
) -> Result<(), GarageError> {
	let addr = &garage.config.s3_api.api_bind_addr;

	let service = make_service_fn(|conn: &AddrStream| {
		let garage = garage.clone();
		let client_addr = conn.remote_addr();
		async move {
			Ok::<_, GarageError>(service_fn(move |req: Request<Body>| {
				let garage = garage.clone();
				handler(garage, req, client_addr)
			}))
		}
	});

	let server = Server::bind(addr).serve(service);

	let graceful = server.with_graceful_shutdown(shutdown_signal);
	info!("API server listening on http://{}", addr);

	graceful.await?;
	Ok(())
}

async fn handler(
	garage: Arc<Garage>,
	req: Request<Body>,
	addr: SocketAddr,
) -> Result<Response<Body>, GarageError> {
	let uri = req.uri().clone();
	info!("{} {} {}", addr, req.method(), uri);
	debug!("{:?}", req);
	match handler_inner(garage.clone(), req).await {
		Ok(x) => {
			debug!("{} {:?}", x.status(), x.headers());
			Ok(x)
		}
		Err(e) => {
			let body: Body = Body::from(e.aws_xml(&garage.config.s3_api.s3_region, uri.path()));
			let mut http_error_builder = Response::builder()
				.status(e.http_status_code())
				.header("Content-Type", "application/xml");

			if let Some(header_map) = http_error_builder.headers_mut() {
				e.add_headers(header_map)
			}

			let http_error = http_error_builder.body(body)?;

			if e.http_status_code().is_server_error() {
				warn!("Response: error {}, {}", e.http_status_code(), e);
			} else {
				info!("Response: error {}, {}", e.http_status_code(), e);
			}
			Ok(http_error)
		}
	}
}

async fn handler_inner(garage: Arc<Garage>, req: Request<Body>) -> Result<Response<Body>, Error> {
	let (api_key, content_sha256) = check_payload_signature(&garage, &req).await?;
	let api_key = api_key.ok_or_else(|| {
		Error::Forbidden("Garage does not support anonymous access yet".to_string())
	})?;

	let authority = req
		.headers()
		.get(header::HOST)
		.ok_or_else(|| Error::BadRequest("HOST header required".to_owned()))?
		.to_str()?;

	let host = authority_to_host(authority)?;

	let bucket = garage
		.config
		.s3_api
		.root_domain
		.as_ref()
		.and_then(|root_domain| host_to_bucket(&host, root_domain));

	let (endpoint, bucket) = Endpoint::from_request(&req, bucket.map(ToOwned::to_owned))?;
	debug!("Endpoint: {:?}", endpoint);

	let bucket_name = match bucket {
		None => return handle_request_without_bucket(garage, req, api_key, endpoint).await,
		Some(bucket) => bucket.to_string(),
	};

	// Special code path for CreateBucket API endpoint
	if let Endpoint::CreateBucket {} = endpoint {
		return handle_create_bucket(&garage, req, content_sha256, api_key, bucket_name).await;
	}

	let bucket_id = resolve_bucket(&garage, &bucket_name, &api_key).await?;

	let allowed = match endpoint.authorization_type() {
		Authorization::Read => api_key.allow_read(&bucket_id),
		Authorization::Write => api_key.allow_write(&bucket_id),
		Authorization::Owner => api_key.allow_owner(&bucket_id),
		_ => unreachable!(),
	};

	if !allowed {
		return Err(Error::Forbidden(
			"Operation is not allowed for this key.".to_string(),
		));
	}

	match endpoint {
		Endpoint::HeadObject { key, .. } => handle_head(garage, &req, bucket_id, &key).await,
		Endpoint::GetObject { key, .. } => handle_get(garage, &req, bucket_id, &key).await,
		Endpoint::UploadPart {
			key,
			part_number,
			upload_id,
		} => {
			handle_put_part(
				garage,
				req,
				bucket_id,
				&key,
				part_number,
				&upload_id,
				content_sha256,
			)
			.await
		}
		Endpoint::CopyObject { key } => handle_copy(garage, &api_key, &req, bucket_id, &key).await,
		Endpoint::UploadPartCopy {
			key,
			part_number,
			upload_id,
		} => {
			handle_upload_part_copy(
				garage,
				&api_key,
				&req,
				bucket_id,
				&key,
				part_number,
				&upload_id,
			)
			.await
		}
		Endpoint::PutObject { key } => {
			handle_put(garage, req, bucket_id, &key, &api_key, content_sha256).await
		}
		Endpoint::AbortMultipartUpload { key, upload_id } => {
			handle_abort_multipart_upload(garage, bucket_id, &key, &upload_id).await
		}
		Endpoint::DeleteObject { key, .. } => handle_delete(garage, bucket_id, &key).await,
		Endpoint::CreateMultipartUpload { key } => {
			handle_create_multipart_upload(garage, &req, &bucket_name, bucket_id, &key).await
		}
		Endpoint::CompleteMultipartUpload { key, upload_id } => {
			handle_complete_multipart_upload(
				garage,
				req,
				&bucket_name,
				bucket_id,
				&key,
				&upload_id,
				content_sha256,
			)
			.await
		}
		Endpoint::CreateBucket {} => unreachable!(),
		Endpoint::HeadBucket {} => {
			let empty_body: Body = Body::from(vec![]);
			let response = Response::builder().body(empty_body).unwrap();
			Ok(response)
		}
		Endpoint::DeleteBucket {} => {
			handle_delete_bucket(&garage, bucket_id, bucket_name, api_key).await
		}
		Endpoint::GetBucketLocation {} => handle_get_bucket_location(garage),
		Endpoint::GetBucketVersioning {} => handle_get_bucket_versioning(),
		Endpoint::ListObjects {
			delimiter,
			encoding_type,
			marker,
			max_keys,
			prefix,
		} => {
			handle_list(
				garage,
				&ListObjectsQuery {
					common: ListQueryCommon {
						bucket_name,
						bucket_id,
						delimiter: delimiter.map(|d| d.to_string()),
						page_size: max_keys.map(|p| min(1000, max(1, p))).unwrap_or(1000),
						prefix: prefix.unwrap_or_default(),
						urlencode_resp: encoding_type.map(|e| e == "url").unwrap_or(false),
					},
					is_v2: false,
					marker,
					continuation_token: None,
					start_after: None,
				},
			)
			.await
		}
		Endpoint::ListObjectsV2 {
			delimiter,
			encoding_type,
			max_keys,
			prefix,
			continuation_token,
			start_after,
			list_type,
			..
		} => {
			if list_type == "2" {
				handle_list(
					garage,
					&ListObjectsQuery {
						common: ListQueryCommon {
							bucket_name,
							bucket_id,
							delimiter: delimiter.map(|d| d.to_string()),
							page_size: max_keys.map(|p| min(1000, max(1, p))).unwrap_or(1000),
							urlencode_resp: encoding_type.map(|e| e == "url").unwrap_or(false),
							prefix: prefix.unwrap_or_default(),
						},
						is_v2: true,
						marker: None,
						continuation_token,
						start_after,
					},
				)
				.await
			} else {
				Err(Error::BadRequest(format!(
					"Invalid endpoint: list-type={}",
					list_type
				)))
			}
		}
		Endpoint::ListMultipartUploads {
			delimiter,
			encoding_type,
			key_marker,
			max_uploads,
			prefix,
			upload_id_marker,
		} => {
			handle_list_multipart_upload(
				garage,
				&ListMultipartUploadsQuery {
					common: ListQueryCommon {
						bucket_name,
						bucket_id,
						delimiter: delimiter.map(|d| d.to_string()),
						page_size: max_uploads.map(|p| min(1000, max(1, p))).unwrap_or(1000),
						prefix: prefix.unwrap_or_default(),
						urlencode_resp: encoding_type.map(|e| e == "url").unwrap_or(false),
					},
					key_marker,
					upload_id_marker,
				},
			)
			.await
		}
		Endpoint::DeleteObjects {} => {
			handle_delete_objects(garage, bucket_id, req, content_sha256).await
		}
		Endpoint::GetBucketWebsite {} => handle_get_website(garage, bucket_id).await,
		Endpoint::PutBucketWebsite {} => {
			handle_put_website(garage, bucket_id, req, content_sha256).await
		}
		Endpoint::DeleteBucketWebsite {} => handle_delete_website(garage, bucket_id).await,
		endpoint => Err(Error::NotImplemented(endpoint.name().to_owned())),
	}
}

async fn handle_request_without_bucket(
	garage: Arc<Garage>,
	_req: Request<Body>,
	api_key: Key,
	endpoint: Endpoint,
) -> Result<Response<Body>, Error> {
	match endpoint {
		Endpoint::ListBuckets => handle_list_buckets(&garage, &api_key).await,
		endpoint => Err(Error::NotImplemented(endpoint.name().to_owned())),
	}
}

#[allow(clippy::ptr_arg)]
pub async fn resolve_bucket(
	garage: &Garage,
	bucket_name: &String,
	api_key: &Key,
) -> Result<Uuid, Error> {
	let api_key_params = api_key
		.state
		.as_option()
		.ok_or_internal_error("Key should not be deleted at this point")?;

	if let Some(Some(bucket_id)) = api_key_params.local_aliases.get(bucket_name) {
		Ok(*bucket_id)
	} else {
		Ok(garage
			.bucket_helper()
			.resolve_global_bucket_name(bucket_name)
			.await?
			.ok_or(Error::NoSuchBucket)?)
	}
}

/// Extract the bucket name and the key name from an HTTP path and possibly a bucket provided in
/// the host header of the request
///
/// S3 internally manages only buckets and keys. This function splits
/// an HTTP path to get the corresponding bucket name and key.
pub fn parse_bucket_key<'a>(
	path: &'a str,
	host_bucket: Option<&'a str>,
) -> Result<(&'a str, Option<&'a str>), Error> {
	let path = path.trim_start_matches('/');

	if let Some(bucket) = host_bucket {
		if !path.is_empty() {
			return Ok((bucket, Some(path)));
		} else {
			return Ok((bucket, None));
		}
	}

	let (bucket, key) = match path.find('/') {
		Some(i) => {
			let key = &path[i + 1..];
			if !key.is_empty() {
				(&path[..i], Some(key))
			} else {
				(&path[..i], None)
			}
		}
		None => (path, None),
	};
	if bucket.is_empty() {
		return Err(Error::BadRequest("No bucket specified".to_string()));
	}
	Ok((bucket, key))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_bucket_containing_a_key() -> Result<(), Error> {
		let (bucket, key) = parse_bucket_key("/my_bucket/a/super/file.jpg", None)?;
		assert_eq!(bucket, "my_bucket");
		assert_eq!(key.expect("key must be set"), "a/super/file.jpg");
		Ok(())
	}

	#[test]
	fn parse_bucket_containing_no_key() -> Result<(), Error> {
		let (bucket, key) = parse_bucket_key("/my_bucket/", None)?;
		assert_eq!(bucket, "my_bucket");
		assert!(key.is_none());
		let (bucket, key) = parse_bucket_key("/my_bucket", None)?;
		assert_eq!(bucket, "my_bucket");
		assert!(key.is_none());
		Ok(())
	}

	#[test]
	fn parse_bucket_containing_no_bucket() {
		let parsed = parse_bucket_key("", None);
		assert!(parsed.is_err());
		let parsed = parse_bucket_key("/", None);
		assert!(parsed.is_err());
		let parsed = parse_bucket_key("////", None);
		assert!(parsed.is_err());
	}

	#[test]
	fn parse_bucket_with_vhost_and_key() -> Result<(), Error> {
		let (bucket, key) = parse_bucket_key("/a/super/file.jpg", Some("my-bucket"))?;
		assert_eq!(bucket, "my-bucket");
		assert_eq!(key.expect("key must be set"), "a/super/file.jpg");
		Ok(())
	}

	#[test]
	fn parse_bucket_with_vhost_no_key() -> Result<(), Error> {
		let (bucket, key) = parse_bucket_key("", Some("my-bucket"))?;
		assert_eq!(bucket, "my-bucket");
		assert!(key.is_none());
		let (bucket, key) = parse_bucket_key("/", Some("my-bucket"))?;
		assert_eq!(bucket, "my-bucket");
		assert!(key.is_none());
		Ok(())
	}
}
