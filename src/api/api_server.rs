use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::Future;
use hyper::header;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};

use garage_util::error::Error as GarageError;

use garage_model::garage::Garage;

use crate::error::*;
use crate::signature::check_signature;

use crate::helpers::*;
use crate::s3_bucket::*;
use crate::s3_copy::*;
use crate::s3_delete::*;
use crate::s3_get::*;
use crate::s3_list::*;
use crate::s3_put::*;
use crate::s3_router::{Authorization, Endpoint};

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
	let (api_key, content_sha256) = check_signature(&garage, &req).await?;

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

	let endpoint = Endpoint::from_request(&req, bucket.map(ToOwned::to_owned))?;
	let allowed = match endpoint.authorization_type() {
		Authorization::None => true,
		Authorization::Read(bucket) => api_key.allow_read(bucket),
		Authorization::Write(bucket) => api_key.allow_write(bucket),
	};

	if !allowed {
		return Err(Error::Forbidden(
			"Operation is not allowed for this key.".to_string(),
		));
	}

	match endpoint {
		Endpoint::ListBuckets => handle_list_buckets(&api_key),
		Endpoint::HeadObject { bucket, key, .. } => handle_head(garage, &req, &bucket, &key).await,
		Endpoint::GetObject { bucket, key, .. } => handle_get(garage, &req, &bucket, &key).await,
		Endpoint::UploadPart {
			bucket,
			key,
			part_number,
			upload_id,
		} => {
			handle_put_part(
				garage,
				req,
				&bucket,
				&key,
				part_number,
				&upload_id,
				content_sha256,
			)
			.await
		}
		Endpoint::CopyObject { bucket, key } => {
			let copy_source = req.headers().get("x-amz-copy-source").unwrap().to_str()?;
			let copy_source = percent_encoding::percent_decode_str(copy_source).decode_utf8()?;
			let (source_bucket, source_key) = parse_bucket_key(&copy_source, None)?;
			if !api_key.allow_read(source_bucket) {
				return Err(Error::Forbidden(format!(
					"Reading from bucket {} not allowed for this key",
					source_bucket
				)));
			}
			let source_key = source_key.ok_or_bad_request("No source key specified")?;
			handle_copy(garage, &req, &bucket, &key, source_bucket, source_key).await
		}
		Endpoint::PutObject { bucket, key } => {
			handle_put(garage, req, &bucket, &key, content_sha256).await
		}
		Endpoint::AbortMultipartUpload {
			bucket,
			key,
			upload_id,
		} => handle_abort_multipart_upload(garage, &bucket, &key, &upload_id).await,
		Endpoint::DeleteObject { bucket, key, .. } => handle_delete(garage, &bucket, &key).await,
		Endpoint::CreateMultipartUpload { bucket, key } => {
			handle_create_multipart_upload(garage, &req, &bucket, &key).await
		}
		Endpoint::CompleteMultipartUpload {
			bucket,
			key,
			upload_id,
		} => {
			handle_complete_multipart_upload(garage, req, &bucket, &key, &upload_id, content_sha256)
				.await
		}
		Endpoint::CreateBucket { bucket } => {
			debug!(
				"Body: {}",
				std::str::from_utf8(&hyper::body::to_bytes(req.into_body()).await?)
					.unwrap_or("<invalid utf8>")
			);
			let empty_body: Body = Body::from(vec![]);
			let response = Response::builder()
				.header("Location", format!("/{}", bucket))
				.body(empty_body)
				.unwrap();
			Ok(response)
		}
		Endpoint::HeadBucket { .. } => {
			let empty_body: Body = Body::from(vec![]);
			let response = Response::builder().body(empty_body).unwrap();
			Ok(response)
		}
		Endpoint::DeleteBucket { .. } => Err(Error::Forbidden(
			"Cannot delete buckets using S3 api, please talk to Garage directly".into(),
		)),
		Endpoint::GetBucketLocation { .. } => handle_get_bucket_location(garage),
		Endpoint::GetBucketVersioning { .. } => handle_get_bucket_versioning(),
		Endpoint::ListObjects {
			bucket,
			delimiter,
			encoding_type,
			marker,
			max_keys,
			prefix,
		} => {
			handle_list(
				garage,
				&ListObjectsQuery {
					is_v2: false,
					bucket,
					delimiter: delimiter.map(|d| d.to_string()),
					max_keys: max_keys.unwrap_or(1000),
					prefix: prefix.unwrap_or_default(),
					marker,
					continuation_token: None,
					start_after: None,
					urlencode_resp: encoding_type.map(|e| e == "url").unwrap_or(false),
				},
			)
			.await
		}
		Endpoint::ListObjectsV2 {
			bucket,
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
						is_v2: true,
						bucket,
						delimiter: delimiter.map(|d| d.to_string()),
						max_keys: max_keys.unwrap_or(1000),
						prefix: prefix.unwrap_or_default(),
						marker: None,
						continuation_token,
						start_after,
						urlencode_resp: encoding_type.map(|e| e == "url").unwrap_or(false),
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
		Endpoint::DeleteObjects { bucket } => {
			handle_delete_objects(garage, &bucket, req, content_sha256).await
		}
		endpoint => Err(Error::NotImplemented(endpoint.name().to_owned())),
	}
}

/// Extract the bucket name and the key name from an HTTP path and possibly a bucket provided in
/// the host header of the request
///
/// S3 internally manages only buckets and keys. This function splits
/// an HTTP path to get the corresponding bucket name and key.
fn parse_bucket_key<'a>(
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
