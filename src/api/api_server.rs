use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::Future;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server};

use garage_util::error::Error as GarageError;

use garage_model::garage::Garage;

use crate::error::*;
use crate::signature::check_signature;

use crate::s3_bucket::*;
use crate::s3_copy::*;
use crate::s3_delete::*;
use crate::s3_get::*;
use crate::s3_list::*;
use crate::s3_put::*;

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

	let server = Server::bind(&addr).serve(service);

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
			let http_error = Response::builder()
				.status(e.http_status_code())
				.header("Content-Type", "application/xml")
				.body(body)?;

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
	let path = req.uri().path().to_string();
	let path = percent_encoding::percent_decode_str(&path).decode_utf8()?;

	let (bucket, key) = parse_bucket_key(&path)?;

	let (api_key, content_sha256) = check_signature(&garage, &req).await?;
	let allowed = match req.method() {
		&Method::HEAD | &Method::GET => api_key.allow_read(&bucket),
		_ => api_key.allow_write(&bucket),
	};
	if !allowed {
		return Err(Error::Forbidden(format!(
			"Operation is not allowed for this key."
		)));
	}

	let mut params = HashMap::new();
	if let Some(query) = req.uri().query() {
		let query_pairs = url::form_urlencoded::parse(query.as_bytes());
		for (key, val) in query_pairs {
			params.insert(key.to_lowercase(), val.to_string());
		}
	}

	if let Some(key) = key {
		match req.method() {
			&Method::HEAD => {
				// HeadObject query
				Ok(handle_head(garage, &req, &bucket, &key).await?)
			}
			&Method::GET => {
				// GetObject query
				Ok(handle_get(garage, &req, &bucket, &key).await?)
			}
			&Method::PUT => {
				if params.contains_key(&"partnumber".to_string())
					&& params.contains_key(&"uploadid".to_string())
				{
					// UploadPart query
					let part_number = params.get("partnumber").unwrap();
					let upload_id = params.get("uploadid").unwrap();
					Ok(handle_put_part(
						garage,
						req,
						&bucket,
						&key,
						part_number,
						upload_id,
						content_sha256,
					)
					.await?)
				} else if req.headers().contains_key("x-amz-copy-source") {
					// CopyObject query
					let copy_source = req.headers().get("x-amz-copy-source").unwrap().to_str()?;
					let copy_source =
						percent_encoding::percent_decode_str(&copy_source).decode_utf8()?;
					let (source_bucket, source_key) = parse_bucket_key(&copy_source)?;
					if !api_key.allow_read(&source_bucket) {
						return Err(Error::Forbidden(format!(
							"Reading from bucket {} not allowed for this key",
							source_bucket
						)));
					}
					let source_key = source_key.ok_or_bad_request("No source key specified")?;
					Ok(
						handle_copy(garage, &req, &bucket, &key, &source_bucket, &source_key)
							.await?,
					)
				} else {
					// PutObject query
					Ok(handle_put(garage, req, &bucket, &key, content_sha256).await?)
				}
			}
			&Method::DELETE => {
				if params.contains_key(&"uploadid".to_string()) {
					// AbortMultipartUpload query
					let upload_id = params.get("uploadid").unwrap();
					Ok(handle_abort_multipart_upload(garage, &bucket, &key, upload_id).await?)
				} else {
					// DeleteObject query
					Ok(handle_delete(garage, &bucket, &key).await?)
				}
			}
			&Method::POST => {
				if params.contains_key(&"uploads".to_string()) {
					// CreateMultipartUpload call
					Ok(handle_create_multipart_upload(garage, &req, &bucket, &key).await?)
				} else if params.contains_key(&"uploadid".to_string()) {
					// CompleteMultipartUpload call
					let upload_id = params.get("uploadid").unwrap();
					Ok(handle_complete_multipart_upload(
						garage,
						req,
						&bucket,
						&key,
						upload_id,
						content_sha256,
					)
					.await?)
				} else {
					Err(Error::BadRequest(format!(
						"Not a CreateMultipartUpload call, what is it?"
					)))
				}
			}
			_ => Err(Error::BadRequest(format!("Invalid method"))),
		}
	} else {
		match req.method() {
			&Method::PUT => {
				// CreateBucket
				// If we're here, the bucket already exists, so just answer ok
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
			&Method::HEAD => {
				// HeadBucket
				let empty_body: Body = Body::from(vec![]);
				let response = Response::builder().body(empty_body).unwrap();
				Ok(response)
			}
			&Method::DELETE => {
				// DeleteBucket query
				Err(Error::Forbidden(
					"Cannot delete buckets using S3 api, please talk to Garage directly".into(),
				))
			}
			&Method::GET => {
				if params.contains_key("location") {
					// GetBucketLocation call
					Ok(handle_get_bucket_location(garage)?)
				} else {
					// ListObjects or ListObjectsV2 query
					let q = parse_list_objects_query(bucket, &params)?;
					Ok(handle_list(garage, &q).await?)
				}
			}
			&Method::POST => {
				if params.contains_key(&"delete".to_string()) {
					// DeleteObjects
					Ok(handle_delete_objects(garage, bucket, req, content_sha256).await?)
				} else {
					debug!(
						"Body: {}",
						std::str::from_utf8(&hyper::body::to_bytes(req.into_body()).await?)
							.unwrap_or("<invalid utf8>")
					);
					Err(Error::BadRequest(format!("Unsupported call")))
				}
			}
			_ => Err(Error::BadRequest(format!("Invalid method"))),
		}
	}
}

/// Extract the bucket name and the key name from an HTTP path
///
/// S3 internally manages only buckets and keys. This function splits
/// an HTTP path to get the corresponding bucket name and key.
fn parse_bucket_key(path: &str) -> Result<(&str, Option<&str>), Error> {
	let path = path.trim_start_matches('/');

	let (bucket, key) = match path.find('/') {
		Some(i) => {
			let key = &path[i + 1..];
			if key.len() > 0 {
				(&path[..i], Some(key))
			} else {
				(&path[..i], None)
			}
		}
		None => (path, None),
	};
	if bucket.len() == 0 {
		return Err(Error::BadRequest(format!("No bucket specified")));
	}
	Ok((bucket, key))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_bucket_containing_a_key() -> Result<(), Error> {
		let (bucket, key) = parse_bucket_key("/my_bucket/a/super/file.jpg")?;
		assert_eq!(bucket, "my_bucket");
		assert_eq!(key.expect("key must be set"), "a/super/file.jpg");
		Ok(())
	}

	#[test]
	fn parse_bucket_containing_no_key() -> Result<(), Error> {
		let (bucket, key) = parse_bucket_key("/my_bucket/")?;
		assert_eq!(bucket, "my_bucket");
		assert!(key.is_none());
		let (bucket, key) = parse_bucket_key("/my_bucket")?;
		assert_eq!(bucket, "my_bucket");
		assert!(key.is_none());
		Ok(())
	}

	#[test]
	fn parse_bucket_containing_no_bucket() {
		let parsed = parse_bucket_key("");
		assert!(parsed.is_err());
		let parsed = parse_bucket_key("/");
		assert!(parsed.is_err());
		let parsed = parse_bucket_key("////");
		assert!(parsed.is_err());
	}
}
