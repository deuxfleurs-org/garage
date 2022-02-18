use std::net::SocketAddr;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Utc};
use futures::future::Future;
use futures::prelude::*;
use hyper::header;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server};

use opentelemetry::{
	global,
	metrics::{Counter, ValueRecorder},
	trace::{FutureExt, TraceContextExt, Tracer},
	Context, KeyValue,
};

use garage_util::data::*;
use garage_util::error::Error as GarageError;
use garage_util::metrics::{gen_trace_id, RecordDuration};

use garage_model::garage::Garage;
use garage_model::key_table::Key;

use garage_table::util::*;

use crate::error::*;
use crate::signature::compute_scope;
use crate::signature::payload::check_payload_signature;
use crate::signature::streaming::SignedPayloadStream;
use crate::signature::LONG_DATETIME;

use crate::helpers::*;
use crate::s3_bucket::*;
use crate::s3_copy::*;
use crate::s3_cors::*;
use crate::s3_delete::*;
use crate::s3_get::*;
use crate::s3_list::*;
use crate::s3_post_object::handle_post_object;
use crate::s3_put::*;
use crate::s3_router::{Authorization, Endpoint};
use crate::s3_website::*;

struct ApiMetrics {
	request_counter: Counter<u64>,
	error_counter: Counter<u64>,
	request_duration: ValueRecorder<f64>,
}

impl ApiMetrics {
	fn new() -> Self {
		let meter = global::meter("garage/api");
		Self {
			request_counter: meter
				.u64_counter("api.request_counter")
				.with_description("Number of API calls to the various S3 API endpoints")
				.init(),
			error_counter: meter
				.u64_counter("api.error_counter")
				.with_description(
					"Number of API calls to the various S3 API endpoints that resulted in errors",
				)
				.init(),
			request_duration: meter
				.f64_value_recorder("api.request_duration")
				.with_description("Duration of API calls to the various S3 API endpoints")
				.init(),
		}
	}
}

/// Run the S3 API server
pub async fn run_api_server(
	garage: Arc<Garage>,
	shutdown_signal: impl Future<Output = ()>,
) -> Result<(), GarageError> {
	let addr = &garage.config.s3_api.api_bind_addr;

	let metrics = Arc::new(ApiMetrics::new());

	let service = make_service_fn(|conn: &AddrStream| {
		let garage = garage.clone();
		let metrics = metrics.clone();

		let client_addr = conn.remote_addr();
		async move {
			Ok::<_, GarageError>(service_fn(move |req: Request<Body>| {
				let garage = garage.clone();
				let metrics = metrics.clone();

				handler(garage, metrics, req, client_addr)
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
	metrics: Arc<ApiMetrics>,
	req: Request<Body>,
	addr: SocketAddr,
) -> Result<Response<Body>, GarageError> {
	let uri = req.uri().clone();
	info!("{} {} {}", addr, req.method(), uri);
	debug!("{:?}", req);

	let tracer = opentelemetry::global::tracer("garage");
	let span = tracer
		.span_builder("S3 API call (unknown)")
		.with_trace_id(gen_trace_id())
		.with_attributes(vec![
			KeyValue::new("method", format!("{}", req.method())),
			KeyValue::new("uri", req.uri().to_string()),
		])
		.start(&tracer);

	let res = handler_stage2(garage.clone(), metrics, req)
		.with_context(Context::current_with_span(span))
		.await;

	match res {
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

async fn handler_stage2(
	garage: Arc<Garage>,
	metrics: Arc<ApiMetrics>,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let authority = req
		.headers()
		.get(header::HOST)
		.ok_or_bad_request("Host header required")?
		.to_str()?;

	let host = authority_to_host(authority)?;

	let bucket_name = garage
		.config
		.s3_api
		.root_domain
		.as_ref()
		.and_then(|root_domain| host_to_bucket(&host, root_domain));

	let (endpoint, bucket_name) = Endpoint::from_request(&req, bucket_name.map(ToOwned::to_owned))?;
	debug!("Endpoint: {:?}", endpoint);

	let current_context = Context::current();
	let current_span = current_context.span();
	current_span.update_name::<String>(format!("S3 API {}", endpoint.name()));
	current_span.set_attribute(KeyValue::new("endpoint", endpoint.name()));
	current_span.set_attribute(KeyValue::new(
		"bucket",
		bucket_name.clone().unwrap_or_default(),
	));

	let metrics_tags = &[KeyValue::new("api_endpoint", endpoint.name())];

	let res = handler_stage3(garage, req, endpoint, bucket_name)
		.record_duration(&metrics.request_duration, &metrics_tags[..])
		.await;

	metrics.request_counter.add(1, &metrics_tags[..]);

	let status_code = match &res {
		Ok(r) => r.status(),
		Err(e) => e.http_status_code(),
	};
	if status_code.is_client_error() || status_code.is_server_error() {
		metrics.error_counter.add(
			1,
			&[
				metrics_tags[0].clone(),
				KeyValue::new("status_code", status_code.as_str().to_string()),
			],
		);
	}

	res
}

async fn handler_stage3(
	garage: Arc<Garage>,
	req: Request<Body>,
	endpoint: Endpoint,
	bucket_name: Option<String>,
) -> Result<Response<Body>, Error> {
	// Some endpoints are processed early, before we even check for an API key
	if let Endpoint::PostObject = endpoint {
		return handle_post_object(garage, req, bucket_name.unwrap()).await;
	}
	if let Endpoint::Options = endpoint {
		return handle_options_s3api(garage, &req, bucket_name).await;
	}

	let (api_key, mut content_sha256) = check_payload_signature(&garage, &req).await?;
	let api_key = api_key.ok_or_else(|| {
		Error::Forbidden("Garage does not support anonymous access yet".to_string())
	})?;

	let req = match req.headers().get("x-amz-content-sha256") {
		Some(header) if header == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" => {
			let signature = content_sha256
				.take()
				.ok_or_bad_request("No signature provided")?;

			let secret_key = &api_key
				.state
				.as_option()
				.ok_or_internal_error("Deleted key state")?
				.secret_key;

			let date = req
				.headers()
				.get("x-amz-date")
				.ok_or_bad_request("Missing X-Amz-Date field")?
				.to_str()?;
			let date: NaiveDateTime = NaiveDateTime::parse_from_str(date, LONG_DATETIME)
				.ok_or_bad_request("Invalid date")?;
			let date: DateTime<Utc> = DateTime::from_utc(date, Utc);

			let scope = compute_scope(&date, &garage.config.s3_api.s3_region);
			let signing_hmac = crate::signature::signing_hmac(
				&date,
				secret_key,
				&garage.config.s3_api.s3_region,
				"s3",
			)
			.ok_or_internal_error("Unable to build signing HMAC")?;

			req.map(move |body| {
				Body::wrap_stream(
					SignedPayloadStream::new(
						body.map_err(Error::from),
						signing_hmac,
						date,
						&scope,
						signature,
					)
					.map_err(Error::from),
				)
			})
		}
		_ => req,
	};

	let bucket_name = match bucket_name {
		None => return handle_request_without_bucket(garage, req, api_key, endpoint).await,
		Some(bucket) => bucket.to_string(),
	};

	// Special code path for CreateBucket API endpoint
	if let Endpoint::CreateBucket {} = endpoint {
		return handle_create_bucket(&garage, req, content_sha256, api_key, bucket_name).await;
	}

	let bucket_id = resolve_bucket(&garage, &bucket_name, &api_key).await?;
	let bucket = garage
		.bucket_table
		.get(&EmptyKey, &bucket_id)
		.await?
		.filter(|b| !b.state.is_deleted())
		.ok_or(Error::NoSuchBucket)?;

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

	// Look up what CORS rule might apply to response.
	// Requests for methods different than GET, HEAD or POST
	// are always preflighted, i.e. the browser should make
	// an OPTIONS call before to check it is allowed
	let matching_cors_rule = match *req.method() {
		Method::GET | Method::HEAD | Method::POST => find_matching_cors_rule(&bucket, &req)?,
		_ => None,
	};

	let resp = match endpoint {
		Endpoint::HeadObject {
			key, part_number, ..
		} => handle_head(garage, &req, bucket_id, &key, part_number).await,
		Endpoint::GetObject {
			key, part_number, ..
		} => handle_get(garage, &req, bucket_id, &key, part_number).await,
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
			handle_put(garage, req, bucket_id, &key, content_sha256).await
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
						page_size: max_keys.map(|p| p.clamp(1, 1000)).unwrap_or(1000),
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
							page_size: max_keys.map(|p| p.clamp(1, 1000)).unwrap_or(1000),
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
						page_size: max_uploads.map(|p| p.clamp(1, 1000)).unwrap_or(1000),
						prefix: prefix.unwrap_or_default(),
						urlencode_resp: encoding_type.map(|e| e == "url").unwrap_or(false),
					},
					key_marker,
					upload_id_marker,
				},
			)
			.await
		}
		Endpoint::ListParts {
			key,
			max_parts,
			part_number_marker,
			upload_id,
		} => {
			handle_list_parts(
				garage,
				&ListPartsQuery {
					bucket_name,
					bucket_id,
					key,
					upload_id,
					part_number_marker: part_number_marker.map(|p| p.clamp(1, 10000)),
					max_parts: max_parts.map(|p| p.clamp(1, 1000)).unwrap_or(1000),
				},
			)
			.await
		}
		Endpoint::DeleteObjects {} => {
			handle_delete_objects(garage, bucket_id, req, content_sha256).await
		}
		Endpoint::GetBucketWebsite {} => handle_get_website(&bucket).await,
		Endpoint::PutBucketWebsite {} => {
			handle_put_website(garage, bucket_id, req, content_sha256).await
		}
		Endpoint::DeleteBucketWebsite {} => handle_delete_website(garage, bucket_id).await,
		Endpoint::GetBucketCors {} => handle_get_cors(&bucket).await,
		Endpoint::PutBucketCors {} => handle_put_cors(garage, bucket_id, req, content_sha256).await,
		Endpoint::DeleteBucketCors {} => handle_delete_cors(garage, bucket_id).await,
		endpoint => Err(Error::NotImplemented(endpoint.name().to_owned())),
	};

	// If request was a success and we have a CORS rule that applies to it,
	// add the corresponding CORS headers to the response
	let mut resp_ok = resp?;
	if let Some(rule) = matching_cors_rule {
		add_cors_headers(&mut resp_ok, rule)
			.ok_or_internal_error("Invalid bucket CORS configuration")?;
	}

	Ok(resp_ok)
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
