use std::sync::Arc;

use async_trait::async_trait;

use futures::future::Future;
use hyper::header;
use hyper::{Body, Method, Request, Response};

use opentelemetry::{trace::SpanRef, KeyValue};

use garage_table::util::*;
use garage_util::error::Error as GarageError;

use garage_model::garage::Garage;
use garage_model::key_table::Key;

use crate::error::*;
use crate::generic_server::*;

use crate::signature::payload::check_payload_signature;
use crate::signature::streaming::*;

use crate::helpers::*;
use crate::s3::bucket::*;
use crate::s3::copy::*;
use crate::s3::cors::*;
use crate::s3::delete::*;
use crate::s3::get::*;
use crate::s3::list::*;
use crate::s3::post_object::handle_post_object;
use crate::s3::put::*;
use crate::s3::router::Endpoint;
use crate::s3::website::*;

pub struct S3ApiServer {
	garage: Arc<Garage>,
}

pub(crate) struct S3ApiEndpoint {
	bucket_name: Option<String>,
	endpoint: Endpoint,
}

impl S3ApiServer {
	pub async fn run(
		garage: Arc<Garage>,
		shutdown_signal: impl Future<Output = ()>,
	) -> Result<(), GarageError> {
		let addr = garage.config.s3_api.api_bind_addr;

		ApiServer::new(
			garage.config.s3_api.s3_region.clone(),
			S3ApiServer { garage },
		)
		.run_server(addr, shutdown_signal)
		.await
	}

	async fn handle_request_without_bucket(
		&self,
		_req: Request<Body>,
		api_key: Key,
		endpoint: Endpoint,
	) -> Result<Response<Body>, Error> {
		match endpoint {
			Endpoint::ListBuckets => handle_list_buckets(&self.garage, &api_key).await,
			endpoint => Err(Error::NotImplemented(endpoint.name().to_owned())),
		}
	}
}

#[async_trait]
impl ApiHandler for S3ApiServer {
	const API_NAME: &'static str = "s3";
	const API_NAME_DISPLAY: &'static str = "S3";

	type Endpoint = S3ApiEndpoint;

	fn parse_endpoint(&self, req: &Request<Body>) -> Result<S3ApiEndpoint, Error> {
		let authority = req
			.headers()
			.get(header::HOST)
			.ok_or_bad_request("Host header required")?
			.to_str()?;

		let host = authority_to_host(authority)?;

		let bucket_name = self
			.garage
			.config
			.s3_api
			.root_domain
			.as_ref()
			.and_then(|root_domain| host_to_bucket(&host, root_domain));

		let (endpoint, bucket_name) =
			Endpoint::from_request(req, bucket_name.map(ToOwned::to_owned))?;

		Ok(S3ApiEndpoint {
			bucket_name,
			endpoint,
		})
	}

	async fn handle(
		&self,
		req: Request<Body>,
		endpoint: S3ApiEndpoint,
	) -> Result<Response<Body>, Error> {
		let S3ApiEndpoint {
			bucket_name,
			endpoint,
		} = endpoint;
		let garage = self.garage.clone();

		// Some endpoints are processed early, before we even check for an API key
		if let Endpoint::PostObject = endpoint {
			return handle_post_object(garage, req, bucket_name.unwrap()).await;
		}
		if let Endpoint::Options = endpoint {
			return handle_options_s3api(garage, &req, bucket_name).await;
		}

		let (api_key, mut content_sha256) = check_payload_signature(&garage, "s3", &req).await?;
		let api_key = api_key.ok_or_else(|| {
			Error::Forbidden("Garage does not support anonymous access yet".to_string())
		})?;

		let req = parse_streaming_body(
			&api_key,
			req,
			&mut content_sha256,
			&garage.config.s3_api.s3_region,
			"s3",
		)?;

		let bucket_name = match bucket_name {
			None => {
				return self
					.handle_request_without_bucket(req, api_key, endpoint)
					.await
			}
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
			Endpoint::CopyObject { key } => {
				handle_copy(garage, &api_key, &req, bucket_id, &key).await
			}
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
			Endpoint::PutBucketCors {} => {
				handle_put_cors(garage, bucket_id, req, content_sha256).await
			}
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
}

impl ApiEndpoint for S3ApiEndpoint {
	fn name(&self) -> &'static str {
		self.endpoint.name()
	}

	fn add_span_attributes(&self, span: SpanRef<'_>) {
		span.set_attribute(KeyValue::new(
			"bucket",
			self.bucket_name.clone().unwrap_or_default(),
		));
	}
}
