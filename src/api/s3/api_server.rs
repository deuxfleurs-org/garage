use std::sync::Arc;

use hyper::header;
use hyper::{body::Incoming as IncomingBody, Request, Response};
use tokio::sync::watch;

use opentelemetry::{trace::SpanRef, KeyValue};

use garage_util::error::Error as GarageError;
use garage_util::socket_address::UnixOrTCPSocketAddress;

use garage_model::garage::Garage;
use garage_model::key_table::Key;

use garage_api_common::cors::*;
use garage_api_common::generic_server::*;
use garage_api_common::helpers::*;
use garage_api_common::signature::verify_request;

use crate::bucket::*;
use crate::copy::*;
use crate::cors::*;
use crate::delete::*;
use crate::error::*;
use crate::get::*;
use crate::lifecycle::*;
use crate::list::*;
use crate::multipart::*;
use crate::post_object::handle_post_object;
use crate::put::*;
use crate::router::Endpoint;
use crate::website::*;

pub use garage_api_common::signature::streaming::ReqBody;
pub type ResBody = BoxBody<Error>;

pub struct S3ApiServer {
	garage: Arc<Garage>,
}

pub struct S3ApiEndpoint {
	bucket_name: Option<String>,
	endpoint: Endpoint,
}

impl S3ApiServer {
	pub async fn run(
		garage: Arc<Garage>,
		addr: UnixOrTCPSocketAddress,
		s3_region: String,
		must_exit: watch::Receiver<bool>,
	) -> Result<(), GarageError> {
		ApiServer::new(s3_region, S3ApiServer { garage })
			.run_server(addr, None, must_exit)
			.await
	}

	async fn handle_request_without_bucket(
		&self,
		_req: Request<ReqBody>,
		api_key: Key,
		endpoint: Endpoint,
	) -> Result<Response<ResBody>, Error> {
		match endpoint {
			Endpoint::ListBuckets => handle_list_buckets(&self.garage, &api_key).await,
			endpoint => Err(Error::NotImplemented(endpoint.name().to_owned())),
		}
	}
}

impl ApiHandler for S3ApiServer {
	const API_NAME: &'static str = "s3";
	const API_NAME_DISPLAY: &'static str = "S3";

	type Endpoint = S3ApiEndpoint;
	type Error = Error;

	fn parse_endpoint(&self, req: &Request<IncomingBody>) -> Result<S3ApiEndpoint, Error> {
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
		req: Request<IncomingBody>,
		endpoint: S3ApiEndpoint,
	) -> Result<Response<ResBody>, Error> {
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
			let options_res = handle_options_api(garage, &req, bucket_name).await?;
			return Ok(options_res.map(|_empty_body: EmptyBody| empty_body()));
		}

		let verified_request = verify_request(&garage, req, "s3").await?;
		let req = verified_request.request;
		let api_key = verified_request.access_key;

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
			return handle_create_bucket(&garage, req, &api_key.key_id, bucket_name).await;
		}

		let bucket_id = garage
			.bucket_helper()
			.resolve_bucket(&bucket_name, &api_key)
			.await
			.map_err(pass_helper_error)?;
		let bucket = garage
			.bucket_helper()
			.get_existing_bucket(bucket_id)
			.await?;
		let bucket_params = bucket.state.into_option().unwrap();

		let allowed = match endpoint.authorization_type() {
			Authorization::Read => api_key.allow_read(&bucket_id),
			Authorization::Write => api_key.allow_write(&bucket_id),
			Authorization::Owner => api_key.allow_owner(&bucket_id),
			_ => unreachable!(),
		};

		if !allowed {
			return Err(Error::forbidden("Operation is not allowed for this key."));
		}

		let matching_cors_rule = find_matching_cors_rule(&bucket_params, &req)?.cloned();

		let ctx = ReqCtx {
			garage,
			bucket_id,
			bucket_name,
			bucket_params,
			api_key,
		};

		let resp = match endpoint {
			Endpoint::HeadObject {
				key, part_number, ..
			} => handle_head(ctx, &req.map(|_| ()), &key, part_number).await,
			Endpoint::GetObject {
				key,
				part_number,
				response_cache_control,
				response_content_disposition,
				response_content_encoding,
				response_content_language,
				response_content_type,
				response_expires,
				..
			} => {
				let overrides = GetObjectOverrides {
					response_cache_control,
					response_content_disposition,
					response_content_encoding,
					response_content_language,
					response_content_type,
					response_expires,
				};
				handle_get(ctx, &req.map(|_| ()), &key, part_number, overrides).await
			}
			Endpoint::UploadPart {
				key,
				part_number,
				upload_id,
			} => handle_put_part(ctx, req, &key, part_number, &upload_id).await,
			Endpoint::CopyObject { key } => handle_copy(ctx, &req, &key).await,
			Endpoint::UploadPartCopy {
				key,
				part_number,
				upload_id,
			} => handle_upload_part_copy(ctx, &req, &key, part_number, &upload_id).await,
			Endpoint::PutObject { key } => handle_put(ctx, req, &key).await,
			Endpoint::AbortMultipartUpload { key, upload_id } => {
				handle_abort_multipart_upload(ctx, &key, &upload_id).await
			}
			Endpoint::DeleteObject { key, .. } => handle_delete(ctx, &key).await,
			Endpoint::CreateMultipartUpload { key } => {
				handle_create_multipart_upload(ctx, &req, &key).await
			}
			Endpoint::CompleteMultipartUpload { key, upload_id } => {
				handle_complete_multipart_upload(ctx, req, &key, &upload_id).await
			}
			Endpoint::CreateBucket {} => unreachable!(),
			Endpoint::HeadBucket {} => {
				let response = Response::builder().body(empty_body()).unwrap();
				Ok(response)
			}
			Endpoint::DeleteBucket {} => handle_delete_bucket(ctx).await,
			Endpoint::GetBucketLocation {} => handle_get_bucket_location(ctx),
			Endpoint::GetBucketVersioning {} => handle_get_bucket_versioning(),
			Endpoint::GetBucketAcl {} => handle_get_bucket_acl(ctx),
			Endpoint::ListObjects {
				delimiter,
				encoding_type,
				marker,
				max_keys,
				prefix,
			} => {
				let query = ListObjectsQuery {
					common: ListQueryCommon {
						bucket_name: ctx.bucket_name.clone(),
						bucket_id,
						delimiter,
						page_size: max_keys.unwrap_or(1000).clamp(1, 1000),
						prefix: prefix.unwrap_or_default(),
						urlencode_resp: encoding_type.map(|e| e == "url").unwrap_or(false),
					},
					is_v2: false,
					marker,
					continuation_token: None,
					start_after: None,
				};
				handle_list(ctx, &query).await
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
					let query = ListObjectsQuery {
						common: ListQueryCommon {
							bucket_name: ctx.bucket_name.clone(),
							bucket_id,
							delimiter,
							page_size: max_keys.unwrap_or(1000).clamp(1, 1000),
							urlencode_resp: encoding_type.map(|e| e == "url").unwrap_or(false),
							prefix: prefix.unwrap_or_default(),
						},
						is_v2: true,
						marker: None,
						continuation_token,
						start_after,
					};
					handle_list(ctx, &query).await
				} else {
					Err(Error::bad_request(format!(
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
				let query = ListMultipartUploadsQuery {
					common: ListQueryCommon {
						bucket_name: ctx.bucket_name.clone(),
						bucket_id,
						delimiter,
						page_size: max_uploads.unwrap_or(1000).clamp(1, 1000),
						prefix: prefix.unwrap_or_default(),
						urlencode_resp: encoding_type.map(|e| e == "url").unwrap_or(false),
					},
					key_marker,
					upload_id_marker,
				};
				handle_list_multipart_upload(ctx, &query).await
			}
			Endpoint::ListParts {
				key,
				max_parts,
				part_number_marker,
				upload_id,
			} => {
				let query = ListPartsQuery {
					bucket_name: ctx.bucket_name.clone(),
					key,
					upload_id,
					part_number_marker: part_number_marker.map(|p| p.min(10000)),
					max_parts: max_parts.unwrap_or(1000).clamp(1, 1000),
				};
				handle_list_parts(ctx, req, &query).await
			}
			Endpoint::DeleteObjects {} => handle_delete_objects(ctx, req).await,
			Endpoint::GetBucketWebsite {} => handle_get_website(ctx).await,
			Endpoint::PutBucketWebsite {} => handle_put_website(ctx, req).await,
			Endpoint::DeleteBucketWebsite {} => handle_delete_website(ctx).await,
			Endpoint::GetBucketCors {} => handle_get_cors(ctx).await,
			Endpoint::PutBucketCors {} => handle_put_cors(ctx, req).await,
			Endpoint::DeleteBucketCors {} => handle_delete_cors(ctx).await,
			Endpoint::GetBucketLifecycleConfiguration {} => handle_get_lifecycle(ctx).await,
			Endpoint::PutBucketLifecycleConfiguration {} => handle_put_lifecycle(ctx, req).await,
			Endpoint::DeleteBucketLifecycle {} => handle_delete_lifecycle(ctx).await,
			endpoint => Err(Error::NotImplemented(endpoint.name().to_owned())),
		};

		// If request was a success and we have a CORS rule that applies to it,
		// add the corresponding CORS headers to the response
		let mut resp_ok = resp?;
		if let Some(rule) = matching_cors_rule {
			add_cors_headers(&mut resp_ok, &rule)
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
