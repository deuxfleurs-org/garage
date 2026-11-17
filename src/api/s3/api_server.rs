use std::borrow::Cow;
use std::convert::TryInto;
use std::sync::Arc;

use hyper::header;
use hyper::{body::Incoming as IncomingBody, Request, Response};
use tokio::sync::watch;

use opentelemetry::{trace::SpanRef, KeyValue};

use garage_util::error::Error as GarageError;
use garage_util::socket_address::UnixOrTCPSocketAddress;

use garage_model::bucket_table::Bucket;
use garage_model::garage::Garage;
use garage_model::key_table::Key;

use garage_api_common::common_error::CommonError;
use garage_api_common::cors::*;
use garage_api_common::generic_server::*;
use garage_api_common::helpers::*;
use garage_api_common::signature::{verify_request, VerifiedRequest};

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
use crate::tagging::*;
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
			endpoint => Err(CommonError::NotImplemented(endpoint.name().to_owned()).into()),
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
			let options_res = handle_options_api(garage, &req, bucket_name)?;
			return Ok(options_res.map(|_empty_body: EmptyBody| empty_body()));
		}

		let verified_request = verify_request(&garage, req, "s3")?;

		// Depending on whether we have an authenticated request, we are going
		// to take two different paths to authorization:
		//
		//  1. If the request contained valid credentials, those need to go
		//     through authorization (check that the API key have the necessary
		//     access level for the requested method)
		//
		//  2. On the other hand, if we have an unauthenticated request, we
		//     verify that the bucket has all the methods required by the endpoint
		//     allowlisted.
		let (ctx, req): (AnyReqCtx, Request<ReqBody>) = match verified_request {
			VerifiedRequest {
				request: req,
				access_key: Some(api_key),
				..
			} => {
				let bucket_name = match bucket_name {
					None => {
						return self
							.handle_request_without_bucket(req, api_key, endpoint)
							.await
					}
					Some(bucket) => bucket,
				};

				// Special code path for CreateBucket API endpoint
				if let Endpoint::CreateBucket {} = endpoint {
					return handle_create_bucket(&garage, req, &api_key.key_id, bucket_name).await;
				}

				let bucket = garage
					.bucket_helper()
					.resolve_bucket_fast(&bucket_name, &api_key)
					.map_err(pass_helper_error)?;
				let bucket_id = bucket.id;

				// Return HTTP 501 Not Implemented before permission check
				if !endpoint.is_implemented() {
					return Err(CommonError::NotImplemented(endpoint.name().to_owned()).into());
				}

				let allowed = match endpoint.authorization_type() {
					Authorization::Read => api_key.allow_read(&bucket_id),
					Authorization::Write => api_key.allow_write(&bucket_id),
					Authorization::Owner => api_key.allow_owner(&bucket_id),
					_ => unreachable!(),
				};

				if !allowed {
					return Err(Error::forbidden("Operation is not allowed for this key."));
				}

				(
					AnyReqCtx::new(garage, bucket, bucket_name).with_key(api_key),
					req,
				)
			}

			anonymous_request => {
				let bucket_name = bucket_name.ok_or(CommonError::AnonymousAccessDenied)?;

				// return generic error to not leak existence of a bucket on anonymous access
				let Ok(Some(bucket)) = garage
					.bucket_helper()
					.resolve_global_bucket_fast(&bucket_name)
				else {
					return Err(CommonError::AnonymousAccessDenied.into());
				};

				if !allow_anonymous_operation(&endpoint, &bucket) {
					return Err(CommonError::AnonymousAccessDenied.into());
				}

				(
					AnyReqCtx::new(garage, bucket, bucket_name),
					anonymous_request.request,
				)
			}
		};

		let matching_cors = find_matching_cors_rule(&ctx.anon.bucket_params, &req)?
			.map(|(rule, origin)| (rule.clone(), origin.to_string()));

		let resp = match endpoint {
			Endpoint::HeadObject {
				key, part_number, ..
			} => handle_head(ctx.anon, &req.map(|_| ()), &key, part_number).await,
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
				handle_get(ctx.anon, &req.map(|_| ()), &key, part_number, overrides).await
			}
			Endpoint::GetObjectTagging { key, .. } => {
				handle_get_object_tagging(ctx.anon, &key).await
			}
			Endpoint::ListObjects {
				delimiter,
				encoding_type,
				marker,
				max_keys,
				prefix,
			} => {
				let query = ListObjectsQuery {
					common: ListQueryCommon {
						bucket_name: ctx.anon.bucket_name.clone(),
						bucket_id: ctx.anon.bucket_id,
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
				handle_list(ctx.anon, &query).await
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
							bucket_name: ctx.anon.bucket_name.clone(),
							bucket_id: ctx.anon.bucket_id,
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
					handle_list(ctx.anon, &query).await
				} else {
					Err(Error::bad_request(format!(
						"Invalid endpoint: list-type={}",
						list_type
					)))
				}
			}
			Endpoint::ListParts {
				key,
				max_parts,
				part_number_marker,
				upload_id,
			} => {
				let query = ListPartsQuery {
					bucket_name: ctx.anon.bucket_name.clone(),
					key,
					upload_id,
					part_number_marker: part_number_marker.map(|p| p.min(10000)),
					max_parts: max_parts.unwrap_or(1000).clamp(1, 1000),
				};
				handle_list_parts(ctx.anon, req, &query).await
			}
			authenticated_endpoint => {
				let ctx: ReqCtx = ctx.try_into()?;
				match authenticated_endpoint {
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
					Endpoint::PutObjectTagging { key, .. } => {
						handle_put_object_tagging(ctx, &key, req).await
					}
					Endpoint::DeleteObjectTagging { key, .. } => {
						handle_delete_object_tagging(ctx, &key).await
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
								bucket_id: ctx.bucket_id,
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
					Endpoint::DeleteObjects {} => handle_delete_objects(ctx, req).await,
					Endpoint::GetBucketWebsite {} => handle_get_website(ctx).await,
					Endpoint::PutBucketWebsite {} => handle_put_website(ctx, req).await,
					Endpoint::DeleteBucketWebsite {} => handle_delete_website(ctx).await,
					Endpoint::GetBucketCors {} => handle_get_cors(ctx).await,
					Endpoint::PutBucketCors {} => handle_put_cors(ctx, req).await,
					Endpoint::DeleteBucketCors {} => handle_delete_cors(ctx).await,
					Endpoint::GetBucketLifecycleConfiguration {} => handle_get_lifecycle(ctx).await,
					Endpoint::PutBucketLifecycleConfiguration {} => {
						handle_put_lifecycle(ctx, req).await
					}
					Endpoint::DeleteBucketLifecycle {} => handle_delete_lifecycle(ctx).await,
					Endpoint::GetBucketTagging {} => handle_get_bucket_tagging(ctx).await,
					Endpoint::PutBucketTagging {} => handle_put_bucket_tagging(ctx, req).await,
					Endpoint::DeleteBucketTagging {} => handle_delete_bucket_tagging(ctx).await,
					_ => unreachable!(), // unimplemented check is earlier
				}
			}
		};

		// If request was a success and we have a CORS rule that applies to it,
		// add the corresponding CORS headers to the response
		let mut resp_ok = resp?;
		if let Some((rule, origin)) = matching_cors {
			add_cors_headers(&mut resp_ok, &rule, &origin)
				.ok_or_internal_error("Invalid bucket CORS configuration")?;
		}

		Ok(resp_ok)
	}

	fn key_id_from_request(&self, req: &Request<IncomingBody>) -> Option<String> {
		garage_api_common::signature::payload::Authorization::parse_header(req.headers())
			.map(|auth| auth.key_id)
			.ok()
	}
}

impl ApiEndpoint for S3ApiEndpoint {
	fn name(&self) -> Cow<'static, str> {
		Cow::Borrowed(self.endpoint.name())
	}

	fn add_span_attributes(&self, span: SpanRef<'_>) {
		span.set_attribute(KeyValue::new(
			"bucket",
			self.bucket_name.clone().unwrap_or_default(),
		));
	}
}

fn allow_anonymous_operation(endpoint: &Endpoint, bucket: &Bucket) -> bool {
	if endpoint.authorization_type() != Authorization::Read {
		return false;
	}

	bucket
		.allowed_anonymous_operations()
		.iter()
		.any(|x| x == endpoint.name() || x == "*")
}
