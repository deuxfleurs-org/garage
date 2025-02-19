use std::sync::Arc;

use hyper::{body::Incoming as IncomingBody, Method, Request, Response};
use tokio::sync::watch;

use opentelemetry::{trace::SpanRef, KeyValue};

use garage_util::error::Error as GarageError;
use garage_util::socket_address::UnixOrTCPSocketAddress;

use garage_model::garage::Garage;

use garage_api_common::cors::*;
use garage_api_common::generic_server::*;
use garage_api_common::helpers::*;
use garage_api_common::signature::verify_request;

use crate::batch::*;
use crate::error::*;
use crate::index::*;
use crate::item::*;
use crate::router::Endpoint;

pub use garage_api_common::signature::streaming::ReqBody;
pub type ResBody = BoxBody<Error>;

pub struct K2VApiServer {
	garage: Arc<Garage>,
}

pub struct K2VApiEndpoint {
	bucket_name: String,
	endpoint: Endpoint,
}

impl K2VApiServer {
	pub async fn run(
		garage: Arc<Garage>,
		bind_addr: UnixOrTCPSocketAddress,
		s3_region: String,
		must_exit: watch::Receiver<bool>,
	) -> Result<(), GarageError> {
		ApiServer::new(s3_region, K2VApiServer { garage })
			.run_server(bind_addr, None, must_exit)
			.await
	}
}

impl ApiHandler for K2VApiServer {
	const API_NAME: &'static str = "k2v";
	const API_NAME_DISPLAY: &'static str = "K2V";

	type Endpoint = K2VApiEndpoint;
	type Error = Error;

	fn parse_endpoint(&self, req: &Request<IncomingBody>) -> Result<K2VApiEndpoint, Error> {
		let (endpoint, bucket_name) = Endpoint::from_request(req)?;

		Ok(K2VApiEndpoint {
			bucket_name,
			endpoint,
		})
	}

	async fn handle(
		&self,
		req: Request<IncomingBody>,
		endpoint: K2VApiEndpoint,
	) -> Result<Response<ResBody>, Error> {
		let K2VApiEndpoint {
			bucket_name,
			endpoint,
		} = endpoint;
		let garage = self.garage.clone();

		// The OPTIONS method is processed early, before we even check for an API key
		if let Endpoint::Options = endpoint {
			let options_res = handle_options_api(garage, &req, Some(bucket_name))
				.await
				.ok_or_bad_request("Error handling OPTIONS")?;
			return Ok(options_res.map(|_empty_body: EmptyBody| empty_body()));
		}

		let verified_request = verify_request(&garage, req, "k2v").await?;
		let req = verified_request.request;
		let api_key = verified_request.access_key;

		let bucket_id = garage
			.bucket_helper()
			.resolve_bucket(&bucket_name, &api_key)
			.await
			.map_err(pass_helper_error)?;
		let bucket = garage
			.bucket_helper()
			.get_existing_bucket(bucket_id)
			.await
			.map_err(helper_error_as_internal)?;
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

		// Look up what CORS rule might apply to response.
		// Requests for methods different than GET, HEAD or POST
		// are always preflighted, i.e. the browser should make
		// an OPTIONS call before to check it is allowed
		let matching_cors_rule = match *req.method() {
			Method::GET | Method::HEAD | Method::POST => {
				find_matching_cors_rule(&bucket_params, &req)
					.ok_or_internal_error("Error looking up CORS rule")?
					.cloned()
			}
			_ => None,
		};

		let ctx = ReqCtx {
			garage,
			bucket_id,
			bucket_name,
			bucket_params,
			api_key,
		};

		let resp = match endpoint {
			Endpoint::DeleteItem {
				partition_key,
				sort_key,
			} => handle_delete_item(ctx, req, &partition_key, &sort_key).await,
			Endpoint::InsertItem {
				partition_key,
				sort_key,
			} => handle_insert_item(ctx, req, &partition_key, &sort_key).await,
			Endpoint::ReadItem {
				partition_key,
				sort_key,
			} => handle_read_item(ctx, &req, &partition_key, &sort_key).await,
			Endpoint::PollItem {
				partition_key,
				sort_key,
				causality_token,
				timeout,
			} => {
				handle_poll_item(ctx, &req, partition_key, sort_key, causality_token, timeout).await
			}
			Endpoint::ReadIndex {
				prefix,
				start,
				end,
				limit,
				reverse,
			} => handle_read_index(ctx, prefix, start, end, limit, reverse).await,
			Endpoint::InsertBatch {} => handle_insert_batch(ctx, req).await,
			Endpoint::ReadBatch {} => handle_read_batch(ctx, req).await,
			Endpoint::DeleteBatch {} => handle_delete_batch(ctx, req).await,
			Endpoint::PollRange { partition_key } => {
				handle_poll_range(ctx, &partition_key, req).await
			}
			Endpoint::Options => unreachable!(),
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

impl ApiEndpoint for K2VApiEndpoint {
	fn name(&self) -> &'static str {
		self.endpoint.name()
	}

	fn add_span_attributes(&self, span: SpanRef<'_>) {
		span.set_attribute(KeyValue::new("bucket", self.bucket_name.clone()));
	}
}
