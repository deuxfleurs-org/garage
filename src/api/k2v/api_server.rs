use std::sync::Arc;

use async_trait::async_trait;

use futures::future::Future;
use hyper::{Body, Method, Request, Response};

use opentelemetry::{trace::SpanRef, KeyValue};

use garage_table::util::*;
use garage_util::error::Error as GarageError;

use garage_model::garage::Garage;

use crate::error::*;
use crate::generic_server::*;

use crate::signature::payload::check_payload_signature;
use crate::signature::streaming::*;

use crate::helpers::*;
use crate::k2v::batch::*;
use crate::k2v::index::*;
use crate::k2v::item::*;
use crate::k2v::router::Endpoint;
use crate::s3::cors::*;

pub struct K2VApiServer {
	garage: Arc<Garage>,
}

pub(crate) struct K2VApiEndpoint {
	bucket_name: String,
	endpoint: Endpoint,
}

impl K2VApiServer {
	pub async fn run(
		garage: Arc<Garage>,
		shutdown_signal: impl Future<Output = ()>,
	) -> Result<(), GarageError> {
		if let Some(cfg) = &garage.config.k2v_api {
			let bind_addr = cfg.api_bind_addr;

			ApiServer::new(
				garage.config.s3_api.s3_region.clone(),
				K2VApiServer { garage },
			)
			.run_server(bind_addr, shutdown_signal)
			.await
		} else {
			Ok(())
		}
	}
}

#[async_trait]
impl ApiHandler for K2VApiServer {
	const API_NAME: &'static str = "k2v";
	const API_NAME_DISPLAY: &'static str = "K2V";

	type Endpoint = K2VApiEndpoint;

	fn parse_endpoint(&self, req: &Request<Body>) -> Result<K2VApiEndpoint, Error> {
		let (endpoint, bucket_name) = Endpoint::from_request(req)?;

		Ok(K2VApiEndpoint {
			bucket_name,
			endpoint,
		})
	}

	async fn handle(
		&self,
		req: Request<Body>,
		endpoint: K2VApiEndpoint,
	) -> Result<Response<Body>, Error> {
		let K2VApiEndpoint {
			bucket_name,
			endpoint,
		} = endpoint;
		let garage = self.garage.clone();

		// The OPTIONS method is procesed early, before we even check for an API key
		if let Endpoint::Options = endpoint {
			return handle_options_s3api(garage, &req, Some(bucket_name)).await;
		}

		let (api_key, mut content_sha256) = check_payload_signature(&garage, "k2v", &req).await?;
		let api_key = api_key.ok_or_else(|| {
			Error::Forbidden("Garage does not support anonymous access yet".to_string())
		})?;

		let req = parse_streaming_body(
			&api_key,
			req,
			&mut content_sha256,
			&garage.config.s3_api.s3_region,
			"k2v",
		)?;

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
			Endpoint::DeleteItem {
				partition_key,
				sort_key,
			} => handle_delete_item(garage, req, bucket_id, &partition_key, &sort_key).await,
			Endpoint::InsertItem {
				partition_key,
				sort_key,
			} => handle_insert_item(garage, req, bucket_id, &partition_key, &sort_key).await,
			Endpoint::ReadItem {
				partition_key,
				sort_key,
			} => handle_read_item(garage, &req, bucket_id, &partition_key, &sort_key).await,
			Endpoint::PollItem {
				partition_key,
				sort_key,
				causality_token,
				timeout,
			} => {
				handle_poll_item(
					garage,
					&req,
					bucket_id,
					partition_key,
					sort_key,
					causality_token,
					timeout,
				)
				.await
			}
			Endpoint::ReadIndex {
				prefix,
				start,
				end,
				limit,
				reverse,
			} => handle_read_index(garage, bucket_id, prefix, start, end, limit, reverse).await,
			Endpoint::InsertBatch {} => handle_insert_batch(garage, bucket_id, req).await,
			Endpoint::ReadBatch {} => handle_read_batch(garage, bucket_id, req).await,
			Endpoint::DeleteBatch {} => handle_delete_batch(garage, bucket_id, req).await,
			Endpoint::Options => unreachable!(),
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

impl ApiEndpoint for K2VApiEndpoint {
	fn name(&self) -> &'static str {
		self.endpoint.name()
	}

	fn add_span_attributes(&self, span: SpanRef<'_>) {
		span.set_attribute(KeyValue::new("bucket", self.bucket_name.clone()));
	}
}
