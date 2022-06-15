use std::sync::Arc;

use async_trait::async_trait;

use futures::future::Future;
use http::header::{
	ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ALLOW, CONTENT_TYPE,
};
use hyper::{Body, Request, Response};

use opentelemetry::trace::{SpanRef, Tracer};
use opentelemetry_prometheus::PrometheusExporter;
use prometheus::{Encoder, TextEncoder};

use garage_model::garage::Garage;
use garage_util::error::Error as GarageError;

use crate::generic_server::*;

use crate::admin::bucket::*;
use crate::admin::cluster::*;
use crate::admin::error::*;
use crate::admin::key::*;
use crate::admin::router::{Authorization, Endpoint};

pub struct AdminApiServer {
	garage: Arc<Garage>,
	exporter: PrometheusExporter,
	metrics_token: Option<String>,
	admin_token: Option<String>,
}

impl AdminApiServer {
	pub fn new(garage: Arc<Garage>) -> Self {
		let exporter = opentelemetry_prometheus::exporter().init();
		let cfg = &garage.config.admin;
		let metrics_token = cfg
			.metrics_token
			.as_ref()
			.map(|tok| format!("Bearer {}", tok));
		let admin_token = cfg
			.admin_token
			.as_ref()
			.map(|tok| format!("Bearer {}", tok));
		Self {
			garage,
			exporter,
			metrics_token,
			admin_token,
		}
	}

	pub async fn run(self, shutdown_signal: impl Future<Output = ()>) -> Result<(), GarageError> {
		if let Some(bind_addr) = self.garage.config.admin.api_bind_addr {
			let region = self.garage.config.s3_api.s3_region.clone();
			ApiServer::new(region, self)
				.run_server(bind_addr, shutdown_signal)
				.await
		} else {
			Ok(())
		}
	}

	fn handle_options(&self, _req: &Request<Body>) -> Result<Response<Body>, Error> {
		Ok(Response::builder()
			.status(204)
			.header(ALLOW, "OPTIONS, GET, POST")
			.header(ACCESS_CONTROL_ALLOW_METHODS, "OPTIONS, GET, POST")
			.header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
			.body(Body::empty())?)
	}

	fn handle_metrics(&self) -> Result<Response<Body>, Error> {
		let mut buffer = vec![];
		let encoder = TextEncoder::new();

		let tracer = opentelemetry::global::tracer("garage");
		let metric_families = tracer.in_span("admin/gather_metrics", |_| {
			self.exporter.registry().gather()
		});

		encoder
			.encode(&metric_families, &mut buffer)
			.ok_or_internal_error("Could not serialize metrics")?;

		Ok(Response::builder()
			.status(200)
			.header(CONTENT_TYPE, encoder.format_type())
			.body(Body::from(buffer))?)
	}
}

#[async_trait]
impl ApiHandler for AdminApiServer {
	const API_NAME: &'static str = "admin";
	const API_NAME_DISPLAY: &'static str = "Admin";

	type Endpoint = Endpoint;
	type Error = Error;

	fn parse_endpoint(&self, req: &Request<Body>) -> Result<Endpoint, Error> {
		Endpoint::from_request(req)
	}

	async fn handle(
		&self,
		req: Request<Body>,
		endpoint: Endpoint,
	) -> Result<Response<Body>, Error> {
		let expected_auth_header =
			match endpoint.authorization_type() {
				Authorization::MetricsToken => self.metrics_token.as_ref(),
				Authorization::AdminToken => match &self.admin_token {
					None => return Err(Error::forbidden(
						"Admin token isn't configured, admin API access is disabled for security.",
					)),
					Some(t) => Some(t),
				},
			};

		if let Some(h) = expected_auth_header {
			match req.headers().get("Authorization") {
				None => return Err(Error::forbidden("Authorization token must be provided")),
				Some(v) => {
					let authorized = v.to_str().map(|hv| hv.trim() == h).unwrap_or(false);
					if !authorized {
						return Err(Error::forbidden("Invalid authorization token provided"));
					}
				}
			}
		}

		match endpoint {
			Endpoint::Options => self.handle_options(&req),
			Endpoint::Metrics => self.handle_metrics(),
			Endpoint::GetClusterStatus => handle_get_cluster_status(&self.garage).await,
			Endpoint::ConnectClusterNodes => handle_connect_cluster_nodes(&self.garage, req).await,
			// Layout
			Endpoint::GetClusterLayout => handle_get_cluster_layout(&self.garage).await,
			Endpoint::UpdateClusterLayout => handle_update_cluster_layout(&self.garage, req).await,
			Endpoint::ApplyClusterLayout => handle_apply_cluster_layout(&self.garage, req).await,
			Endpoint::RevertClusterLayout => handle_revert_cluster_layout(&self.garage, req).await,
			// Keys
			Endpoint::ListKeys => handle_list_keys(&self.garage).await,
			Endpoint::GetKeyInfo { id, search } => {
				handle_get_key_info(&self.garage, id, search).await
			}
			Endpoint::CreateKey => handle_create_key(&self.garage, req).await,
			Endpoint::ImportKey => handle_import_key(&self.garage, req).await,
			Endpoint::UpdateKey { id } => handle_update_key(&self.garage, id, req).await,
			Endpoint::DeleteKey { id } => handle_delete_key(&self.garage, id).await,
			// Buckets
			Endpoint::ListBuckets => handle_list_buckets(&self.garage).await,
			Endpoint::GetBucketInfo { id, global_alias } => {
				handle_get_bucket_info(&self.garage, id, global_alias).await
			}
			Endpoint::CreateBucket => handle_create_bucket(&self.garage, req).await,
			Endpoint::DeleteBucket { id } => handle_delete_bucket(&self.garage, id).await,
			Endpoint::UpdateBucket { id } => handle_update_bucket(&self.garage, id, req).await,
			// Bucket-key permissions
			Endpoint::BucketAllowKey => {
				handle_bucket_change_key_perm(&self.garage, req, true).await
			}
			Endpoint::BucketDenyKey => {
				handle_bucket_change_key_perm(&self.garage, req, false).await
			}
			// Bucket aliasing
			Endpoint::GlobalAliasBucket { id, alias } => {
				handle_global_alias_bucket(&self.garage, id, alias).await
			}
			Endpoint::GlobalUnaliasBucket { id, alias } => {
				handle_global_unalias_bucket(&self.garage, id, alias).await
			}
			Endpoint::LocalAliasBucket {
				id,
				access_key_id,
				alias,
			} => handle_local_alias_bucket(&self.garage, id, access_key_id, alias).await,
			Endpoint::LocalUnaliasBucket {
				id,
				access_key_id,
				alias,
			} => handle_local_unalias_bucket(&self.garage, id, access_key_id, alias).await,
		}
	}
}

impl ApiEndpoint for Endpoint {
	fn name(&self) -> &'static str {
		Endpoint::name(self)
	}

	fn add_span_attributes(&self, _span: SpanRef<'_>) {}
}
