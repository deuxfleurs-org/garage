use std::sync::Arc;

use async_trait::async_trait;

use futures::future::Future;
use http::header::{CONTENT_TYPE, ALLOW, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN};
use hyper::{Body, Request, Response};

use opentelemetry::trace::{SpanRef, Tracer};
use opentelemetry_prometheus::PrometheusExporter;
use prometheus::{Encoder, TextEncoder};

use garage_model::garage::Garage;
use garage_util::error::Error as GarageError;

use crate::error::*;
use crate::generic_server::*;

use crate::admin::router::{Authorization, Endpoint};
use crate::admin::cluster::*;

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

	fn parse_endpoint(&self, req: &Request<Body>) -> Result<Endpoint, Error> {
		Endpoint::from_request(req)
	}

	async fn handle(
		&self,
		req: Request<Body>,
		endpoint: Endpoint,
	) -> Result<Response<Body>, Error> {
		let expected_auth_header = match endpoint.authorization_type() {
			Authorization::MetricsToken => self.metrics_token.as_ref(),
			Authorization::AdminToken => self.admin_token.as_ref(),
		};

		if let Some(h) = expected_auth_header {
			match req.headers().get("Authorization") {
				None => Err(Error::Forbidden(
					"Authorization token must be provided".into(),
				)),
				Some(v) if v.to_str().map(|hv| hv == h).unwrap_or(false) => Ok(()),
				_ => Err(Error::Forbidden(
					"Invalid authorization token provided".into(),
				)),
			}?;
		}

		match endpoint {
			Endpoint::Options => self.handle_options(&req),
			Endpoint::Metrics => self.handle_metrics(),
			Endpoint::GetClusterStatus => handle_get_cluster_status(&self.garage).await,
			_ => Err(Error::NotImplemented(format!(
				"Admin endpoint {} not implemented yet",
				endpoint.name()
			))),
		}
	}
}

impl ApiEndpoint for Endpoint {
	fn name(&self) -> &'static str {
		Endpoint::name(self)
	}

	fn add_span_attributes(&self, _span: SpanRef<'_>) {}
}
