use std::sync::Arc;

use http::header::{
	ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ALLOW,
};
use hyper::{Response, StatusCode};

#[cfg(feature = "metrics")]
use prometheus::{Encoder, TextEncoder};

use garage_model::garage::Garage;
use garage_rpc::system::ClusterHealthStatus;

use garage_api_common::helpers::*;

use crate::api::{CheckDomainRequest, HealthRequest, MetricsRequest, OptionsRequest};
use crate::api_server::ResBody;
use crate::error::*;
use crate::{Admin, RequestHandler};

impl RequestHandler for OptionsRequest {
	type Response = Response<ResBody>;

	async fn handle(
		self,
		_garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<Response<ResBody>, Error> {
		Ok(Response::builder()
			.status(StatusCode::OK)
			.header(ALLOW, "OPTIONS,GET,POST")
			.header(ACCESS_CONTROL_ALLOW_METHODS, "OPTIONS,GET,POST")
			.header(ACCESS_CONTROL_ALLOW_HEADERS, "authorization,content-type")
			.header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
			.body(empty_body())?)
	}
}

impl RequestHandler for MetricsRequest {
	type Response = Response<ResBody>;

	async fn handle(
		self,
		_garage: &Arc<Garage>,
		admin: &Admin,
	) -> Result<Response<ResBody>, Error> {
		#[cfg(feature = "metrics")]
		{
			use opentelemetry::trace::Tracer;

			let mut buffer = vec![];
			let encoder = TextEncoder::new();

			let tracer = opentelemetry::global::tracer("garage");
			let metric_families = tracer.in_span("admin/gather_metrics", |_| {
				admin.exporter.registry().gather()
			});

			encoder
				.encode(&metric_families, &mut buffer)
				.ok_or_internal_error("Could not serialize metrics")?;

			Ok(Response::builder()
				.status(StatusCode::OK)
				.header(http::header::CONTENT_TYPE, encoder.format_type())
				.body(bytes_body(buffer.into()))?)
		}
		#[cfg(not(feature = "metrics"))]
		Err(Error::bad_request(
			"Garage was built without the metrics feature".to_string(),
		))
	}
}

impl RequestHandler for HealthRequest {
	type Response = Response<ResBody>;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<Response<ResBody>, Error> {
		let health = garage.system.health();

		let (status, status_str) = match health.status {
			ClusterHealthStatus::Healthy => (StatusCode::OK, "Garage is fully operational"),
			ClusterHealthStatus::Degraded => (
				StatusCode::OK,
				"Garage is operational but some storage nodes are unavailable",
			),
			ClusterHealthStatus::Unavailable => (
				StatusCode::SERVICE_UNAVAILABLE,
				"Quorum is not available for some/all partitions, reads and writes will fail",
			),
		};
		let status_str = format!(
			"{}\nConsult the full health check API endpoint at /v2/GetClusterHealth for more details\n",
			status_str
		);

		Ok(Response::builder()
			.status(status)
			.header(http::header::CONTENT_TYPE, "text/plain")
			.body(string_body(status_str))?)
	}
}

impl RequestHandler for CheckDomainRequest {
	type Response = Response<ResBody>;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<Response<ResBody>, Error> {
		if check_domain(garage, &self.domain).await? {
			Ok(Response::builder()
				.status(StatusCode::OK)
				.body(string_body(format!(
					"Domain '{}' is managed by Garage",
					self.domain
				)))?)
		} else {
			Err(Error::bad_request(format!(
				"Domain '{}' is not managed by Garage",
				self.domain
			)))
		}
	}
}

async fn check_domain(garage: &Arc<Garage>, domain: &str) -> Result<bool, Error> {
	// Resolve bucket from domain name, inferring if the website must be activated for the
	// domain to be valid.
	let (bucket_name, must_check_website) = if let Some(bname) = garage
		.config
		.s3_api
		.root_domain
		.as_ref()
		.and_then(|rd| host_to_bucket(domain, rd))
	{
		(bname.to_string(), false)
	} else if let Some(bname) = garage
		.config
		.s3_web
		.as_ref()
		.and_then(|sw| host_to_bucket(domain, sw.root_domain.as_str()))
	{
		(bname.to_string(), true)
	} else {
		(domain.to_string(), true)
	};

	let bucket = match garage
		.bucket_helper()
		.resolve_global_bucket_fast(&bucket_name)?
	{
		Some(b) => b,
		None => return Ok(false),
	};

	if !must_check_website {
		return Ok(true);
	}

	let bucket_state = bucket.state.as_option().unwrap();
	let bucket_website_config = bucket_state.website_config.get();

	match bucket_website_config {
		Some(_v) => Ok(true),
		None => Ok(false),
	}
}
