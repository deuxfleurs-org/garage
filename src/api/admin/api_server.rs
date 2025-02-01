use std::borrow::Cow;
use std::sync::Arc;

use argon2::password_hash::PasswordHash;
use async_trait::async_trait;

use http::header::{HeaderValue, ACCESS_CONTROL_ALLOW_ORIGIN, AUTHORIZATION};
use hyper::{body::Incoming as IncomingBody, Request, Response, StatusCode};
use tokio::sync::watch;

use opentelemetry::trace::SpanRef;

#[cfg(feature = "metrics")]
use opentelemetry_prometheus::PrometheusExporter;
#[cfg(feature = "metrics")]
use prometheus::{Encoder, TextEncoder};

use garage_model::garage::Garage;
use garage_util::error::Error as GarageError;
use garage_util::socket_address::UnixOrTCPSocketAddress;

use garage_api_common::generic_server::*;
use garage_api_common::helpers::*;

use crate::api::*;
use crate::error::*;
use crate::router_v0;
use crate::router_v1;
use crate::Authorization;
use crate::EndpointHandler;

pub type ResBody = BoxBody<Error>;

pub struct AdminApiServer {
	garage: Arc<Garage>,
	#[cfg(feature = "metrics")]
	exporter: PrometheusExporter,
	metrics_token: Option<String>,
	admin_token: Option<String>,
}

pub enum Endpoint {
	Old(router_v1::Endpoint),
	New(String),
}

impl AdminApiServer {
	pub fn new(
		garage: Arc<Garage>,
		#[cfg(feature = "metrics")] exporter: PrometheusExporter,
	) -> Self {
		let cfg = &garage.config.admin;
		let metrics_token = cfg.metrics_token.as_deref().map(hash_bearer_token);
		let admin_token = cfg.admin_token.as_deref().map(hash_bearer_token);
		Self {
			garage,
			#[cfg(feature = "metrics")]
			exporter,
			metrics_token,
			admin_token,
		}
	}

	pub async fn run(
		self,
		bind_addr: UnixOrTCPSocketAddress,
		must_exit: watch::Receiver<bool>,
	) -> Result<(), GarageError> {
		let region = self.garage.config.s3_api.s3_region.clone();
		ApiServer::new(region, self)
			.run_server(bind_addr, Some(0o220), must_exit)
			.await
	}

	fn handle_metrics(&self) -> Result<Response<ResBody>, Error> {
		#[cfg(feature = "metrics")]
		{
			use opentelemetry::trace::Tracer;

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

#[async_trait]
impl ApiHandler for AdminApiServer {
	const API_NAME: &'static str = "admin";
	const API_NAME_DISPLAY: &'static str = "Admin";

	type Endpoint = Endpoint;
	type Error = Error;

	fn parse_endpoint(&self, req: &Request<IncomingBody>) -> Result<Endpoint, Error> {
		if req.uri().path().starts_with("/v0/") {
			let endpoint_v0 = router_v0::Endpoint::from_request(req)?;
			let endpoint_v1 = router_v1::Endpoint::from_v0(endpoint_v0)?;
			Ok(Endpoint::Old(endpoint_v1))
		} else if req.uri().path().starts_with("/v1/") {
			let endpoint_v1 = router_v1::Endpoint::from_request(req)?;
			Ok(Endpoint::Old(endpoint_v1))
		} else {
			Ok(Endpoint::New(req.uri().path().to_string()))
		}
	}

	async fn handle(
		&self,
		req: Request<IncomingBody>,
		endpoint: Endpoint,
	) -> Result<Response<ResBody>, Error> {
		let auth_header = req.headers().get(AUTHORIZATION).cloned();

		let request = match endpoint {
			Endpoint::Old(endpoint_v1) => AdminApiRequest::from_v1(endpoint_v1, req).await?,
			Endpoint::New(_) => AdminApiRequest::from_request(req).await?,
		};

		let required_auth_hash =
			match request.authorization_type() {
				Authorization::None => None,
				Authorization::MetricsToken => self.metrics_token.as_deref(),
				Authorization::AdminToken => match self.admin_token.as_deref() {
					None => return Err(Error::forbidden(
						"Admin token isn't configured, admin API access is disabled for security.",
					)),
					Some(t) => Some(t),
				},
			};

		if let Some(password_hash) = required_auth_hash {
			match auth_header {
				None => return Err(Error::forbidden("Authorization token must be provided")),
				Some(authorization) => {
					verify_bearer_token(&authorization, password_hash)?;
				}
			}
		}

		match request {
			AdminApiRequest::Options(req) => req.handle(&self.garage).await,
			AdminApiRequest::CheckDomain(req) => req.handle(&self.garage).await,
			AdminApiRequest::Health(req) => req.handle(&self.garage).await,
			AdminApiRequest::Metrics(_req) => self.handle_metrics(),
			req => {
				let res = req.handle(&self.garage).await?;
				let mut res = json_ok_response(&res)?;
				res.headers_mut()
					.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
				Ok(res)
			}
		}
	}
}

impl ApiEndpoint for Endpoint {
	fn name(&self) -> Cow<'static, str> {
		match self {
			Self::Old(endpoint_v1) => Cow::Borrowed(endpoint_v1.name()),
			Self::New(path) => Cow::Owned(path.clone()),
		}
	}

	fn add_span_attributes(&self, _span: SpanRef<'_>) {}
}

fn hash_bearer_token(token: &str) -> String {
	use argon2::{
		password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
		Argon2,
	};

	let salt = SaltString::generate(&mut OsRng);
	let argon2 = Argon2::default();
	argon2
		.hash_password(token.trim().as_bytes(), &salt)
		.expect("could not hash API token")
		.to_string()
}

fn verify_bearer_token(token: &hyper::http::HeaderValue, password_hash: &str) -> Result<(), Error> {
	use argon2::{password_hash::PasswordVerifier, Argon2};

	let parsed_hash = PasswordHash::new(&password_hash).unwrap();

	token
		.to_str()?
		.strip_prefix("Bearer ")
		.and_then(|token| {
			Argon2::default()
				.verify_password(token.trim().as_bytes(), &parsed_hash)
				.ok()
		})
		.ok_or_else(|| Error::forbidden("Invalid authorization token"))?;

	Ok(())
}
