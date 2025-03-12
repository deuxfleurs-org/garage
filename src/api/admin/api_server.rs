use std::borrow::Cow;
use std::sync::Arc;

use http::header::{HeaderValue, ACCESS_CONTROL_ALLOW_ORIGIN, AUTHORIZATION};
use hyper::{body::Incoming as IncomingBody, Request, Response};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use opentelemetry::trace::SpanRef;

#[cfg(feature = "metrics")]
use opentelemetry_prometheus::PrometheusExporter;

use garage_model::garage::Garage;
use garage_rpc::{Endpoint as RpcEndpoint, *};
use garage_table::EmptyKey;
use garage_util::background::BackgroundRunner;
use garage_util::data::Uuid;
use garage_util::error::Error as GarageError;
use garage_util::socket_address::UnixOrTCPSocketAddress;
use garage_util::time::now_msec;

use garage_api_common::generic_server::*;
use garage_api_common::helpers::*;

use crate::api::*;
use crate::error::*;
use crate::router_v0;
use crate::router_v1;
use crate::Authorization;
use crate::RequestHandler;

// ---- FOR RPC ----

pub const ADMIN_RPC_PATH: &str = "garage_api/admin/rpc.rs/Rpc";

#[derive(Debug, Serialize, Deserialize)]
pub enum AdminRpc {
	Proxy(AdminApiRequest),
	Internal(LocalAdminApiRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AdminRpcResponse {
	ProxyApiOkResponse(TaggedAdminApiResponse),
	InternalApiOkResponse(LocalAdminApiResponse),
	ApiErrorResponse {
		http_code: u16,
		error_code: String,
		message: String,
	},
}

impl Rpc for AdminRpc {
	type Response = Result<AdminRpcResponse, GarageError>;
}

impl EndpointHandler<AdminRpc> for AdminApiServer {
	async fn handle(
		self: &Arc<Self>,
		message: &AdminRpc,
		_from: NodeID,
	) -> Result<AdminRpcResponse, GarageError> {
		match message {
			AdminRpc::Proxy(req) => {
				info!("Proxied admin API request: {}", req.name());
				let res = req.clone().handle(&self.garage, &self).await;
				match res {
					Ok(res) => Ok(AdminRpcResponse::ProxyApiOkResponse(res.tagged())),
					Err(e) => Ok(AdminRpcResponse::ApiErrorResponse {
						http_code: e.http_status_code().as_u16(),
						error_code: e.code().to_string(),
						message: e.to_string(),
					}),
				}
			}
			AdminRpc::Internal(req) => {
				info!("Internal admin API request: {}", req.name());
				let res = req.clone().handle(&self.garage, &self).await;
				match res {
					Ok(res) => Ok(AdminRpcResponse::InternalApiOkResponse(res)),
					Err(e) => Ok(AdminRpcResponse::ApiErrorResponse {
						http_code: e.http_status_code().as_u16(),
						error_code: e.code().to_string(),
						message: e.to_string(),
					}),
				}
			}
		}
	}
}

// ---- FOR HTTP ----

pub type ResBody = BoxBody<Error>;

pub struct AdminApiServer {
	garage: Arc<Garage>,
	#[cfg(feature = "metrics")]
	pub(crate) exporter: PrometheusExporter,
	metrics_token: Option<String>,
	metrics_require_token: bool,
	admin_token: Option<String>,
	pub(crate) background: Arc<BackgroundRunner>,
	pub(crate) endpoint: Arc<RpcEndpoint<AdminRpc, Self>>,
}

pub enum HttpEndpoint {
	Old(router_v1::Endpoint),
	New(String),
}

impl AdminApiServer {
	pub fn new(
		garage: Arc<Garage>,
		background: Arc<BackgroundRunner>,
		#[cfg(feature = "metrics")] exporter: PrometheusExporter,
	) -> Arc<Self> {
		let cfg = &garage.config.admin;
		let metrics_token = cfg.metrics_token.as_deref().map(hash_bearer_token);
		let admin_token = cfg.admin_token.as_deref().map(hash_bearer_token);
		let metrics_require_token = cfg.metrics_require_token;

		let endpoint = garage.system.netapp.endpoint(ADMIN_RPC_PATH.into());
		let admin = Arc::new(Self {
			garage,
			#[cfg(feature = "metrics")]
			exporter,
			metrics_token,
			metrics_require_token,
			admin_token,
			background,
			endpoint,
		});
		admin.endpoint.set_handler(admin.clone());
		admin
	}

	pub async fn run(
		self: Arc<Self>,
		bind_addr: UnixOrTCPSocketAddress,
		must_exit: watch::Receiver<bool>,
	) -> Result<(), GarageError> {
		let region = self.garage.config.s3_api.s3_region.clone();
		ApiServer::new(region, ArcAdminApiServer(self))
			.run_server(bind_addr, Some(0o220), must_exit)
			.await
	}

	async fn handle_http_api(
		&self,
		req: Request<IncomingBody>,
		endpoint: HttpEndpoint,
	) -> Result<Response<ResBody>, Error> {
		let auth_header = req.headers().get(AUTHORIZATION).cloned();

		let request = match endpoint {
			HttpEndpoint::Old(endpoint_v1) => AdminApiRequest::from_v1(endpoint_v1, req).await?,
			HttpEndpoint::New(_) => AdminApiRequest::from_request(req).await?,
		};

		let (global_token_hash, token_required) = match request.authorization_type() {
			Authorization::None => (None, false),
			Authorization::MetricsToken => (
				self.metrics_token.as_deref(),
				self.metrics_token.is_some() || self.metrics_require_token,
			),
			Authorization::AdminToken => (self.admin_token.as_deref(), true),
		};

		if token_required {
			verify_authorization(&self.garage, global_token_hash, auth_header, request.name())
				.await?;
		}

		match request {
			AdminApiRequest::Options(req) => req.handle(&self.garage, &self).await,
			AdminApiRequest::CheckDomain(req) => req.handle(&self.garage, &self).await,
			AdminApiRequest::Health(req) => req.handle(&self.garage, &self).await,
			AdminApiRequest::Metrics(req) => req.handle(&self.garage, &self).await,
			req => {
				let res = req.handle(&self.garage, &self).await?;
				let mut res = json_ok_response(&res)?;
				res.headers_mut()
					.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
				Ok(res)
			}
		}
	}
}

struct ArcAdminApiServer(Arc<AdminApiServer>);

impl ApiHandler for ArcAdminApiServer {
	const API_NAME: &'static str = "admin";
	const API_NAME_DISPLAY: &'static str = "Admin";

	type Endpoint = HttpEndpoint;
	type Error = Error;

	fn parse_endpoint(&self, req: &Request<IncomingBody>) -> Result<HttpEndpoint, Error> {
		if req.uri().path().starts_with("/v0/") {
			let endpoint_v0 = router_v0::Endpoint::from_request(req)?;
			let endpoint_v1 = router_v1::Endpoint::from_v0(endpoint_v0)?;
			Ok(HttpEndpoint::Old(endpoint_v1))
		} else if req.uri().path().starts_with("/v1/") {
			let endpoint_v1 = router_v1::Endpoint::from_request(req)?;
			Ok(HttpEndpoint::Old(endpoint_v1))
		} else {
			Ok(HttpEndpoint::New(req.uri().path().to_string()))
		}
	}

	async fn handle(
		&self,
		req: Request<IncomingBody>,
		endpoint: HttpEndpoint,
	) -> Result<Response<ResBody>, Error> {
		self.0.handle_http_api(req, endpoint).await
	}
}

impl ApiEndpoint for HttpEndpoint {
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

async fn verify_authorization(
	garage: &Garage,
	global_token_hash: Option<&str>,
	auth_header: Option<hyper::http::HeaderValue>,
	endpoint_name: &str,
) -> Result<(), Error> {
	use argon2::{password_hash::PasswordHash, password_hash::PasswordVerifier, Argon2};

	let invalid_msg = "Invalid bearer token";

	let token = match &auth_header {
		None => {
			return Err(Error::forbidden(
				"Bearer token must be provided in Authorization header",
			))
		}
		Some(authorization) => authorization
			.to_str()?
			.strip_prefix("Bearer ")
			.ok_or_else(|| Error::forbidden("Invalid Authorization header"))?
			.trim(),
	};

	let token_hash_string = if let Some((prefix, _)) = token.split_once('.') {
		garage
			.admin_token_table
			.get(&EmptyKey, &prefix.to_string())
			.await?
			.and_then(|k| k.state.into_option())
			.filter(|p| {
				p.expiration
					.get()
					.map(|exp| now_msec() < exp)
					.unwrap_or(true)
			})
			.filter(|p| {
				p.scope
					.get()
					.0
					.iter()
					.any(|x| x == "*" || x == endpoint_name)
			})
			.ok_or_else(|| Error::forbidden(invalid_msg))?
			.token_hash
	} else {
		global_token_hash
			.ok_or_else(|| Error::forbidden(invalid_msg))?
			.to_string()
	};

	let token_hash =
		PasswordHash::new(&token_hash_string).ok_or_internal_error("Could not parse token hash")?;

	Argon2::default()
		.verify_password(token.as_bytes(), &token_hash)
		.map_err(|_| Error::forbidden(invalid_msg))?;

	Ok(())
}

pub(crate) fn find_matching_nodes(garage: &Garage, spec: &str) -> Result<Vec<Uuid>, Error> {
	let mut res = vec![];
	if spec == "*" {
		res = garage.system.cluster_layout().all_nodes().to_vec();
		for node in garage.system.get_known_nodes() {
			if node.is_up && !res.contains(&node.id) {
				res.push(node.id);
			}
		}
	} else if spec == "self" {
		res.push(garage.system.id);
	} else {
		let layout = garage.system.cluster_layout();
		let known_nodes = garage.system.get_known_nodes();
		let all_nodes = layout
			.all_nodes()
			.iter()
			.copied()
			.chain(known_nodes.iter().filter(|x| x.is_up).map(|x| x.id));
		for node in all_nodes {
			if !res.contains(&node) && hex::encode(node).starts_with(spec) {
				res.push(node);
			}
		}
		if res.is_empty() {
			return Err(Error::bad_request(format!("No nodes matching {}", spec)));
		}
		if res.len() > 1 {
			return Err(Error::bad_request(format!(
				"Multiple nodes matching {}: {:?}",
				spec, res
			)));
		}
	}
	Ok(res)
}
