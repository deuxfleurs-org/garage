use std::fs::{self, Permissions};
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use async_trait::async_trait;

use futures::future::Future;

use http_body_util::BodyExt;
use hyper::header::HeaderValue;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Incoming as IncomingBody, Request, Response};
use hyper::{HeaderMap, StatusCode};
use hyper_util::rt::TokioIo;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UnixListener};

use opentelemetry::{
	global,
	metrics::{Counter, ValueRecorder},
	trace::{FutureExt, SpanRef, TraceContextExt, Tracer},
	Context, KeyValue,
};

use garage_util::error::Error as GarageError;
use garage_util::forwarded_headers;
use garage_util::metrics::{gen_trace_id, RecordDuration};
use garage_util::socket_address::UnixOrTCPSocketAddress;

use crate::helpers::{BoxBody, BytesBody};

pub(crate) trait ApiEndpoint: Send + Sync + 'static {
	fn name(&self) -> &'static str;
	fn add_span_attributes(&self, span: SpanRef<'_>);
}

pub trait ApiError: std::error::Error + Send + Sync + 'static {
	fn http_status_code(&self) -> StatusCode;
	fn add_http_headers(&self, header_map: &mut HeaderMap<HeaderValue>);
	fn http_body(&self, garage_region: &str, path: &str) -> BytesBody;
}

#[async_trait]
pub(crate) trait ApiHandler: Send + Sync + 'static {
	const API_NAME: &'static str;
	const API_NAME_DISPLAY: &'static str;

	type Endpoint: ApiEndpoint;
	type Error: ApiError;

	fn parse_endpoint(&self, r: &Request<IncomingBody>) -> Result<Self::Endpoint, Self::Error>;
	async fn handle(
		&self,
		req: Request<IncomingBody>,
		endpoint: Self::Endpoint,
	) -> Result<Response<BoxBody<Self::Error>>, Self::Error>;
}

pub(crate) struct ApiServer<A: ApiHandler> {
	region: String,
	api_handler: A,

	// Metrics
	request_counter: Counter<u64>,
	error_counter: Counter<u64>,
	request_duration: ValueRecorder<f64>,
}

impl<A: ApiHandler> ApiServer<A> {
	pub fn new(region: String, api_handler: A) -> Arc<Self> {
		let meter = global::meter("garage/api");
		Arc::new(Self {
			region,
			api_handler,
			request_counter: meter
				.u64_counter(format!("api.{}.request_counter", A::API_NAME))
				.with_description(format!(
					"Number of API calls to the various {} API endpoints",
					A::API_NAME_DISPLAY
				))
				.init(),
			error_counter: meter
				.u64_counter(format!("api.{}.error_counter", A::API_NAME))
				.with_description(format!(
					"Number of API calls to the various {} API endpoints that resulted in errors",
					A::API_NAME_DISPLAY
				))
				.init(),
			request_duration: meter
				.f64_value_recorder(format!("api.{}.request_duration", A::API_NAME))
				.with_description(format!(
					"Duration of API calls to the various {} API endpoints",
					A::API_NAME_DISPLAY
				))
				.init(),
		})
	}

	pub async fn run_server(
		self: Arc<Self>,
		bind_addr: UnixOrTCPSocketAddress,
		unix_bind_addr_mode: Option<u32>,
		shutdown_signal: impl Future<Output = ()>,
	) -> Result<(), GarageError> {
		info!(
			"{} API server listening on {}",
			A::API_NAME_DISPLAY,
			bind_addr
		);

		tokio::pin!(shutdown_signal);

		match bind_addr {
			UnixOrTCPSocketAddress::TCPSocket(addr) => {
				let listener = TcpListener::bind(addr).await?;

				loop {
					let (stream, client_addr) = tokio::select! {
						acc = listener.accept() => acc?,
						_ = &mut shutdown_signal => break,
					};

					self.launch_handler(stream, client_addr.to_string());
				}
			}
			UnixOrTCPSocketAddress::UnixSocket(ref path) => {
				if path.exists() {
					fs::remove_file(path)?
				}

				let listener = UnixListener::bind(path)?;

				fs::set_permissions(
					path,
					Permissions::from_mode(unix_bind_addr_mode.unwrap_or(0o222)),
				)?;

				loop {
					let (stream, _) = tokio::select! {
						acc = listener.accept() => acc?,
						_ = &mut shutdown_signal => break,
					};

					self.launch_handler(stream, path.display().to_string());
				}
			}
		};

		Ok(())
	}

	fn launch_handler<S>(self: &Arc<Self>, stream: S, client_addr: String)
	where
		S: AsyncRead + AsyncWrite + Send + Sync + 'static,
	{
		let this = self.clone();
		let io = TokioIo::new(stream);

		let serve =
			move |req: Request<IncomingBody>| this.clone().handler(req, client_addr.to_string());

		tokio::task::spawn(async move {
			let io = Box::pin(io);
			if let Err(e) = http1::Builder::new()
				.serve_connection(io, service_fn(serve))
				.await
			{
				debug!("Error handling HTTP connection: {}", e);
			}
		});
	}

	async fn handler(
		self: Arc<Self>,
		req: Request<IncomingBody>,
		addr: String,
	) -> Result<Response<BoxBody<A::Error>>, GarageError> {
		let uri = req.uri().clone();

		if let Ok(forwarded_for_ip_addr) =
			forwarded_headers::handle_forwarded_for_headers(req.headers())
		{
			info!(
				"{} (via {}) {} {}",
				forwarded_for_ip_addr,
				addr,
				req.method(),
				uri
			);
		} else {
			info!("{} {} {}", addr, req.method(), uri);
		}
		debug!("{:?}", req);

		let tracer = opentelemetry::global::tracer("garage");
		let span = tracer
			.span_builder(format!("{} API call (unknown)", A::API_NAME_DISPLAY))
			.with_trace_id(gen_trace_id())
			.with_attributes(vec![
				KeyValue::new("method", format!("{}", req.method())),
				KeyValue::new("uri", req.uri().to_string()),
			])
			.start(&tracer);

		let res = self
			.handler_stage2(req)
			.with_context(Context::current_with_span(span))
			.await;

		match res {
			Ok(x) => {
				debug!("{} {:?}", x.status(), x.headers());
				Ok(x)
			}
			Err(e) => {
				let body = e.http_body(&self.region, uri.path());
				let mut http_error_builder = Response::builder().status(e.http_status_code());

				if let Some(header_map) = http_error_builder.headers_mut() {
					e.add_http_headers(header_map)
				}

				let http_error = http_error_builder.body(body)?;

				if e.http_status_code().is_server_error() {
					warn!("Response: error {}, {}", e.http_status_code(), e);
				} else {
					info!("Response: error {}, {}", e.http_status_code(), e);
				}
				Ok(http_error.map(|body| BoxBody::new(body.map_err(|_| unreachable!()))))
			}
		}
	}

	async fn handler_stage2(
		&self,
		req: Request<IncomingBody>,
	) -> Result<Response<BoxBody<A::Error>>, A::Error> {
		let endpoint = self.api_handler.parse_endpoint(&req)?;
		debug!("Endpoint: {}", endpoint.name());

		let current_context = Context::current();
		let current_span = current_context.span();
		current_span.update_name::<String>(format!(
			"{} API {}",
			A::API_NAME_DISPLAY,
			endpoint.name()
		));
		current_span.set_attribute(KeyValue::new("endpoint", endpoint.name()));
		endpoint.add_span_attributes(current_span);

		let metrics_tags = &[KeyValue::new("api_endpoint", endpoint.name())];

		let res = self
			.api_handler
			.handle(req, endpoint)
			.record_duration(&self.request_duration, &metrics_tags[..])
			.await;

		self.request_counter.add(1, &metrics_tags[..]);

		let status_code = match &res {
			Ok(r) => r.status(),
			Err(e) => e.http_status_code(),
		};
		if status_code.is_client_error() || status_code.is_server_error() {
			self.error_counter.add(
				1,
				&[
					metrics_tags[0].clone(),
					KeyValue::new("status_code", status_code.as_str().to_string()),
				],
			);
		}

		res
	}
}
