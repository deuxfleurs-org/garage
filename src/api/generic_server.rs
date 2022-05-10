use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;

use futures::future::Future;

use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};

use opentelemetry::{
	global,
	metrics::{Counter, ValueRecorder},
	trace::{FutureExt, SpanRef, TraceContextExt, Tracer},
	Context, KeyValue,
};

use garage_util::error::Error as GarageError;
use garage_util::metrics::{gen_trace_id, RecordDuration};

use crate::error::*;

pub(crate) trait ApiEndpoint: Send + Sync + 'static {
	fn name(&self) -> &'static str;
	fn add_span_attributes(&self, span: SpanRef<'_>);
}

#[async_trait]
pub(crate) trait ApiHandler: Send + Sync + 'static {
	const API_NAME: &'static str;
	const API_NAME_DISPLAY: &'static str;

	type Endpoint: ApiEndpoint;

	fn parse_endpoint(&self, r: &Request<Body>) -> Result<Self::Endpoint, Error>;
	async fn handle(
		&self,
		req: Request<Body>,
		endpoint: Self::Endpoint,
	) -> Result<Response<Body>, Error>;
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
		bind_addr: SocketAddr,
		shutdown_signal: impl Future<Output = ()>,
	) -> Result<(), GarageError> {
		let service = make_service_fn(|conn: &AddrStream| {
			let this = self.clone();

			let client_addr = conn.remote_addr();
			async move {
				Ok::<_, GarageError>(service_fn(move |req: Request<Body>| {
					let this = this.clone();

					this.handler(req, client_addr)
				}))
			}
		});

		let server = Server::bind(&bind_addr).serve(service);

		let graceful = server.with_graceful_shutdown(shutdown_signal);
		info!(
			"{} API server listening on http://{}",
			A::API_NAME_DISPLAY,
			bind_addr
		);

		graceful.await?;
		Ok(())
	}

	async fn handler(
		self: Arc<Self>,
		req: Request<Body>,
		addr: SocketAddr,
	) -> Result<Response<Body>, GarageError> {
		let uri = req.uri().clone();
		info!("{} {} {}", addr, req.method(), uri);
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
				let body: Body = Body::from(e.aws_xml(&self.region, uri.path()));
				let mut http_error_builder = Response::builder()
					.status(e.http_status_code())
					.header("Content-Type", "application/xml");

				if let Some(header_map) = http_error_builder.headers_mut() {
					e.add_headers(header_map)
				}

				let http_error = http_error_builder.body(body)?;

				if e.http_status_code().is_server_error() {
					warn!("Response: error {}, {}", e.http_status_code(), e);
				} else {
					info!("Response: error {}, {}", e.http_status_code(), e);
				}
				Ok(http_error)
			}
		}
	}

	async fn handler_stage2(&self, req: Request<Body>) -> Result<Response<Body>, Error> {
		let endpoint = self.api_handler.parse_endpoint(&req)?;
		debug!("Endpoint: {}", endpoint.name());

		let current_context = Context::current();
		let current_span = current_context.span();
		current_span.update_name::<String>(format!("S3 API {}", endpoint.name()));
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
