use std::convert::Infallible;
use std::fs::{self, Permissions};
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::Duration;

use futures::future::Future;
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt};

use http_body_util::BodyExt;
use hyper::header::HeaderValue;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Incoming as IncomingBody, Request, Response};
use hyper::{HeaderMap, StatusCode};
use hyper_util::rt::TokioIo;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::sync::watch;
use tokio::time::{sleep_until, Instant};

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

use crate::helpers::{BoxBody, ErrorBody};

pub trait ApiEndpoint: Send + Sync + 'static {
	fn name(&self) -> &'static str;
	fn add_span_attributes(&self, span: SpanRef<'_>);
}

pub trait ApiError: std::error::Error + Send + Sync + 'static {
	fn http_status_code(&self) -> StatusCode;
	fn add_http_headers(&self, header_map: &mut HeaderMap<HeaderValue>);
	fn http_body(&self, garage_region: &str, path: &str) -> ErrorBody;
}

pub trait ApiHandler: Send + Sync + 'static {
	const API_NAME: &'static str;
	const API_NAME_DISPLAY: &'static str;

	type Endpoint: ApiEndpoint;
	type Error: ApiError;

	fn parse_endpoint(&self, r: &Request<IncomingBody>) -> Result<Self::Endpoint, Self::Error>;
	fn handle(
		&self,
		req: Request<IncomingBody>,
		endpoint: Self::Endpoint,
	) -> impl Future<Output = Result<Response<BoxBody<Self::Error>>, Self::Error>> + Send;
}

pub struct ApiServer<A: ApiHandler> {
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
		must_exit: watch::Receiver<bool>,
	) -> Result<(), GarageError> {
		let server_name = format!("{} API", A::API_NAME_DISPLAY);
		info!("{} server listening on {}", server_name, bind_addr);

		match bind_addr {
			UnixOrTCPSocketAddress::TCPSocket(addr) => {
				let listener = TcpListener::bind(addr).await?;

				let handler = move |request, socketaddr| self.clone().handler(request, socketaddr);
				server_loop(server_name, listener, handler, must_exit).await
			}
			UnixOrTCPSocketAddress::UnixSocket(ref path) => {
				if path.exists() {
					fs::remove_file(path)?
				}

				let listener = UnixListener::bind(path)?;
				let listener = UnixListenerOn(listener, path.display().to_string());

				fs::set_permissions(
					path,
					Permissions::from_mode(unix_bind_addr_mode.unwrap_or(0o222)),
				)?;

				let handler = move |request, socketaddr| self.clone().handler(request, socketaddr);
				server_loop(server_name, listener, handler, must_exit).await
			}
		}
	}

	async fn handler(
		self: Arc<Self>,
		req: Request<IncomingBody>,
		addr: String,
	) -> Result<Response<BoxBody<A::Error>>, http::Error> {
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
				Ok(http_error
					.map(|body| BoxBody::new(body.map_err(|_: Infallible| unreachable!()))))
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

// ==== helper functions ====

pub trait Accept: Send + Sync + 'static {
	type Stream: AsyncRead + AsyncWrite + Send + Sync + 'static;
	fn accept(&self) -> impl Future<Output = std::io::Result<(Self::Stream, String)>> + Send;
}

impl Accept for TcpListener {
	type Stream = TcpStream;
	async fn accept(&self) -> std::io::Result<(Self::Stream, String)> {
		self.accept()
			.await
			.map(|(stream, addr)| (stream, addr.to_string()))
	}
}

pub struct UnixListenerOn(pub UnixListener, pub String);

impl Accept for UnixListenerOn {
	type Stream = UnixStream;
	async fn accept(&self) -> std::io::Result<(Self::Stream, String)> {
		self.0
			.accept()
			.await
			.map(|(stream, _addr)| (stream, self.1.clone()))
	}
}

pub async fn server_loop<A, H, F, E>(
	server_name: String,
	listener: A,
	handler: H,
	mut must_exit: watch::Receiver<bool>,
) -> Result<(), GarageError>
where
	A: Accept,
	H: Fn(Request<IncomingBody>, String) -> F + Send + Sync + Clone + 'static,
	F: Future<Output = Result<Response<BoxBody<E>>, http::Error>> + Send + 'static,
	E: Send + Sync + std::error::Error + 'static,
{
	let (conn_in, mut conn_out) = tokio::sync::mpsc::unbounded_channel();
	let connection_collector = tokio::spawn({
		let server_name = server_name.clone();
		async move {
			let mut connections = FuturesUnordered::<tokio::task::JoinHandle<()>>::new();
			loop {
				let collect_next = async {
					if connections.is_empty() {
						futures::future::pending().await
					} else {
						connections.next().await
					}
				};
				tokio::select! {
					result = collect_next => {
						trace!("{} server: HTTP connection finished: {:?}", server_name, result);
					}
					new_fut = conn_out.recv() => {
						match new_fut {
							Some(f) => connections.push(f),
							None => break,
						}
					}
				}
			}
			let deadline = Instant::now() + Duration::from_secs(10);
			while !connections.is_empty() {
				info!(
					"{} server: {} connections still open, deadline in {:.2}s",
					server_name,
					connections.len(),
					(deadline - Instant::now()).as_secs_f32(),
				);
				tokio::select! {
					conn_res = connections.next() => {
						trace!(
							"{} server: HTTP connection finished: {:?}",
							server_name,
							conn_res.unwrap(),
						);
					}
					_ = sleep_until(deadline) => {
						warn!("{} server: exit deadline reached with {} connections still open, killing them now",
							server_name,
							connections.len());
						for conn in connections.iter() {
							conn.abort();
						}
						for conn in connections {
							assert!(conn.await.unwrap_err().is_cancelled());
						}
						break;
					}
				}
			}
		}
	});

	while !*must_exit.borrow() {
		let (stream, client_addr) = tokio::select! {
			acc = listener.accept() => acc?,
			_ = must_exit.changed() => continue,
		};

		let io = TokioIo::new(stream);

		let handler = handler.clone();
		let serve = move |req: Request<IncomingBody>| handler(req, client_addr.clone());

		let fut = tokio::task::spawn(async move {
			let io = Box::pin(io);
			if let Err(e) = http1::Builder::new()
				.serve_connection(io, service_fn(serve))
				.await
			{
				debug!("Error handling HTTP connection: {}", e);
			}
		});
		conn_in.send(fut)?;
	}

	info!("{} server exiting", server_name);
	drop(conn_in);
	connection_collector.await?;

	Ok(())
}
