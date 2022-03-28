use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use futures::future::*;
use hyper::{
	header::CONTENT_TYPE,
	service::{make_service_fn, service_fn},
	Body, Method, Request, Response, Server,
};

use opentelemetry::{
	global,
	metrics::{BoundCounter, BoundValueRecorder},
	trace::{FutureExt, TraceContextExt, Tracer},
	Context,
};
use opentelemetry_prometheus::PrometheusExporter;

use prometheus::{Encoder, TextEncoder};

use garage_util::error::Error as GarageError;
use garage_util::metrics::*;

// serve_req on metric endpoint
async fn serve_req(
	req: Request<Body>,
	admin_server: Arc<AdminServer>,
) -> Result<Response<Body>, hyper::Error> {
	debug!("Receiving request at path {}", req.uri());
	let request_start = SystemTime::now();

	admin_server.metrics.http_counter.add(1);

	let response = match (req.method(), req.uri().path()) {
		(&Method::GET, "/metrics") => {
			let mut buffer = vec![];
			let encoder = TextEncoder::new();

			let tracer = opentelemetry::global::tracer("garage");
			let metric_families = tracer.in_span("admin/gather_metrics", |_| {
				admin_server.exporter.registry().gather()
			});

			encoder.encode(&metric_families, &mut buffer).unwrap();
			admin_server
				.metrics
				.http_body_gauge
				.record(buffer.len() as u64);

			Response::builder()
				.status(200)
				.header(CONTENT_TYPE, encoder.format_type())
				.body(Body::from(buffer))
				.unwrap()
		}
		_ => Response::builder()
			.status(404)
			.body(Body::from("Not implemented"))
			.unwrap(),
	};

	admin_server
		.metrics
		.http_req_histogram
		.record(request_start.elapsed().map_or(0.0, |d| d.as_secs_f64()));
	Ok(response)
}

// AdminServer hold the admin server internal admin_server and the metric exporter
pub struct AdminServer {
	exporter: PrometheusExporter,
	metrics: AdminServerMetrics,
}

// GarageMetricadmin_server holds the metrics counter definition for Garage
// FIXME: we would rather have that split up among the different libraries?
struct AdminServerMetrics {
	http_counter: BoundCounter<u64>,
	http_body_gauge: BoundValueRecorder<u64>,
	http_req_histogram: BoundValueRecorder<f64>,
}

impl AdminServer {
	/// init initilialize the AdminServer and background metric server
	pub fn init() -> AdminServer {
		let exporter = opentelemetry_prometheus::exporter().init();
		let meter = global::meter("garage/admin_server");
		AdminServer {
			exporter,
			metrics: AdminServerMetrics {
				http_counter: meter
					.u64_counter("admin.http_requests_total")
					.with_description("Total number of HTTP requests made.")
					.init()
					.bind(&[]),
				http_body_gauge: meter
					.u64_value_recorder("admin.http_response_size_bytes")
					.with_description("The metrics HTTP response sizes in bytes.")
					.init()
					.bind(&[]),
				http_req_histogram: meter
					.f64_value_recorder("admin.http_request_duration_seconds")
					.with_description("The HTTP request latencies in seconds.")
					.init()
					.bind(&[]),
			},
		}
	}
	/// run execute the admin server on the designated HTTP port and listen for requests
	pub async fn run(
		self,
		bind_addr: SocketAddr,
		shutdown_signal: impl Future<Output = ()>,
	) -> Result<(), GarageError> {
		let admin_server = Arc::new(self);
		// For every connection, we must make a `Service` to handle all
		// incoming HTTP requests on said connection.
		let make_svc = make_service_fn(move |_conn| {
			let admin_server = admin_server.clone();
			// This is the `Service` that will handle the connection.
			// `service_fn` is a helper to convert a function that
			// returns a Response into a `Service`.
			async move {
				Ok::<_, Infallible>(service_fn(move |req| {
					let tracer = opentelemetry::global::tracer("garage");
					let span = tracer
						.span_builder("admin/request")
						.with_trace_id(gen_trace_id())
						.start(&tracer);

					serve_req(req, admin_server.clone())
						.with_context(Context::current_with_span(span))
				}))
			}
		});

		let server = Server::bind(&bind_addr).serve(make_svc);
		let graceful = server.with_graceful_shutdown(shutdown_signal);
		info!("Admin server listening on http://{}", bind_addr);

		graceful.await?;
		Ok(())
	}
}
