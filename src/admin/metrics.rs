use hyper::{
	header::CONTENT_TYPE,
	service::{make_service_fn, service_fn},
	Body, Method, Request, Response, Server,
};
use lazy_static::lazy_static;
use opentelemetry::{
	global,
	metrics::{BoundCounter, BoundValueRecorder},
	KeyValue,
};
use opentelemetry_prometheus::PrometheusExporter;
use prometheus::{Encoder, TextEncoder};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::SystemTime;

use futures::future::*;
use garage_model::garage::Garage;
use garage_util::error::Error as GarageError;

lazy_static! {
	// This defines the differennt tags that will be referenced by the object
	static ref HANDLER_ALL: [KeyValue; 1] = [KeyValue::new("handler", "all")];
}

// serve_req on metric endpoint
async fn serve_req(
	req: Request<Body>,
	admin_server: Arc<AdminServer>,
) -> Result<Response<Body>, hyper::Error> {
	println!("Receiving request at path {}", req.uri());
	let request_start = SystemTime::now();

	admin_server.metrics.http_counter.add(1);

	let response = match (req.method(), req.uri().path()) {
		(&Method::GET, "/metrics") => {
			let mut buffer = vec![];
			let encoder = TextEncoder::new();
			let metric_families = admin_server.exporter.registry().gather();
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
	bucket_v2_merkle_updater_todo_queue_length: BoundValueRecorder<f64>,
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
					.u64_counter("router.http_requests_total")
					.with_description("Total number of HTTP requests made.")
					.init()
					.bind(HANDLER_ALL.as_ref()),
				http_body_gauge: meter
					.u64_value_recorder("example.http_response_size_bytes")
					.with_description("The metrics HTTP response sizes in bytes.")
					.init()
					.bind(HANDLER_ALL.as_ref()),
				http_req_histogram: meter
					.f64_value_recorder("example.http_request_duration_seconds")
					.with_description("The HTTP request latencies in seconds.")
					.init()
					.bind(HANDLER_ALL.as_ref()),
				bucket_v2_merkle_updater_todo_queue_length: meter
					.f64_value_recorder("bucket_v2.merkle_updater.todo_queue_length")
					.with_description("Bucket merkle updater TODO queue length.")
					.init()
					.bind(HANDLER_ALL.as_ref()),
			},
		}
	}
	/// run execute the admin server on the designated HTTP port and listen for requests
	pub async fn run(
		self,
		garage: Arc<Garage>,
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
				Ok::<_, Infallible>(service_fn(move |req| serve_req(req, admin_server.clone())))
			}
		});

		let addr = &garage.config.admin_api.bind_addr;

		let server = Server::bind(&addr).serve(make_svc);
		let graceful = server.with_graceful_shutdown(shutdown_signal);
		info!("Admin server listening on http://{}", addr);

		graceful.await?;
		Ok(())
	}
}
