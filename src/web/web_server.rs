use std::fs::{self, Permissions};
use std::os::unix::prelude::PermissionsExt;
use std::{convert::Infallible, sync::Arc};

use futures::future::Future;

use hyper::{
	header::{HeaderValue, HOST},
	server::conn::AddrStream,
	service::{make_service_fn, service_fn},
	Body, Method, Request, Response, Server, StatusCode,
};

use hyperlocal::UnixServerExt;

use tokio::net::UnixStream;

use opentelemetry::{
	global,
	metrics::{Counter, ValueRecorder},
	trace::{FutureExt, TraceContextExt, Tracer},
	Context, KeyValue,
};

use crate::error::*;

use garage_api::helpers::{authority_to_host, host_to_bucket};
use garage_api::s3::cors::{add_cors_headers, find_matching_cors_rule, handle_options_for_bucket};
use garage_api::s3::error::{
	CommonErrorDerivative, Error as ApiError, OkOrBadRequest, OkOrInternalError,
};
use garage_api::s3::get::{handle_get, handle_head};

use garage_model::garage::Garage;

use garage_table::*;
use garage_util::data::Uuid;
use garage_util::error::Error as GarageError;
use garage_util::forwarded_headers;
use garage_util::metrics::{gen_trace_id, RecordDuration};
use garage_util::socket_address::UnixOrTCPSocketAddress;

struct WebMetrics {
	request_counter: Counter<u64>,
	error_counter: Counter<u64>,
	request_duration: ValueRecorder<f64>,
}

impl WebMetrics {
	fn new() -> Self {
		let meter = global::meter("garage/web");
		Self {
			request_counter: meter
				.u64_counter("web.request_counter")
				.with_description("Number of requests to the web endpoint")
				.init(),
			error_counter: meter
				.u64_counter("web.error_counter")
				.with_description("Number of requests to the web endpoint resulting in errors")
				.init(),
			request_duration: meter
				.f64_value_recorder("web.request_duration")
				.with_description("Duration of requests to the web endpoint")
				.init(),
		}
	}
}

pub struct WebServer {
	garage: Arc<Garage>,
	metrics: Arc<WebMetrics>,
	root_domain: String,
}

impl WebServer {
	/// Run a web server
	pub async fn run(
		garage: Arc<Garage>,
		addr: UnixOrTCPSocketAddress,
		root_domain: String,
		shutdown_signal: impl Future<Output = ()>,
	) -> Result<(), GarageError> {
		let metrics = Arc::new(WebMetrics::new());
		let web_server = Arc::new(WebServer {
			garage,
			metrics,
			root_domain,
		});

		let tcp_service = make_service_fn(|conn: &AddrStream| {
			let web_server = web_server.clone();

			let client_addr = conn.remote_addr();
			async move {
				Ok::<_, Error>(service_fn(move |req: Request<Body>| {
					let web_server = web_server.clone();

					web_server.handle_request(req, client_addr.to_string())
				}))
			}
		});

		let unix_service = make_service_fn(|_: &UnixStream| {
			let web_server = web_server.clone();

			let path = addr.to_string();
			async move {
				Ok::<_, Error>(service_fn(move |req: Request<Body>| {
					let web_server = web_server.clone();

					web_server.handle_request(req, path.clone())
				}))
			}
		});

		info!("Web server listening on {}", addr);

		match addr {
			UnixOrTCPSocketAddress::TCPSocket(addr) => {
				Server::bind(&addr)
					.serve(tcp_service)
					.with_graceful_shutdown(shutdown_signal)
					.await?
			}
			UnixOrTCPSocketAddress::UnixSocket(ref path) => {
				if path.exists() {
					fs::remove_file(path)?
				}

				let bound = Server::bind_unix(path)?;

				fs::set_permissions(path, Permissions::from_mode(0o222))?;

				bound
					.serve(unix_service)
					.with_graceful_shutdown(shutdown_signal)
					.await?;
			}
		};

		Ok(())
	}

	async fn handle_request(
		self: Arc<Self>,
		req: Request<Body>,
		addr: String,
	) -> Result<Response<Body>, Infallible> {
		if let Ok(forwarded_for_ip_addr) =
			forwarded_headers::handle_forwarded_for_headers(req.headers())
		{
			info!(
				"{} (via {}) {} {}",
				forwarded_for_ip_addr,
				addr,
				req.method(),
				req.uri()
			);
		} else {
			info!("{} {} {}", addr, req.method(), req.uri());
		}

		// Lots of instrumentation
		let tracer = opentelemetry::global::tracer("garage");
		let span = tracer
			.span_builder(format!("Web {} request", req.method()))
			.with_trace_id(gen_trace_id())
			.with_attributes(vec![
				KeyValue::new("method", format!("{}", req.method())),
				KeyValue::new("uri", req.uri().to_string()),
			])
			.start(&tracer);

		let metrics_tags = &[KeyValue::new("method", req.method().to_string())];

		// The actual handler
		let res = self
			.serve_file(&req)
			.with_context(Context::current_with_span(span))
			.record_duration(&self.metrics.request_duration, &metrics_tags[..])
			.await;

		// More instrumentation
		self.metrics.request_counter.add(1, &metrics_tags[..]);

		// Returning the result
		match res {
			Ok(res) => {
				debug!("{} {} {}", req.method(), res.status(), req.uri());
				Ok(res)
			}
			Err(error) => {
				info!(
					"{} {} {} {}",
					req.method(),
					error.http_status_code(),
					req.uri(),
					error
				);
				self.metrics.error_counter.add(
					1,
					&[
						metrics_tags[0].clone(),
						KeyValue::new("status_code", error.http_status_code().to_string()),
					],
				);
				Ok(error_to_res(error))
			}
		}
	}

	async fn check_key_exists(self: &Arc<Self>, bucket_id: Uuid, key: &str) -> Result<bool, Error> {
		let exists = self
			.garage
			.object_table
			.get(&bucket_id, &key.to_string())
			.await?
			.map(|object| object.versions().iter().any(|v| v.is_data()))
			.unwrap_or(false);
		Ok(exists)
	}

	async fn serve_file(self: &Arc<Self>, req: &Request<Body>) -> Result<Response<Body>, Error> {
		// Get http authority string (eg. [::1]:3902 or garage.tld:80)
		let authority = req
			.headers()
			.get(HOST)
			.ok_or_bad_request("HOST header required")?
			.to_str()?;

		// Get bucket
		let host = authority_to_host(authority)?;

		let bucket_name = host_to_bucket(&host, &self.root_domain).unwrap_or(&host);
		let bucket_id = self
			.garage
			.bucket_alias_table
			.get(&EmptyKey, &bucket_name.to_string())
			.await?
			.and_then(|x| x.state.take())
			.ok_or(Error::NotFound)?;

		// Check bucket isn't deleted and has website access enabled
		let bucket = self
			.garage
			.bucket_table
			.get(&EmptyKey, &bucket_id)
			.await?
			.ok_or(Error::NotFound)?;

		let website_config = bucket
			.params()
			.ok_or(Error::NotFound)?
			.website_config
			.get()
			.as_ref()
			.ok_or(Error::NotFound)?;

		// Get path
		let path = req.uri().path().to_string();
		let index = &website_config.index_document;
		let (key, may_redirect) = path_to_keys(&path, index)?;

		debug!(
			"Selected bucket: \"{}\" {:?}, target key: \"{}\", may redirect to: {:?}",
			bucket_name, bucket_id, key, may_redirect
		);

		let ret_doc = match *req.method() {
			Method::OPTIONS => handle_options_for_bucket(req, &bucket),
			Method::HEAD => handle_head(self.garage.clone(), req, bucket_id, &key, None).await,
			Method::GET => handle_get(self.garage.clone(), req, bucket_id, &key, None).await,
			_ => Err(ApiError::bad_request("HTTP method not supported")),
		};

		// Try implicit redirect on error
		let ret_doc_with_redir = match (&ret_doc, may_redirect) {
			(Err(ApiError::NoSuchKey), ImplicitRedirect::To { key, url })
				if self.check_key_exists(bucket_id, key.as_str()).await? =>
			{
				Ok(Response::builder()
					.status(StatusCode::FOUND)
					.header("Location", url)
					.body(Body::empty())
					.unwrap())
			}
			_ => ret_doc,
		};

		match ret_doc_with_redir.map_err(Error::from) {
			Err(error) => {
				// For a HEAD or OPTIONS method, and for non-4xx errors,
				// we don't return the error document as content,
				// we return above and just return the error message
				// by relying on err_to_res that is called when we return an Err.
				if *req.method() == Method::HEAD
					|| *req.method() == Method::OPTIONS
					|| !error.http_status_code().is_client_error()
				{
					return Err(error);
				}

				// If no error document is set: just return the error directly
				let error_document = match &website_config.error_document {
					Some(ed) => ed.trim_start_matches('/').to_owned(),
					None => return Err(error),
				};

				// We want to return the error document
				// Create a fake HTTP request with path = the error document
				let req2 = Request::builder()
					.uri(format!("http://{}/{}", host, &error_document))
					.body(Body::empty())
					.unwrap();

				match handle_get(self.garage.clone(), &req2, bucket_id, &error_document, None).await
				{
					Ok(mut error_doc) => {
						// The error won't be logged back in handle_request,
						// so log it here
						info!(
							"{} {} {} {}",
							req.method(),
							req.uri(),
							error.http_status_code(),
							error
						);

						*error_doc.status_mut() = error.http_status_code();

						// Preserve error message in a special header
						for error_line in error.to_string().split('\n') {
							if let Ok(v) = HeaderValue::from_bytes(error_line.as_bytes()) {
								error_doc.headers_mut().append("X-Garage-Error", v);
							}
						}

						Ok(error_doc)
					}
					Err(error_doc_error) => {
						warn!(
							"Couldn't get error document {} for bucket {:?}: {}",
							error_document, bucket_id, error_doc_error
						);
						Err(error)
					}
				}
			}
			Ok(mut resp) => {
				// Maybe add CORS headers
				if let Some(rule) = find_matching_cors_rule(&bucket, req)? {
					add_cors_headers(&mut resp, rule)
						.ok_or_internal_error("Invalid bucket CORS configuration")?;
				}
				Ok(resp)
			}
		}
	}
}

fn error_to_res(e: Error) -> Response<Body> {
	// If we are here, it is either that:
	// - there was an error before trying to get the requested URL
	//   from the bucket (e.g. bucket not found)
	// - there was an error processing the request and (the request
	//   was a HEAD request or we couldn't get the error document)
	// We do NOT enter this code path when returning the bucket's
	// error document (this is handled in serve_file)
	let body = Body::from(format!("{}\n", e));
	let mut http_error = Response::new(body);
	*http_error.status_mut() = e.http_status_code();
	e.add_headers(http_error.headers_mut());
	http_error
}

#[derive(Debug, PartialEq)]
enum ImplicitRedirect {
	No,
	To { key: String, url: String },
}

/// Path to key
///
/// Convert the provided path to the internal key
/// When a path ends with "/", we append the index name to match traditional web server behavior
/// which is also AWS S3 behavior.
///
/// Check: https://docs.aws.amazon.com/AmazonS3/latest/userguide/IndexDocumentSupport.html
fn path_to_keys<'a>(path: &'a str, index: &str) -> Result<(String, ImplicitRedirect), Error> {
	let path_utf8 = percent_encoding::percent_decode_str(path).decode_utf8()?;

	let base_key = match path_utf8.strip_prefix("/") {
		Some(bk) => bk,
		None => return Err(Error::BadRequest("Path must start with a / (slash)".into())),
	};
	let is_bucket_root = base_key.len() == 0;
	let is_trailing_slash = path_utf8.ends_with("/");

	match (is_bucket_root, is_trailing_slash) {
		// It is not possible to store something at the root of the bucket (ie. empty key),
		// the only option is to fetch the index
		(true, _) => Ok((index.to_string(), ImplicitRedirect::No)),

		// "If you create a folder structure in your bucket, you must have an index document at each level. In each folder, the index document must have the same name, for example, index.html. When a user specifies a URL that resembles a folder lookup, the presence or absence of a trailing slash determines the behavior of the website. For example, the following URL, with a trailing slash, returns the photos/index.html index document."
		(false, true) => Ok((format!("{base_key}{index}"), ImplicitRedirect::No)),

		// "However, if you exclude the trailing slash from the preceding URL, Amazon S3 first looks for an object photos in the bucket. If the photos object is not found, it searches for an index document, photos/index.html. If that document is found, Amazon S3 returns a 302 Found message and points to the photos/ key. For subsequent requests to photos/, Amazon S3 returns photos/index.html. If the index document is not found, Amazon S3 returns an error."
		(false, false) => Ok((
			base_key.to_string(),
			ImplicitRedirect::To {
				key: format!("{base_key}/{index}"),
				url: format!("{path}/"),
			},
		)),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn path_to_keys_test() -> Result<(), Error> {
		assert_eq!(
			path_to_keys("/file%20.jpg", "index.html")?,
			(
				"file .jpg".to_string(),
				ImplicitRedirect::To {
					key: "file .jpg/index.html".to_string(),
					url: "/file%20.jpg/".to_string()
				}
			)
		);
		assert_eq!(
			path_to_keys("/%20t/", "index.html")?,
			(" t/index.html".to_string(), ImplicitRedirect::No)
		);
		assert_eq!(
			path_to_keys("/", "index.html")?,
			("index.html".to_string(), ImplicitRedirect::No)
		);
		assert_eq!(
			path_to_keys("/hello", "index.html")?,
			(
				"hello".to_string(),
				ImplicitRedirect::To {
					key: "hello/index.html".to_string(),
					url: "/hello/".to_string()
				}
			)
		);
		assert!(path_to_keys("", "index.html").is_err());
		assert!(path_to_keys("i/am/relative", "index.html").is_err());
		Ok(())
	}
}
