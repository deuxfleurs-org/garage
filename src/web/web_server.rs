use std::{borrow::Cow, convert::Infallible, net::SocketAddr, sync::Arc};

use futures::future::Future;

use http::header::{ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD};
use hyper::{
	header::{HeaderValue, HOST},
	server::conn::AddrStream,
	service::{make_service_fn, service_fn},
	Body, Method, Request, Response, Server, StatusCode,
};

use crate::error::*;

use garage_api::error::{Error as ApiError, OkOrBadRequest, OkOrInternalError};
use garage_api::helpers::{authority_to_host, host_to_bucket};
use garage_api::s3_cors::{add_cors_headers, cors_rule_matches};
use garage_api::s3_get::{handle_get, handle_head};

use garage_model::bucket_table::Bucket;
use garage_model::garage::Garage;

use garage_table::*;
use garage_util::error::Error as GarageError;

/// Run a web server
pub async fn run_web_server(
	garage: Arc<Garage>,
	shutdown_signal: impl Future<Output = ()>,
) -> Result<(), GarageError> {
	let addr = &garage.config.s3_web.bind_addr;

	let service = make_service_fn(|conn: &AddrStream| {
		let garage = garage.clone();
		let client_addr = conn.remote_addr();
		async move {
			Ok::<_, Error>(service_fn(move |req: Request<Body>| {
				let garage = garage.clone();
				handle_request(garage, req, client_addr)
			}))
		}
	});

	let server = Server::bind(addr).serve(service);
	let graceful = server.with_graceful_shutdown(shutdown_signal);
	info!("Web server listening on http://{}", addr);

	graceful.await?;
	Ok(())
}

async fn handle_request(
	garage: Arc<Garage>,
	req: Request<Body>,
	addr: SocketAddr,
) -> Result<Response<Body>, Infallible> {
	info!("{} {} {}", addr, req.method(), req.uri());
	match serve_file(garage, &req).await {
		Ok(res) => {
			debug!("{} {} {}", req.method(), req.uri(), res.status());
			Ok(res)
		}
		Err(error) => {
			info!(
				"{} {} {} {}",
				req.method(),
				req.uri(),
				error.http_status_code(),
				error
			);
			Ok(error_to_res(error))
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

async fn serve_file(garage: Arc<Garage>, req: &Request<Body>) -> Result<Response<Body>, Error> {
	// Get http authority string (eg. [::1]:3902 or garage.tld:80)
	let authority = req
		.headers()
		.get(HOST)
		.ok_or_bad_request("HOST header required")?
		.to_str()?;

	// Get bucket
	let host = authority_to_host(authority)?;
	let root = &garage.config.s3_web.root_domain;

	let bucket_name = host_to_bucket(&host, root).unwrap_or(&host);
	let bucket_id = garage
		.bucket_alias_table
		.get(&EmptyKey, &bucket_name.to_string())
		.await?
		.map(|x| x.state.take())
		.flatten()
		.ok_or(Error::NotFound)?;

	// Check bucket isn't deleted and has website access enabled
	let bucket = garage
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
	let key = path_to_key(&path, index)?;

	debug!(
		"Selected bucket: \"{}\" {:?}, selected key: \"{}\"",
		bucket_name, bucket_id, key
	);

	let ret_doc = match *req.method() {
		Method::OPTIONS => return handle_options(&bucket, req),
		Method::HEAD => {
			return handle_head(garage.clone(), req, bucket_id, &key)
				.await
				.map_err(Error::from)
		}
		Method::GET => handle_get(garage.clone(), req, bucket_id, &key).await,
		_ => Err(ApiError::BadRequest("HTTP method not supported".into())),
	}
	.map_err(Error::from);

	match ret_doc {
		Err(error) => {
			// For a HEAD or OPTIONS method, we don't return the error document
			// as content, we return above and just return the error message
			// by relying on err_to_res that is called when we return an Err.
			assert!(*req.method() != Method::HEAD && *req.method() != Method::OPTIONS);

			if !error.http_status_code().is_client_error() {
				// Do not return the error document if it is not a 4xx error code.
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

			match handle_get(garage, &req2, bucket_id, &error_document).await {
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
					error.add_headers(error_doc.headers_mut());

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
			if let Some(cors_config) = bucket.params().unwrap().cors_config.get() {
				if let Some(origin) = req.headers().get("Origin") {
					let origin = origin.to_str()?;
					let request_headers = match req.headers().get(ACCESS_CONTROL_REQUEST_HEADERS) {
						Some(h) => h.to_str()?.split(',').map(|h| h.trim()).collect::<Vec<_>>(),
						None => vec![],
					};
					let matching_rule = cors_config.iter().find(|rule| {
						cors_rule_matches(
							rule,
							origin,
							&req.method().to_string(),
							request_headers.iter(),
						)
					});
					if let Some(rule) = matching_rule {
						add_cors_headers(&mut resp, rule)
							.ok_or_internal_error("Invalid CORS configuration")?;
					}
				}
			}
			Ok(resp)
		}
	}
}

fn handle_options(bucket: &Bucket, req: &Request<Body>) -> Result<Response<Body>, Error> {
	let origin = req
		.headers()
		.get("Origin")
		.ok_or_bad_request("Missing Origin header")?
		.to_str()?;
	let request_method = req
		.headers()
		.get(ACCESS_CONTROL_REQUEST_METHOD)
		.ok_or_bad_request("Missing Access-Control-Request-Method header")?
		.to_str()?;
	let request_headers = match req.headers().get(ACCESS_CONTROL_REQUEST_HEADERS) {
		Some(h) => h.to_str()?.split(',').map(|h| h.trim()).collect::<Vec<_>>(),
		None => vec![],
	};

	if let Some(cors_config) = bucket.params().unwrap().cors_config.get() {
		let matching_rule = cors_config
			.iter()
			.find(|rule| cors_rule_matches(rule, origin, request_method, request_headers.iter()));
		if let Some(rule) = matching_rule {
			let mut resp = Response::builder()
				.status(StatusCode::OK)
				.body(Body::empty())
				.map_err(ApiError::from)?;
			add_cors_headers(&mut resp, rule).ok_or_internal_error("Invalid CORS configuration")?;
			return Ok(resp);
		}
	}

	Err(ApiError::Forbidden("No matching CORS rule".into()).into())
}

/// Path to key
///
/// Convert the provided path to the internal key
/// When a path ends with "/", we append the index name to match traditional web server behavior
/// which is also AWS S3 behavior.
fn path_to_key<'a>(path: &'a str, index: &str) -> Result<Cow<'a, str>, Error> {
	let path_utf8 = percent_encoding::percent_decode_str(path).decode_utf8()?;

	if !path_utf8.starts_with('/') {
		return Err(Error::BadRequest(
			"Path must start with a / (slash)".to_string(),
		));
	}

	match path_utf8.chars().last() {
		None => unreachable!(),
		Some('/') => {
			let mut key = String::with_capacity(path_utf8.len() + index.len());
			key.push_str(&path_utf8[1..]);
			key.push_str(index);
			Ok(key.into())
		}
		Some(_) => match path_utf8 {
			Cow::Borrowed(pu8) => Ok((&pu8[1..]).into()),
			Cow::Owned(pu8) => Ok((&pu8[1..]).to_string().into()),
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn path_to_key_test() -> Result<(), Error> {
		assert_eq!(path_to_key("/file%20.jpg", "index.html")?, "file .jpg");
		assert_eq!(path_to_key("/%20t/", "index.html")?, " t/index.html");
		assert_eq!(path_to_key("/", "index.html")?, "index.html");
		assert_eq!(path_to_key("/hello", "index.html")?, "hello");
		assert!(path_to_key("", "index.html").is_err());
		assert!(path_to_key("i/am/relative", "index.html").is_err());
		Ok(())
	}
}
