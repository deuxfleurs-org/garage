use std::{borrow::Cow, convert::Infallible, net::SocketAddr, sync::Arc};

use futures::future::Future;

use hyper::{
	header::HOST,
	server::conn::AddrStream,
	service::{make_service_fn, service_fn},
	Body, Method, Request, Response, Server,
};

use crate::error::*;
use garage_api::helpers::{authority_to_host, host_to_bucket};
use garage_api::s3_get::{handle_get, handle_head};
use garage_model::bucket_table::*;
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
	let res = serve_file(garage, req).await;
	match &res {
		Ok(r) => debug!("{} {:?}", r.status(), r.headers()),
		Err(e) => warn!("Response: error {}, {}", e.http_status_code(), e),
	}

	Ok(res.unwrap_or_else(error_to_res))
}

fn error_to_res(e: Error) -> Response<Body> {
	let body: Body = Body::from(format!("{}\n", e));
	let mut http_error = Response::new(body);
	*http_error.status_mut() = e.http_status_code();
	http_error
}

async fn serve_file(garage: Arc<Garage>, req: Request<Body>) -> Result<Response<Body>, Error> {
	// Get http authority string (eg. [::1]:3902 or garage.tld:80)
	let authority = req
		.headers()
		.get(HOST)
		.ok_or_else(|| Error::BadRequest("HOST header required".to_owned()))?
		.to_str()?;

	// Get bucket
	let host = authority_to_host(authority)?;
	let root = &garage.config.s3_web.root_domain;
	let bucket = host_to_bucket(&host, root).unwrap_or(&host);

	// Check bucket is exposed as a website
	let bucket_desc = garage
		.bucket_table
		.get(&EmptyKey, &bucket.to_string())
		.await?
		.filter(|b| !b.is_deleted())
		.ok_or(Error::NotFound)?;

	match bucket_desc.state.get() {
		BucketState::Present(params) if *params.website.get() => Ok(()),
		_ => Err(Error::NotFound),
	}?;

	// Get path
	let path = req.uri().path().to_string();
	let index = &garage.config.s3_web.index;
	let key = path_to_key(&path, index)?;

	info!("Selected bucket: \"{}\", selected key: \"{}\"", bucket, key);

	let res = match *req.method() {
		Method::HEAD => handle_head(garage, &req, bucket, &key).await?,
		Method::GET => handle_get(garage, &req, bucket, &key).await?,
		_ => return Err(Error::BadRequest("HTTP method not supported".to_string())),
	};

	Ok(res)
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
