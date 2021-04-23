use std::{borrow::Cow, convert::Infallible, net::SocketAddr, sync::Arc};

use futures::future::Future;

use hyper::{
	header::HOST,
	server::conn::AddrStream,
	service::{make_service_fn, service_fn},
	Body, Method, Request, Response, Server,
};

use idna::domain_to_unicode;

use crate::error::*;
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

	let server = Server::bind(&addr).serve(service);
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
	let (host, _) = domain_to_unicode(authority_to_host(authority)?);
	let root = &garage.config.s3_web.root_domain;
	let bucket = host_to_bucket(&host, root);

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
	let key = path_to_key(&path, &index)?;

	info!("Selected bucket: \"{}\", selected key: \"{}\"", bucket, key);

	let res = match *req.method() {
		Method::HEAD => handle_head(garage, &req, &bucket, &key).await?,
		Method::GET => handle_get(garage, &req, bucket, &key).await?,
		_ => return Err(Error::BadRequest("HTTP method not supported".to_string())),
	};

	Ok(res)
}

/// Extract host from the authority section given by the HTTP host header
///
/// The HTTP host contains both a host and a port.
/// Extracting the port is more complex than just finding the colon (:) symbol due to IPv6
/// We do not use the collect pattern as there is no way in std rust to collect over a stack allocated value
/// check here: https://docs.rs/collect_slice/1.2.0/collect_slice/
fn authority_to_host(authority: &str) -> Result<&str, Error> {
	let mut iter = authority.chars().enumerate();
	let (_, first_char) = iter
		.next()
		.ok_or_else(|| Error::BadRequest("Authority is empty".to_string()))?;

	let split = match first_char {
		'[' => {
			let mut iter = iter.skip_while(|(_, c)| c != &']');
			match iter.next() {
				Some((_, ']')) => iter.next(),
				_ => {
					return Err(Error::BadRequest(format!(
						"Authority {} has an illegal format",
						authority
					)))
				}
			}
		}
		_ => iter.find(|(_, c)| *c == ':'),
	};

	match split {
		Some((i, ':')) => Ok(&authority[..i]),
		None => Ok(authority),
		Some((_, _)) => Err(Error::BadRequest(format!(
			"Authority {} has an illegal format",
			authority
		))),
	}
}

/// Host to bucket
///
/// Convert a host, like "bucket.garage-site.tld" or "john.doe.com"
/// to the corresponding bucket, resp. "bucket" and "john.doe.com"
/// considering that ".garage-site.tld" is the "root domain".
/// This behavior has been chosen to follow AWS S3 semantic.
fn host_to_bucket<'a>(host: &'a str, root: &str) -> &'a str {
	if root.len() >= host.len() || !host.ends_with(root) {
		return host;
	}

	let len_diff = host.len() - root.len();
	let missing_starting_dot = !root.starts_with('.');
	let cursor = if missing_starting_dot {
		len_diff - 1
	} else {
		len_diff
	};
	&host[..cursor]
}

/// Path to key
///
/// Convert the provided path to the internal key
/// When a path ends with "/", we append the index name to match traditional web server behavior
/// which is also AWS S3 behavior.
fn path_to_key<'a>(path: &'a str, index: &str) -> Result<Cow<'a, str>, Error> {
	let path_utf8 = percent_encoding::percent_decode_str(&path).decode_utf8()?;

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
	fn authority_to_host_with_port() -> Result<(), Error> {
		let domain = authority_to_host("[::1]:3902")?;
		assert_eq!(domain, "[::1]");
		let domain2 = authority_to_host("garage.tld:65200")?;
		assert_eq!(domain2, "garage.tld");
		let domain3 = authority_to_host("127.0.0.1:80")?;
		assert_eq!(domain3, "127.0.0.1");
		Ok(())
	}

	#[test]
	fn authority_to_host_without_port() -> Result<(), Error> {
		let domain = authority_to_host("[::1]")?;
		assert_eq!(domain, "[::1]");
		let domain2 = authority_to_host("garage.tld")?;
		assert_eq!(domain2, "garage.tld");
		let domain3 = authority_to_host("127.0.0.1")?;
		assert_eq!(domain3, "127.0.0.1");
		assert!(authority_to_host("[").is_err());
		assert!(authority_to_host("[hello").is_err());
		Ok(())
	}

	#[test]
	fn host_to_bucket_test() {
		assert_eq!(
			host_to_bucket("john.doe.garage.tld", ".garage.tld"),
			"john.doe"
		);

		assert_eq!(
			host_to_bucket("john.doe.garage.tld", "garage.tld"),
			"john.doe"
		);

		assert_eq!(host_to_bucket("john.doe.com", "garage.tld"), "john.doe.com");

		assert_eq!(
			host_to_bucket("john.doe.com", ".garage.tld"),
			"john.doe.com"
		);

		assert_eq!(host_to_bucket("garage.tld", "garage.tld"), "garage.tld");

		assert_eq!(host_to_bucket("garage.tld", ".garage.tld"), "garage.tld");
	}

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
