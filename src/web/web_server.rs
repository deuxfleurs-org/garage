use std::borrow::Cow;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use futures::future::Future;
use futures::stream::*;

use hyper::{
	header::HOST,
	body::Bytes,
	server::conn::AddrStream,
	service::{make_service_fn, service_fn},
	Body, Request, Response, Server, StatusCode};

use idna::domain_to_unicode;

use garage_model::garage::Garage;
use garage_model::object_table::*;
use garage_table::EmptyKey;
use garage_util::error::Error as GarageError;
use crate::error::*;

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
		.ok_or(Error::BadRequest(format!("HOST header required")))?
		.to_str()?;

	// Get bucket
	let (host, _) = domain_to_unicode(authority_to_host(authority)?);
	let root = &garage.config.s3_web.root_domain;
	let bucket = host_to_bucket(&host, root);

	// Get path
	let path = req.uri().path().to_string();
	let index = &garage.config.s3_web.index;
	let key = path_to_key(&path, &index)?;

	info!("Selected bucket: \"{}\", selected key: \"{}\"", bucket, key);

	// Get bucket descriptor
	let object = garage
		.object_table
		.get(&bucket.to_string(), &key.to_string())
		.await?
		.ok_or(Error::NotFound)?;

	// Get last complete version descriptor
	let last_v = object
		.versions()
		.iter()
		.rev()
		.filter(|v| v.is_complete())
		.next()
		.ok_or(Error::NotFound)?;

	// Unwrap version
	let last_v_data = match &last_v.state {
		ObjectVersionState::Complete(x) => x,
		_ => unreachable!(),
	};

	// Get metadata from version
	let last_v_meta = match last_v_data {
		ObjectVersionData::DeleteMarker => return Err(Error::NotFound),
		ObjectVersionData::Inline(meta, _) => meta,
		ObjectVersionData::FirstBlock(meta, _) => meta,
	};

	// @FIXME Support range
	

	// Set headers
	let resp_builder = object_headers(&last_v, last_v_meta).status(StatusCode::OK);


	// Stream body
	match &last_v_data {
		ObjectVersionData::DeleteMarker => unreachable!(),
		ObjectVersionData::Inline(_, bytes) => {
			let body: Body = Body::from(bytes.to_vec());
			Ok(resp_builder.body(body)?)
		}
		ObjectVersionData::FirstBlock(_, first_block_hash) => {
			let read_first_block = garage.block_manager.rpc_get_block(&first_block_hash);
			let get_next_blocks = garage.version_table.get(&last_v.uuid, &EmptyKey);

			let (first_block, version) = futures::try_join!(read_first_block, get_next_blocks)?;
			let version = version.ok_or(Error::NotFound)?;

			let mut blocks = version
				.blocks()
				.iter()
				.map(|vb| (vb.hash, None))
				.collect::<Vec<_>>();
			blocks[0].1 = Some(first_block);

			let body_stream = futures::stream::iter(blocks)
				.map(move |(hash, data_opt)| {
					let garage = garage.clone();
					async move {
						if let Some(data) = data_opt {
							Ok(Bytes::from(data))
						} else {
							garage
								.block_manager
								.rpc_get_block(&hash)
								.await
								.map(Bytes::from)
						}
					}
				})
				.buffered(2);
			//let body: Body = Box::new(StreamBody::new(Box::pin(body_stream)));
			let body = hyper::body::Body::wrap_stream(body_stream);
			Ok(resp_builder.body(body)?)
		}
	}
}

// Copied from api/s3_get.rs
fn object_headers(
	version: &ObjectVersion,
	version_meta: &ObjectVersionMeta,
) -> http::response::Builder {
	let date = UNIX_EPOCH + Duration::from_millis(version.timestamp);
	let date_str = httpdate::fmt_http_date(date);

	let mut resp = Response::builder()
		.header(
			"Content-Type",
			version_meta.headers.content_type.to_string(),
		)
		.header("Content-Length", format!("{}", version_meta.size))
		.header("ETag", version_meta.etag.to_string())
		.header("Last-Modified", date_str)
		.header("Accept-Ranges", format!("bytes"));

	for (k, v) in version_meta.headers.other.iter() {
		resp = resp.header(k, v.to_string());
	}

	resp
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
		.ok_or(Error::BadRequest(format!("Authority is empty")))?;

	let split = match first_char {
		'[' => {
			let mut iter = iter.skip_while(|(_, c)| c != &']');
			iter.next().expect("Authority parsing logic error");
			iter.next()
		}
		_ => iter.skip_while(|(_, c)| c != &':').next(),
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
	let missing_starting_dot = root.chars().next() != Some('.');
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
	
	if path_utf8.chars().next() != Some('/') {
		return Err(Error::BadRequest(format!(
			"Path must start with a / (slash)"
		)))
	}

	match path_utf8.chars().last() {
		None => Err(Error::BadRequest(format!(
			"Path must have at least a character"
		))),
		Some('/') => {
			let mut key = String::with_capacity(path_utf8.len() + index.len());
			key.push_str(&path_utf8[1..]);
			key.push_str(index);
			Ok(key.into())
		}
		Some(_) => {
			match path_utf8 {
				Cow::Borrowed(pu8) => Ok((&pu8[1..]).into()),
				Cow::Owned(pu8) => Ok((&pu8[1..]).to_string().into()),
			}
		}
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
