use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::Future;
use hyper::body::{Bytes, HttpBody};
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server};

use garage_util::error::Error;

use garage_core::garage::Garage;

use crate::http_util::*;
use crate::signature::check_signature;

use crate::s3_get::{handle_get, handle_head};
use crate::s3_list::handle_list;
use crate::s3_put::{handle_delete, handle_put};

pub type BodyType = Box<dyn HttpBody<Data = Bytes, Error = Error> + Send + Unpin>;

pub async fn run_api_server(
	garage: Arc<Garage>,
	shutdown_signal: impl Future<Output = ()>,
) -> Result<(), Error> {
	let addr = &garage.config.s3_api.api_bind_addr;

	let service = make_service_fn(|conn: &AddrStream| {
		let garage = garage.clone();
		let client_addr = conn.remote_addr();
		async move {
			Ok::<_, Error>(service_fn(move |req: Request<Body>| {
				let garage = garage.clone();
				handler(garage, req, client_addr)
			}))
		}
	});

	let server = Server::bind(&addr).serve(service);

	let graceful = server.with_graceful_shutdown(shutdown_signal);
	info!("API server listening on http://{}", addr);

	graceful.await?;
	Ok(())
}

async fn handler(
	garage: Arc<Garage>,
	req: Request<Body>,
	addr: SocketAddr,
) -> Result<Response<BodyType>, Error> {
	info!("{} {} {}", addr, req.method(), req.uri());
	debug!("{:?}", req);
	match handler_inner(garage, req).await {
		Ok(x) => {
			debug!("{} {:?}", x.status(), x.headers());
			Ok(x)
		}
		Err(e) => {
			let body: BodyType = Box::new(BytesBody::from(format!("{}\n", e)));
			let mut http_error = Response::new(body);
			*http_error.status_mut() = e.http_status_code();
			warn!("Response: error {}, {}", e.http_status_code(), e);
			Ok(http_error)
		}
	}
}

async fn handler_inner(
	garage: Arc<Garage>,
	req: Request<Body>,
) -> Result<Response<BodyType>, Error> {
	let path = req.uri().path().to_string();
	let path = path.trim_start_matches('/');
	let (bucket, key) = match path.find('/') {
		Some(i) => {
			let (bucket, key) = path.split_at(i);
			let key = key.trim_start_matches('/');
			(bucket, Some(key))
		}
		None => (path, None),
	};
	if bucket.len() == 0 {
		return Err(Error::Forbidden(format!(
			"Operations on buckets not allowed"
		)));
	}

	let api_key = check_signature(&garage, &req).await?;
	let allowed = match req.method() {
		&Method::HEAD | &Method::GET => api_key.allow_read(&bucket),
		_ => api_key.allow_write(&bucket),
	};
	if !allowed {
		return Err(Error::Forbidden(format!(
			"Operation is not allowed for this key."
		)));
	}

	if let Some(key) = key {
		match req.method() {
			&Method::HEAD => Ok(handle_head(garage, &bucket, &key).await?),
			&Method::GET => Ok(handle_get(garage, &bucket, &key).await?),
			&Method::PUT => {
				let mime_type = req
					.headers()
					.get(hyper::header::CONTENT_TYPE)
					.map(|x| x.to_str())
					.unwrap_or(Ok("blob"))?
					.to_string();
				let version_uuid =
					handle_put(garage, &mime_type, &bucket, &key, req.into_body()).await?;
				let response = format!("{}\n", hex::encode(version_uuid,));
				Ok(Response::new(Box::new(BytesBody::from(response))))
			}
			&Method::DELETE => {
				let version_uuid = handle_delete(garage, &bucket, &key).await?;
				let response = format!("{}\n", hex::encode(version_uuid,));
				Ok(Response::new(Box::new(BytesBody::from(response))))
			}
			_ => Err(Error::BadRequest(format!("Invalid method"))),
		}
	} else {
		match req.method() {
			&Method::PUT | &Method::HEAD => {
				// If PUT: corresponds to a bucket creation call
				// If we're here, the bucket already exists, so just answer ok
				let empty_body: BodyType = Box::new(BytesBody::from(vec![]));
				let response = Response::builder()
					.header("Location", format!("/{}", bucket))
					.body(empty_body)
					.unwrap();
				Ok(response)
			}
			&Method::DELETE => Err(Error::Forbidden(
				"Cannot delete buckets using S3 api, please talk to Garage directly".into(),
			)),
			&Method::GET => {
				let mut params = HashMap::new();
				if let Some(query) = req.uri().query() {
					let query_pairs = url::form_urlencoded::parse(query.as_bytes());
					for (key, val) in query_pairs {
						params.insert(key.to_lowercase(), val.to_string());
					}
				}
				if ["delimiter", "prefix"]
					.iter()
					.all(|x| params.contains_key(&x.to_string()))
				{
					let delimiter = params.get("delimiter").unwrap();
					let max_keys = params
						.get("max-keys")
						.map(|x| {
							x.parse::<usize>().map_err(|e| {
								Error::BadRequest(format!("Invalid value for max-keys: {}", e))
							})
						})
						.unwrap_or(Ok(1000))?;
					let prefix = params.get("prefix").unwrap();
					Ok(handle_list(garage, bucket, delimiter, max_keys, prefix).await?)
				} else {
					Err(Error::BadRequest(format!(
						"Not a list call, so what is it?"
					)))
				}
			}
			_ => Err(Error::BadRequest(format!("Invalid method"))),
		}
	}
}
