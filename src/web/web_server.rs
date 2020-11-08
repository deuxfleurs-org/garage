use std::sync::Arc;
use std::net::SocketAddr;

use futures::future::Future;

use hyper::server::conn::AddrStream;
use hyper::{Body,Request,Response,Server,Uri};
use hyper::header::HOST;
use hyper::service::{make_service_fn, service_fn};

use garage_util::error::Error;
use garage_model::garage::Garage;

pub async fn run_web_server(
	garage: Arc<Garage>,
	shutdown_signal: impl Future<Output = ()>,
) -> Result<(), Error> {
	let addr = &garage.config.s3_web.web_bind_addr;

	let service = make_service_fn(|conn: &AddrStream| {
		let garage = garage.clone();
		let client_addr = conn.remote_addr();
		info!("{:?}", client_addr);
	  async move {
			Ok::<_, Error>(service_fn(move |req: Request<Body>| {
				let garage = garage.clone();
				handler(garage, req, client_addr)
			}))
		}
	});

	let server = Server::bind(&addr).serve(service);
	let graceful = server.with_graceful_shutdown(shutdown_signal);
	info!("Web server listening on http://{}", addr);

	graceful.await?;
	Ok(())
}

async fn handler(
	garage: Arc<Garage>,
	req: Request<Body>,
	addr: SocketAddr,
) -> Result<Response<Body>, Error> {

	// Get http authority string (eg. [::1]:3902)
	let authority = req
		.headers()
		.get(HOST)
		.ok_or(Error::BadRequest(format!("HOST header required")))?
		.to_str()?;
	info!("authority is {}", authority);

	// Get HTTP domain/ip from host
	//let domain = host.to_socket_


	Ok(Response::new(Body::from("hello world\n")))
}

fn authority_to_host(authority: &str) -> Result<String, Error> {
	let mut uri_str: String = "fake://".to_owned();
	uri_str.push_str(authority);

	match uri_str.parse::<Uri>() {
		Ok(uri) => {
			let host = uri
				.host()
				.ok_or(Error::BadRequest(format!("Unable to extract host from authority as string")))?;
			Ok(String::from(host))
		}
		_ => Err(Error::BadRequest(format!("Unable to parse authority (host HTTP header)"))),
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
}
