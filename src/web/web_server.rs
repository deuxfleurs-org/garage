use std::sync::Arc;

use futures::future::Future;

use hyper::server::conn::AddrStream;
use hyper::{Body,Request,Response,Server};
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
				//handler(garage, req, client_addr)
				async move { Ok::<Response<Body>, Error>(Response::new(Body::from("hello world\n"))) }
			}))
		}
	});

	let server = Server::bind(&addr).serve(service);
	let graceful = server.with_graceful_shutdown(shutdown_signal);
	info!("Web server listening on http://{}", addr);

	graceful.await?;
	Ok(())
}
