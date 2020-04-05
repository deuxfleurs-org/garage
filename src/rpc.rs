use bytes::IntoBuf;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use futures::future::Future;

use crate::error::Error;
use crate::proto::Message;
use crate::System;


/// This is our service handler. It receives a Request, routes on its
/// path, and returns a Future of a Response.
async fn echo(req: Request<Body>) -> Result<Response<Body>, Error> {
	if req.method() != &Method::POST {
		let mut bad_request = Response::default();
		*bad_request.status_mut() = StatusCode::BAD_REQUEST;
		return Ok(bad_request);
	}
          

	let whole_body = hyper::body::to_bytes(req.into_body()).await?;
	let msg = rmp_serde::decode::from_read::<_, Message>(whole_body.into_buf());

	let resp = Message::Ok;
	Ok(Response::new(Body::from(
		rmp_serde::encode::to_vec_named(&resp)?
        )))
}


pub async fn run_rpc_server(sys: &System, rpc_port: u16, shutdown_signal: impl Future<Output=()>) -> Result<(), hyper::Error> {
    let addr = ([0, 0, 0, 0], rpc_port).into();

    let service = make_service_fn(|_| async { Ok::<_, Error>(service_fn(echo)) });

    let server = Server::bind(&addr).serve(service);

	let graceful = server.with_graceful_shutdown(shutdown_signal);
    println!("Listening on http://{}", addr);

	graceful.await
}
