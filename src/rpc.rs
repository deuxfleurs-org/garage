use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::IntoBuf;
use hyper::service::{make_service_fn, service_fn};
use hyper::server::conn::AddrStream;
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use futures::future::Future;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;

use crate::data::*;
use crate::error::Error;
use crate::proto::Message;
use crate::membership::System;

// ---- CLIENT PART ----

pub async fn rpc_call_many(sys: Arc<System>,
						   to: &[UUID],
						   msg: &Message,
						   stop_after: Option<usize>,
						   timeout: Duration)
	-> Vec<Result<Message, Error>>
{
	let resp_stream = to.iter()
		.map(|to| rpc_call(sys.clone(), to, msg, timeout))
		.collect::<FuturesUnordered<_>>();

	collect_rpc_results(resp_stream, stop_after).await
}

pub async fn rpc_call_many_addr(sys: Arc<System>,
							    to: &[SocketAddr],
						   	    msg: &Message,
						   		stop_after: Option<usize>,
						   		timeout: Duration)
	-> Vec<Result<Message, Error>>
{
	let resp_stream = to.iter()
		.map(|to| rpc_call_addr(sys.clone(), to, msg, timeout))
		.collect::<FuturesUnordered<_>>();
	
	collect_rpc_results(resp_stream, stop_after).await
}

async fn collect_rpc_results(mut resp_stream: FuturesUnordered<impl Future<Output=Result<Message, Error>>>,
							 stop_after: Option<usize>)
	-> Vec<Result<Message, Error>>
{
	let mut results = vec![];
	let mut n_ok = 0;
	while let Some(resp) = resp_stream.next().await {
		if resp.is_ok() {
			n_ok += 1
		}
		results.push(resp);
		if let Some(n) = stop_after {
			if n_ok >= n {
				break
			}
		}
	}
	results
}

// ----

pub async fn rpc_call(sys: Arc<System>,
					  to: &UUID,
					  msg: &Message,
					  timeout: Duration)
	-> Result<Message, Error>
{
	let addr = {
		let members = sys.members.read().await;
		match members.status.get(to) {
			Some(status) => status.addr.clone(),
			None => return Err(Error::Message(format!("Peer ID not found"))),
		}
	};
	rpc_call_addr(sys, &addr, msg, timeout).await
}

pub async fn rpc_call_addr(sys: Arc<System>,
						   to_addr: &SocketAddr,
					  	   msg: &Message,
					  	   timeout: Duration)
	-> Result<Message, Error>
{
	let uri = format!("http://{}/", to_addr);
	let req = Request::builder()
		.method(Method::POST)
		.uri(uri)
		.body(Body::from(rmp_serde::encode::to_vec_named(msg)?))?;

	let resp_fut = sys.rpc_client.request(req);
	let resp = tokio::time::timeout(timeout, resp_fut).await??;

	if resp.status() == StatusCode::OK {
		let body = hyper::body::to_bytes(resp.into_body()).await?;
		let msg = rmp_serde::decode::from_read::<_, Message>(body.into_buf())?;
		match msg {
			Message::Error(e) => Err(Error::RPCError(e)),
			x => Ok(x)
		}
	} else {
		Err(Error::RPCError(format!("Status code {}", resp.status())))
	}
}

// ---- SERVER PART ----

fn err_to_msg(x: Result<Message, Error>) -> Message {
	match x {
		Err(e) => Message::Error(format!("{}", e)),
		Ok(msg) => msg,
	}
}

async fn handler(sys: Arc<System>, req: Request<Body>, addr: SocketAddr) -> Result<Response<Body>, Error> {
	if req.method() != &Method::POST {
		let mut bad_request = Response::default();
		*bad_request.status_mut() = StatusCode::BAD_REQUEST;
		return Ok(bad_request);
	}

	let whole_body = hyper::body::to_bytes(req.into_body()).await?;
	let msg = rmp_serde::decode::from_read::<_, Message>(whole_body.into_buf())?;

	eprintln!("RPC from {}: {:?}", addr, msg);

	let resp = err_to_msg(match &msg {
		Message::Ping(ping) => sys.handle_ping(&addr, ping).await,
		Message::AdvertiseNode(adv) => sys.handle_advertise_node(adv).await,
		_ => Ok(Message::Error(format!("Unexpected message: {:?}", msg))),
	});

	Ok(Response::new(Body::from(
		rmp_serde::encode::to_vec_named(&resp)?
        )))
}


pub async fn run_rpc_server(sys: Arc<System>, shutdown_signal: impl Future<Output=()>) -> Result<(), hyper::Error> {
    let bind_addr = ([0, 0, 0, 0], sys.config.rpc_port).into();

    let service = make_service_fn(|conn: &AddrStream| {
		let client_addr = conn.remote_addr();
		let sys = sys.clone();
		async move {
			Ok::<_, Error>(service_fn(move |req: Request<Body>| {
				let sys = sys.clone();
				handler(sys, req, client_addr)
			}))
		}
	});

    let server = Server::bind(&bind_addr).serve(service) ;

	let graceful = server.with_graceful_shutdown(shutdown_signal);
    println!("RPC server listening on http://{}", bind_addr);

	graceful.await
}
