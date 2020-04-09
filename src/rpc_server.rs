use std::net::SocketAddr;
use std::sync::Arc;

use serde::Serialize;
use bytes::IntoBuf;
use hyper::service::{make_service_fn, service_fn};
use hyper::server::conn::AddrStream;
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use futures::future::Future;

use crate::error::Error;
use crate::data::rmp_to_vec_all_named;
use crate::proto::Message;
use crate::server::Garage;
use crate::block::*;

fn debug_serialize<T: Serialize>(x: T) -> Result<String, Error> {
	let ss = serde_json::to_string(&x)?;
	if ss.len() > 100 {
		Ok(ss[..100].to_string())
	} else {
		Ok(ss)
	}
}

fn err_to_msg(x: Result<Message, Error>) -> Message {
	match x {
		Err(e) => Message::Error(format!("{}", e)),
		Ok(msg) => msg,
	}
}

async fn handler(garage: Arc<Garage>, req: Request<Body>, addr: SocketAddr) -> Result<Response<Body>, Error> {
	if req.method() != &Method::POST {
		let mut bad_request = Response::default();
		*bad_request.status_mut() = StatusCode::BAD_REQUEST;
		return Ok(bad_request);
	}

	let whole_body = hyper::body::to_bytes(req.into_body()).await?;
	let msg = rmp_serde::decode::from_read::<_, Message>(whole_body.into_buf())?;

	eprintln!("RPC from {}: {} ({} bytes)", addr, debug_serialize(&msg)?, whole_body.len());

	let sys = garage.system.clone();
	let resp = err_to_msg(match &msg {
		Message::Ping(ping) => sys.handle_ping(&addr, ping).await,
		Message::PullStatus => sys.handle_pull_status().await,
		Message::PullConfig => sys.handle_pull_config().await,
		Message::AdvertiseNodesUp(adv) => sys.handle_advertise_nodes_up(adv).await,
		Message::AdvertiseConfig(adv) => sys.handle_advertise_config(adv).await,
		Message::PutBlock(m) => {
			write_block(garage, &m.hash, &m.data).await
		}
		Message::GetBlock(h) => {
			read_block(garage, &h).await
		}
		Message::TableRPC(table, msg) => {
			if let Some(rpc_handler) = garage.table_rpc_handlers.get(table) {
				rpc_handler.handle(&msg[..]).await
					.map(|rep| Message::TableRPC(table.to_string(), rep))
			} else {
				Ok(Message::Error(format!("Unknown table: {}", table)))
			}
		}

		_ => Ok(Message::Error(format!("Unexpected message: {:?}", msg))),
	});

	eprintln!("reply to {}: {}", addr, debug_serialize(&resp)?);

	Ok(Response::new(Body::from(
		rmp_to_vec_all_named(&resp)?
        )))
}


pub async fn run_rpc_server(garage: Arc<Garage>, shutdown_signal: impl Future<Output=()>) -> Result<(), hyper::Error> {
    let bind_addr = ([0, 0, 0, 0], garage.system.config.rpc_port).into();

    let service = make_service_fn(|conn: &AddrStream| {
		let client_addr = conn.remote_addr();
		let garage = garage.clone();
		async move {
			Ok::<_, Error>(service_fn(move |req: Request<Body>| {
				let garage = garage.clone();
				handler(garage, req, client_addr)
			}))
		}
	});

    let server = Server::bind(&bind_addr).serve(service) ;

	let graceful = server.with_graceful_shutdown(shutdown_signal);
    println!("RPC server listening on http://{}", bind_addr);

	graceful.await
}
