use std::net::SocketAddr;
use std::sync::Arc;

use bytes::IntoBuf;
use futures::future::Future;
use futures_util::future::*;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use serde::Serialize;

use crate::data::rmp_to_vec_all_named;
use crate::error::Error;
use crate::proto::Message;
use crate::server::Garage;

fn debug_serialize<T: Serialize>(x: T) -> String {
	match serde_json::to_string(&x) {
		Ok(ss) => {
			if ss.len() > 100 {
				ss[..100].to_string()
			} else {
				ss
			}
		}
		Err(e) => format!("<JSON serialization error: {}>", e),
	}
}

fn err_to_msg(x: Result<Message, Error>) -> Message {
	match x {
		Err(e) => Message::Error(format!("{}", e)),
		Ok(msg) => msg,
	}
}

async fn handler(
	garage: Arc<Garage>,
	req: Request<Body>,
	addr: SocketAddr,
) -> Result<Response<Body>, Error> {
	if req.method() != &Method::POST {
		let mut bad_request = Response::default();
		*bad_request.status_mut() = StatusCode::BAD_REQUEST;
		return Ok(bad_request);
	}

	let whole_body = hyper::body::to_bytes(req.into_body()).await?;
	let msg = rmp_serde::decode::from_read::<_, Message>(whole_body.into_buf())?;

	eprintln!(
		"RPC from {}: {} ({} bytes)",
		addr,
		debug_serialize(&msg),
		whole_body.len()
	);

	let sys = garage.system.clone();
	let resp = err_to_msg(match msg {
		Message::Ping(ping) => sys.handle_ping(&addr, &ping).await,

		Message::PullStatus => sys.handle_pull_status(),
		Message::PullConfig => sys.handle_pull_config(),
		Message::AdvertiseNodesUp(adv) => sys.handle_advertise_nodes_up(&adv).await,
		Message::AdvertiseConfig(adv) => sys.handle_advertise_config(&adv).await,

		Message::PutBlock(m) => {
			// A RPC can be interrupted in the middle, however we don't want to write partial blocks,
			// which might happen if the write_block() future is cancelled in the middle.
			// To solve this, the write itself is in a spawned task that has its own separate lifetime,
			// and the request handler simply sits there waiting for the task to finish.
			// (if it's cancelled, that's not an issue)
			// (TODO FIXME except if garage happens to shut down at that point)
			let write_fut = async move {
				garage.block_manager.write_block(&m.hash, &m.data).await
			};
			tokio::spawn(write_fut).await?
		}
		Message::GetBlock(h) => garage.block_manager.read_block(&h).await,

		Message::TableRPC(table, msg) => {
			// For now, table RPCs use transactions that are not async so even if the future
			// is canceled, the db should be in a consistent state.
			if let Some(rpc_handler) = garage.table_rpc_handlers.get(&table) {
				rpc_handler
					.handle(&msg[..])
					.await
					.map(|rep| Message::TableRPC(table.to_string(), rep))
			} else {
				Ok(Message::Error(format!("Unknown table: {}", table)))
			}
		}

		_ => Ok(Message::Error(format!("Unexpected message: {:?}", msg))),
	});

	eprintln!("reply to {}: {}", addr, debug_serialize(&resp));

	Ok(Response::new(Body::from(rmp_to_vec_all_named(&resp)?)))
}

pub async fn run_rpc_server(
	garage: Arc<Garage>,
	shutdown_signal: impl Future<Output = ()>,
) -> Result<(), Error> {
	let bind_addr = ([0, 0, 0, 0, 0, 0, 0, 0], garage.system.config.rpc_port).into();

	let service = make_service_fn(|conn: &AddrStream| {
		let client_addr = conn.remote_addr();
		let garage = garage.clone();
		async move {
			Ok::<_, Error>(service_fn(move |req: Request<Body>| {
				let garage = garage.clone();
				handler(garage, req, client_addr).map_err(|e| {
					eprintln!("RPC handler error: {}", e);
					e
				})
			}))
		}
	});

	let server = Server::bind(&bind_addr).serve(service);

	let graceful = server.with_graceful_shutdown(shutdown_signal);
	println!("RPC server listening on http://{}", bind_addr);

	graceful.await?;
	Ok(())
}
