use std::net::SocketAddr;
use std::sync::Arc;

use bytes::IntoBuf;
use futures::future::Future;
use futures_util::future::*;
use futures_util::stream::*;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

use crate::data::{rmp_to_vec_all_named, debug_serialize};
use crate::error::Error;
use crate::proto::Message;
use crate::server::Garage;
use crate::tls_util;


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

	// eprintln!(
	// 	"RPC from {}: {} ({} bytes)",
	// 	addr,
	// 	debug_serialize(&msg),
	// 	whole_body.len()
	// );

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
			let write_fut = async move { garage.block_manager.write_block(&m.hash, &m.data).await };
			tokio::spawn(write_fut).await?
		}
		Message::GetBlock(h) => garage.block_manager.read_block(&h).await,

		Message::TableRPC(table, msg) => {
			// Same trick for table RPCs than for PutBlock
			let op_fut = async move {
				if let Some(rpc_handler) = garage.table_rpc_handlers.get(&table) {
					rpc_handler
						.handle(&msg[..])
						.await
						.map(|rep| Message::TableRPC(table.to_string(), rep))
				} else {
					Ok(Message::Error(format!("Unknown table: {}", table)))
				}
			};
			tokio::spawn(op_fut).await?
		}

		_ => Ok(Message::Error(format!("Unexpected message: {:?}", msg))),
	});

	// eprintln!("reply to {}: {}", addr, debug_serialize(&resp));

	Ok(Response::new(Body::from(rmp_to_vec_all_named(&resp)?)))
}

pub async fn run_rpc_server(
	garage: Arc<Garage>,
	shutdown_signal: impl Future<Output = ()>,
) -> Result<(), Error> {
	let bind_addr = ([0, 0, 0, 0, 0, 0, 0, 0], garage.system.config.rpc_port).into();

	if let Some(tls_config) = &garage.system.config.rpc_tls {
		let ca_certs = tls_util::load_certs(&tls_config.ca_cert)?;
		let node_certs = tls_util::load_certs(&tls_config.node_cert)?;
		let node_key = tls_util::load_private_key(&tls_config.node_key)?;

		let mut ca_store = rustls::RootCertStore::empty();
		for crt in ca_certs.iter() {
			ca_store.add(crt)?;
		}

		let mut config =
			rustls::ServerConfig::new(rustls::AllowAnyAuthenticatedClient::new(ca_store));
		config.set_single_cert([&node_certs[..], &ca_certs[..]].concat(), node_key)?;
		let tls_acceptor = Arc::new(TlsAcceptor::from(Arc::new(config)));

		let mut listener = TcpListener::bind(&bind_addr).await?;
		let incoming = listener.incoming().filter_map(|socket| async {
			match socket {
				Ok(stream) => match tls_acceptor.clone().accept(stream).await {
					Ok(x) => Some(Ok::<_, hyper::Error>(x)),
					Err(e) => {
						eprintln!("RPC server TLS error: {}", e);
						None
					}
				},
				Err(_) => None,
			}
		});
		let incoming = hyper::server::accept::from_stream(incoming);

		let service = make_service_fn(|conn: &TlsStream<TcpStream>| {
			let client_addr = conn
				.get_ref()
				.0
				.peer_addr()
				.unwrap_or(([0, 0, 0, 0], 0).into());
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

		let server = Server::builder(incoming).serve(service);

		let graceful = server.with_graceful_shutdown(shutdown_signal);
		println!("RPC server listening on http://{}", bind_addr);

		graceful.await?;
	} else {
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
	}

	Ok(())
}
