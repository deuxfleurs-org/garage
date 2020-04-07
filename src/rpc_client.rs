use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::IntoBuf;
use hyper::{Body, Method, Request, StatusCode};
use hyper::client::Client;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;

use crate::data::*;
use crate::error::Error;
use crate::proto::Message;
use crate::membership::System;

pub async fn rpc_call_many(sys: Arc<System>,
						   to: &[UUID],
						   msg: &Message,
						   stop_after: Option<usize>,
						   timeout: Duration)
	-> Vec<Result<Message, Error>>
{
	let mut resp_stream = to.iter()
		.map(|to| rpc_call(sys.clone(), to, msg, timeout))
		.collect::<FuturesUnordered<_>>();

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
	sys.rpc_client.call(&addr, msg, timeout).await
}

pub struct RpcClient {
	pub client: Client<hyper::client::HttpConnector, hyper::Body>,
}

impl RpcClient {
	pub fn new() -> Self {
		RpcClient{
			client: Client::new(),
		}
	}

	pub async fn call(&self,
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

		let resp_fut = self.client.request(req);
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
}
