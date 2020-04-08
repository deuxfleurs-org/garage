use std::sync::Arc;
use std::net::SocketAddr;
use std::collections::VecDeque;

use futures::stream::StreamExt;
use hyper::service::{make_service_fn, service_fn};
use hyper::server::conn::AddrStream;
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use futures::future::Future;

use crate::error::Error;
use crate::data::*;
use crate::proto::*;
use crate::rpc_client::*;
use crate::server::Garage;

pub async fn run_api_server(garage: Arc<Garage>, shutdown_signal: impl Future<Output=()>) -> Result<(), hyper::Error> {
    let addr = ([0, 0, 0, 0], garage.system.config.api_port).into();

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
    println!("API server listening on http://{}", addr);

	graceful.await
}

async fn handler(garage: Arc<Garage>, req: Request<Body>, addr: SocketAddr) -> Result<Response<Body>, Error> {
	match handler_inner(garage, req, addr).await {
		Ok(x) => Ok(x),
		Err(Error::BadRequest(e)) => {
			let mut bad_request = Response::new(Body::from(format!("{}\n", e)));
			*bad_request.status_mut() = StatusCode::BAD_REQUEST;
			Ok(bad_request)
		}
		Err(e) => {
			let mut ise = Response::new(Body::from(
				format!("Internal server error: {}\n", e)));
			*ise.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
			Ok(ise)
		}
	}
}

async fn handler_inner(garage: Arc<Garage>, req: Request<Body>, addr: SocketAddr) -> Result<Response<Body>, Error> {
	eprintln!("{} {} {}", addr, req.method(), req.uri());

	let bucket = req.headers()
		.get(hyper::header::HOST)
		.map(|x| x.to_str().map_err(Error::from))
		.unwrap_or(Err(Error::BadRequest(format!("Host: header missing"))))?
		.to_lowercase();
	let key = req.uri().path().to_string();

    match req.method() {
		&Method::GET => {
			Ok(Response::new(Body::from(
				"TODO: implement GET object",
			)))
		}
		&Method::PUT => {
			let mime_type = req.headers()
				.get(hyper::header::CONTENT_TYPE)
				.map(|x| x.to_str())
				.unwrap_or(Ok("blob"))?
				.to_string();
			let version_uuid = handle_put(garage, &mime_type, &bucket, &key, req.into_body()).await?;
			Ok(Response::new(Body::from(
				format!("Version UUID: {:?}", version_uuid),
			)))
		}
        _ => Err(Error::BadRequest(format!("Invalid method"))),
    }
}

async fn handle_put(garage: Arc<Garage>,
					mime_type: &str,
					bucket: &str, key: &str, body: Body)
	-> Result<UUID, Error> 
{
	let version_uuid = gen_uuid();

	let mut chunker = BodyChunker::new(body, garage.system.config.block_size);
	let first_block = match chunker.next().await? {
		Some(x) => x,
		None => return Err(Error::BadRequest(format!("Empty body"))),
	};

	let version_key = VersionMetaKey{
		bucket: bucket.to_string(),
		key: key.to_string(),
	};
	let mut version_value = VersionMetaValue {
		timestamp: now_msec(),
		uuid: version_uuid.clone(),
		mime_type: mime_type.to_string(),
		size: first_block.len() as u64,
		is_complete: false,
		data: VersionData::DeleteMarker,
	};

	if first_block.len() < INLINE_THRESHOLD {
		version_value.data = VersionData::Inline(first_block);
		version_value.is_complete = true;
		garage.version_table.insert(&version_key, &version_value).await?;
		return Ok(version_uuid)
	}

	let first_block_hash = hash(&first_block[..]);
	version_value.data = VersionData::FirstBlock(first_block_hash);
	garage.version_table.insert(&version_key, &version_value).await?;

	let block_meta = BlockMeta{
		version_uuid: version_uuid.clone(),
		offset: 0,
		hash: hash(&first_block[..]),
	};
	let mut next_offset = first_block.len();
	let mut put_curr_block = put_block(garage.clone(), block_meta, first_block);
	loop {
		let (_, next_block) = futures::try_join!(put_curr_block, chunker.next())?;
		if let Some(block) = next_block {
			let block_meta = BlockMeta{
				version_uuid: version_uuid.clone(),
				offset: next_offset as u64,
				hash: hash(&block[..]),
			};
			next_offset += block.len();
			put_curr_block = put_block(garage.clone(), block_meta, block);
		} else {
			break;
		}
	}

	// TODO: if at any step we have an error, we should undo everything we did

	version_value.is_complete = true;
	garage.version_table.insert(&version_key, &version_value).await?;
	Ok(version_uuid)
}

async fn put_block(garage: Arc<Garage>, meta: BlockMeta, data: Vec<u8>) -> Result<(), Error> {
	let who = garage.system.members.read().await
		.walk_ring(&meta.hash, garage.system.config.meta_replication_factor);
	rpc_try_call_many(garage.system.clone(),
					  &who[..],
					  &Message::PutBlock(PutBlockMessage{
						  meta,
						  data,
						}),
					  (garage.system.config.meta_replication_factor+1)/2,
					  DEFAULT_TIMEOUT).await?;
	Ok(())
}

struct BodyChunker {
	body: Body,
	block_size: usize,
	buf: VecDeque<u8>,
}

impl BodyChunker {
	fn new(body: Body, block_size: usize) -> Self {
		Self{
			body,
			block_size,
			buf: VecDeque::new(),
		}
	}
	async fn next(&mut self) -> Result<Option<Vec<u8>>, Error> {
		while self.buf.len() < self.block_size {
			if let Some(block) = self.body.next().await {
				let bytes = block?;
				self.buf.extend(&bytes[..]);
			} else {
				break;
			}
		}
		if self.buf.len() == 0 {
			Ok(None)
		} else if self.buf.len() <= self.block_size {
			let block = self.buf.drain(..)
				.collect::<Vec<u8>>();
			Ok(Some(block))
		} else {
			let block = self.buf.drain(..self.block_size)
				.collect::<Vec<u8>>();
			Ok(Some(block))
		}
	}
}
