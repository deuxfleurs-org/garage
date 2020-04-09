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
		Err(e) => {
			let mut http_error = Response::new(Body::from(format!("{}\n", e)));
			*http_error.status_mut() = e.http_status_code();
			Ok(http_error)
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
			Ok(handle_get(garage, &bucket, &key).await?)
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

	let mut object = Object {
		bucket: bucket.into(),
		key: key.into(),
		versions: Vec::new(),
	};
	object.versions.push(Box::new(Version{
		uuid: version_uuid.clone(),
		timestamp: now_msec(),
		mime_type: mime_type.to_string(),
		size: first_block.len() as u64,
		is_complete: false,
		data: VersionData::DeleteMarker,
	}));

	if first_block.len() < INLINE_THRESHOLD {
		object.versions[0].data = VersionData::Inline(first_block);
		object.versions[0].is_complete = true;
		garage.object_table.insert(&object).await?;
		return Ok(version_uuid)
	}

	let first_block_hash = hash(&first_block[..]);
	object.versions[0].data = VersionData::FirstBlock(first_block_hash);
	garage.object_table.insert(&object).await?;

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

	object.versions[0].is_complete = true;
	object.versions[0].size = next_offset as u64;
	garage.object_table.insert(&object).await?;
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

async fn handle_get(garage: Arc<Garage>, bucket: &str, key: &str) -> Result<Response<Body>, Error> {
	let mut object = match garage.object_table.get(&bucket.to_string(), &key.to_string()).await? {
		None => return Err(Error::NotFound),
		Some(o) => o
	};

	let last_v = match object.versions.drain(..)
		.rev().filter(|v| v.is_complete)
		.next() {
		Some(v) => v,
		None => return Err(Error::NotFound),
	};

	let resp_builder = Response::builder()
		.header("Content-Type", last_v.mime_type)
		.status(StatusCode::OK);

	match last_v.data {
		VersionData::DeleteMarker => Err(Error::NotFound),
		VersionData::Inline(bytes) => {
			Ok(resp_builder.body(bytes.into())?)
		}
		VersionData::FirstBlock(hash) => {
			// TODO
			unimplemented!()
		}
	}
}
