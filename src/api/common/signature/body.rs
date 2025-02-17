use std::sync::Mutex;

use futures::prelude::*;
use futures::stream::BoxStream;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Bytes, Frame};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};

use super::*;

use crate::signature::checksum::*;

pub struct ReqBody {
	// why need mutex to be sync??
	pub stream: Mutex<BoxStream<'static, Result<Frame<Bytes>, Error>>>,
	pub checksummer: Checksummer,
	pub expected_checksums: ExpectedChecksums,
}

pub type StreamingChecksumReceiver = oneshot::Receiver<Result<Checksums, Error>>;

impl ReqBody {
	pub async fn json<T: for<'a> Deserialize<'a>>(self) -> Result<T, Error> {
		let body = self.collect().await?;
		let resp: T = serde_json::from_slice(&body).ok_or_bad_request("Invalid JSON")?;
		Ok(resp)
	}

	pub async fn collect(self) -> Result<Bytes, Error> {
		self.collect_with_checksums().await.map(|(b, _)| b)
	}

	pub async fn collect_with_checksums(mut self) -> Result<(Bytes, Checksums), Error> {
		let stream: BoxStream<_> = self.stream.into_inner().unwrap();
		let bytes = BodyExt::collect(StreamBody::new(stream)).await?.to_bytes();

		self.checksummer.update(&bytes);
		let checksums = self.checksummer.finalize();
		checksums.verify(&self.expected_checksums)?;

		Ok((bytes, checksums))
	}

	pub fn streaming(self) -> impl Stream<Item = Result<Bytes, Error>> {
		self.streaming_with_checksums(false).0
	}

	pub fn streaming_with_checksums(
		self,
		add_md5: bool,
	) -> (
		impl Stream<Item = Result<Bytes, Error>>,
		StreamingChecksumReceiver,
	) {
		let (tx, rx) = oneshot::channel();
		// TODO: actually calculate checksums!!
		let stream: BoxStream<_> = self.stream.into_inner().unwrap();
		(
			stream.map(|x| {
				x.and_then(|f| {
					f.into_data()
						.map_err(|_| Error::bad_request("non-data frame"))
				})
			}),
			rx,
		)
	}
}
