use std::sync::Mutex;

use futures::prelude::*;
use futures::stream::BoxStream;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Bytes, Frame};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task;

use super::*;

use crate::signature::checksum::*;

pub struct ReqBody {
	// why need mutex to be sync??
	pub(crate) stream: Mutex<BoxStream<'static, Result<Frame<Bytes>, Error>>>,
	pub(crate) checksummer: Checksummer,
	pub(crate) expected_checksums: ExpectedChecksums,
	pub(crate) trailer_algorithm: Option<ChecksumAlgorithm>,
}

pub type StreamingChecksumReceiver = task::JoinHandle<Result<Checksums, Error>>;

impl ReqBody {
	pub fn add_expected_checksums(&mut self, more: ExpectedChecksums) {
		if more.md5.is_some() {
			self.expected_checksums.md5 = more.md5;
		}
		if more.sha256.is_some() {
			self.expected_checksums.sha256 = more.sha256;
		}
		if more.extra.is_some() {
			self.expected_checksums.extra = more.extra;
		}
		self.checksummer.add_expected(&self.expected_checksums);
	}

	pub fn add_md5(&mut self) {
		self.checksummer.add_md5();
	}

	// ============ non-streaming =============

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

	// ============ streaming =============

	pub fn streaming_with_checksums(
		self,
	) -> (
		BoxStream<'static, Result<Bytes, Error>>,
		StreamingChecksumReceiver,
	) {
		let Self {
			stream,
			mut checksummer,
			mut expected_checksums,
			trailer_algorithm,
		} = self;

		let (frame_tx, mut frame_rx) = mpsc::channel::<Frame<Bytes>>(5);

		let join_checksums = tokio::spawn(async move {
			while let Some(frame) = frame_rx.recv().await {
				match frame.into_data() {
					Ok(data) => {
						checksummer = tokio::task::spawn_blocking(move || {
							checksummer.update(&data);
							checksummer
						})
						.await
						.unwrap()
					}
					Err(frame) => {
						let trailers = frame.into_trailers().unwrap();
						let algo = trailer_algorithm.unwrap();
						expected_checksums.extra = Some(extract_checksum_value(&trailers, algo)?);
						break;
					}
				}
			}

			if trailer_algorithm.is_some() && expected_checksums.extra.is_none() {
				return Err(Error::bad_request("trailing checksum was not sent"));
			}

			let checksums = checksummer.finalize();
			checksums.verify(&expected_checksums)?;

			Ok(checksums)
		});

		let stream: BoxStream<_> = stream.into_inner().unwrap();
		let stream = stream.filter_map(move |x| {
			let frame_tx = frame_tx.clone();
			async move {
				match x {
					Err(e) => Some(Err(e)),
					Ok(frame) => {
						if frame.is_data() {
							let data = frame.data_ref().unwrap().clone();
							let _ = frame_tx.send(frame).await;
							Some(Ok(data))
						} else {
							let _ = frame_tx.send(frame).await;
							None
						}
					}
				}
			}
		});

		(stream.boxed(), join_checksums)
	}
}
