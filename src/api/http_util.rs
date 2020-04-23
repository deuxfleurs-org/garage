use core::pin::Pin;
use core::task::{Context, Poll};

use futures::ready;
use futures::stream::*;
use hyper::body::{Bytes, HttpBody};

use crate::error::Error;

type StreamType = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

pub struct StreamBody {
	stream: StreamType,
}

impl StreamBody {
	pub fn new(stream: StreamType) -> Self {
		Self { stream }
	}
}

impl HttpBody for StreamBody {
	type Data = Bytes;
	type Error = Error;

	fn poll_data(
		mut self: Pin<&mut Self>,
		cx: &mut Context,
	) -> Poll<Option<Result<Bytes, Self::Error>>> {
		match ready!(self.stream.as_mut().poll_next(cx)) {
			Some(res) => Poll::Ready(Some(res)),
			None => Poll::Ready(None),
		}
	}

	fn poll_trailers(
		self: Pin<&mut Self>,
		_cx: &mut Context,
	) -> Poll<Result<Option<hyper::HeaderMap<hyper::header::HeaderValue>>, Self::Error>> {
		Poll::Ready(Ok(None))
	}
}

pub struct BytesBody {
	bytes: Option<Bytes>,
}

impl BytesBody {
	pub fn new(bytes: Bytes) -> Self {
		Self { bytes: Some(bytes) }
	}
}

impl HttpBody for BytesBody {
	type Data = Bytes;
	type Error = Error;

	fn poll_data(
		mut self: Pin<&mut Self>,
		_cx: &mut Context,
	) -> Poll<Option<Result<Bytes, Self::Error>>> {
		Poll::Ready(self.bytes.take().map(Ok))
	}

	fn poll_trailers(
		self: Pin<&mut Self>,
		_cx: &mut Context,
	) -> Poll<Result<Option<hyper::HeaderMap<hyper::header::HeaderValue>>, Self::Error>> {
		Poll::Ready(Ok(None))
	}
}

impl From<String> for BytesBody {
	fn from(x: String) -> BytesBody {
		Self::new(Bytes::from(x))
	}
}
impl From<Vec<u8>> for BytesBody {
	fn from(x: Vec<u8>) -> BytesBody {
		Self::new(Bytes::from(x))
	}
}
