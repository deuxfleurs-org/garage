use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;

use futures::Future;
use futures::{Stream, StreamExt};
use tokio::io::AsyncRead;

use crate::bytes_buf::BytesBuf;

/// A stream of bytes (click to read more).
///
/// When sent through Netapp, the Vec may be split in smaller chunk in such a way
/// consecutive Vec may get merged, but Vec and error code may not be reordered
///
/// Items sent in the ByteStream may be errors of type `std::io::Error`.
/// An error indicates the end of the ByteStream: a reader should no longer read
/// after receiving an error, and a writer should stop writing after sending an error.
pub type ByteStream = Pin<Box<dyn Stream<Item = Packet> + Send + Sync>>;

/// A packet sent in a ByteStream, which may contain either
/// a Bytes object or an error
pub type Packet = Result<Bytes, std::io::Error>;

// ----

/// A helper struct to read defined lengths of data from a BytesStream
pub struct ByteStreamReader {
	stream: ByteStream,
	buf: BytesBuf,
	eos: bool,
	err: Option<std::io::Error>,
}

impl ByteStreamReader {
	/// Creates a new `ByteStreamReader` from a `ByteStream`
	pub fn new(stream: ByteStream) -> Self {
		ByteStreamReader {
			stream,
			buf: BytesBuf::new(),
			eos: false,
			err: None,
		}
	}

	/// Read exactly `read_len` bytes from the underlying stream
	/// (returns a future)
	pub fn read_exact(&mut self, read_len: usize) -> ByteStreamReadExact<'_> {
		ByteStreamReadExact {
			reader: self,
			read_len,
			fail_on_eos: true,
		}
	}

	/// Read at most `read_len` bytes from the underlying stream, or less
	/// if the end of the stream is reached (returns a future)
	pub fn read_exact_or_eos(&mut self, read_len: usize) -> ByteStreamReadExact<'_> {
		ByteStreamReadExact {
			reader: self,
			read_len,
			fail_on_eos: false,
		}
	}

	/// Read exactly one byte from the underlying stream and returns it
	/// as an u8
	pub async fn read_u8(&mut self) -> Result<u8, ReadExactError> {
		Ok(self.read_exact(1).await?[0])
	}

	/// Read exactly two bytes from the underlying stream and returns them as an u16 (using
	/// big-endian decoding)
	pub async fn read_u16(&mut self) -> Result<u16, ReadExactError> {
		let bytes = self.read_exact(2).await?;
		let mut b = [0u8; 2];
		b.copy_from_slice(&bytes[..]);
		Ok(u16::from_be_bytes(b))
	}

	/// Read exactly four bytes from the underlying stream and returns them as an u32 (using
	/// big-endian decoding)
	pub async fn read_u32(&mut self) -> Result<u32, ReadExactError> {
		let bytes = self.read_exact(4).await?;
		let mut b = [0u8; 4];
		b.copy_from_slice(&bytes[..]);
		Ok(u32::from_be_bytes(b))
	}

	/// Transforms the stream reader back into the underlying stream (starting
	/// after everything that the reader has read)
	pub fn into_stream(self) -> ByteStream {
		let buf_stream = futures::stream::iter(self.buf.into_slices().into_iter().map(Ok));
		if let Some(err) = self.err {
			Box::pin(buf_stream.chain(futures::stream::once(async move { Err(err) })))
		} else if self.eos {
			Box::pin(buf_stream)
		} else {
			Box::pin(buf_stream.chain(self.stream))
		}
	}

	/// Tries to fill the internal read buffer from the underlying stream if it is empty.
	/// Calling this might be necessary to ensure that `.eos()` returns a correct
	/// result, otherwise the reader might not be aware that the underlying
	/// stream has nothing left to return.
	pub async fn fill_buffer(&mut self) {
		if self.buf.is_empty() {
			let packet = self.stream.next().await;
			self.add_stream_next(packet);
		}
	}

	/// Clears the internal read buffer and returns its content
	pub fn take_buffer(&mut self) -> Bytes {
		self.buf.take_all()
	}

	/// Returns true if the end of the underlying stream has been reached
	pub fn eos(&self) -> bool {
		self.buf.is_empty() && self.eos
	}

	fn try_get(&mut self, read_len: usize) -> Option<Bytes> {
		self.buf.take_exact(read_len)
	}

	fn add_stream_next(&mut self, packet: Option<Packet>) {
		match packet {
			Some(Ok(slice)) => {
				self.buf.extend(slice);
			}
			Some(Err(e)) => {
				self.err = Some(e);
				self.eos = true;
			}
			None => {
				self.eos = true;
			}
		}
	}
}

/// The error kind that can be returned by `ByteStreamReader::read_exact` and
/// `ByteStreamReader::read_exact_or_eos`
pub enum ReadExactError {
	/// The end of the stream was reached before the requested number of bytes could be read
	UnexpectedEos,
	/// The underlying data stream returned an IO error when trying to read
	Stream(std::io::Error),
}

/// The future returned by `ByteStreamReader::read_exact` and
/// `ByteStreamReader::read_exact_or_eos`
#[pin_project::pin_project]
pub struct ByteStreamReadExact<'a> {
	#[pin]
	reader: &'a mut ByteStreamReader,
	read_len: usize,
	fail_on_eos: bool,
}

impl<'a> Future for ByteStreamReadExact<'a> {
	type Output = Result<Bytes, ReadExactError>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Bytes, ReadExactError>> {
		let mut this = self.project();

		loop {
			if let Some(bytes) = this.reader.try_get(*this.read_len) {
				return Poll::Ready(Ok(bytes));
			}
			if let Some(err) = &this.reader.err {
				let err = std::io::Error::new(err.kind(), format!("{}", err));
				return Poll::Ready(Err(ReadExactError::Stream(err)));
			}
			if this.reader.eos {
				if *this.fail_on_eos {
					return Poll::Ready(Err(ReadExactError::UnexpectedEos));
				} else {
					return Poll::Ready(Ok(this.reader.take_buffer()));
				}
			}

			let next_packet = futures::ready!(this.reader.stream.as_mut().poll_next(cx));
			this.reader.add_stream_next(next_packet);
		}
	}
}

// ----

/// Turns a `tokio::io::AsyncRead` asynchronous reader into a `ByteStream`
pub fn asyncread_stream<R: AsyncRead + Send + Sync + 'static>(reader: R) -> ByteStream {
	Box::pin(tokio_util::io::ReaderStream::new(reader))
}

/// Turns a `ByteStream` into a `tokio::io::AsyncRead` asynchronous reader
pub fn stream_asyncread(stream: ByteStream) -> impl AsyncRead + Send + Sync + 'static {
	tokio_util::io::StreamReader::new(stream)
}

/// Reads all of the content of a `ByteStream` into a BytesBuf
/// that contains everything
pub async fn read_stream_to_end(mut stream: ByteStream) -> Result<BytesBuf, std::io::Error> {
	let mut buf = BytesBuf::new();
	while let Some(part) = stream.next().await {
		buf.extend(part?);
	}

	Ok(buf)
}
