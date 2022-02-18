use std::pin::Pin;

use chrono::{DateTime, Utc};
use futures::prelude::*;
use futures::task;
use hyper::body::Bytes;

use garage_util::data::Hash;
use hmac::Mac;

use super::sha256sum;
use super::HmacSha256;
use super::LONG_DATETIME;

use crate::error::*;

/// Result of `sha256("")`
const EMPTY_STRING_HEX_DIGEST: &str =
	"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn compute_streaming_payload_signature(
	signing_hmac: &HmacSha256,
	date: DateTime<Utc>,
	scope: &str,
	previous_signature: Hash,
	content_sha256: Hash,
) -> Result<Hash, Error> {
	let string_to_sign = [
		"AWS4-HMAC-SHA256-PAYLOAD",
		&date.format(LONG_DATETIME).to_string(),
		scope,
		&hex::encode(previous_signature),
		EMPTY_STRING_HEX_DIGEST,
		&hex::encode(content_sha256),
	]
	.join("\n");

	let mut hmac = signing_hmac.clone();
	hmac.update(string_to_sign.as_bytes());

	Hash::try_from(&hmac.finalize().into_bytes()).ok_or_internal_error("Invalid signature")
}

mod payload {
	use garage_util::data::Hash;

	pub enum Error<I> {
		Parser(nom::error::Error<I>),
		BadSignature,
	}

	impl<I> Error<I> {
		pub fn description(&self) -> &str {
			match *self {
				Error::Parser(ref e) => e.code.description(),
				Error::BadSignature => "Bad signature",
			}
		}
	}

	#[derive(Debug, Clone)]
	pub struct Header {
		pub size: usize,
		pub signature: Hash,
	}

	impl Header {
		pub fn parse(input: &[u8]) -> nom::IResult<&[u8], Self, Error<&[u8]>> {
			use nom::bytes::streaming::tag;
			use nom::character::streaming::hex_digit1;
			use nom::combinator::map_res;
			use nom::number::streaming::hex_u32;

			macro_rules! try_parse {
				($expr:expr) => {
					$expr.map_err(|e| e.map(Error::Parser))?
				};
			}

			let (input, size) = try_parse!(hex_u32(input));
			let (input, _) = try_parse!(tag(";")(input));

			let (input, _) = try_parse!(tag("chunk-signature=")(input));
			let (input, data) = try_parse!(map_res(hex_digit1, hex::decode)(input));
			let signature = Hash::try_from(&data).ok_or(nom::Err::Failure(Error::BadSignature))?;

			let (input, _) = try_parse!(tag("\r\n")(input));

			let header = Header {
				size: size as usize,
				signature,
			};

			Ok((input, header))
		}
	}
}

#[derive(Debug)]
pub enum SignedPayloadStreamError {
	Stream(Error),
	InvalidSignature,
	Message(String),
}

impl SignedPayloadStreamError {
	fn message(msg: &str) -> Self {
		SignedPayloadStreamError::Message(msg.into())
	}
}

impl From<SignedPayloadStreamError> for Error {
	fn from(err: SignedPayloadStreamError) -> Self {
		match err {
			SignedPayloadStreamError::Stream(e) => e,
			SignedPayloadStreamError::InvalidSignature => {
				Error::BadRequest("Invalid payload signature".into())
			}
			SignedPayloadStreamError::Message(e) => {
				Error::BadRequest(format!("Chunk format error: {}", e))
			}
		}
	}
}

impl<I> From<payload::Error<I>> for SignedPayloadStreamError {
	fn from(err: payload::Error<I>) -> Self {
		Self::message(err.description())
	}
}

impl<I> From<nom::error::Error<I>> for SignedPayloadStreamError {
	fn from(err: nom::error::Error<I>) -> Self {
		Self::message(err.code.description())
	}
}

struct SignedPayload {
	header: payload::Header,
	data: Bytes,
}

#[pin_project::pin_project]
pub struct SignedPayloadStream<S>
where
	S: Stream<Item = Result<Bytes, Error>>,
{
	#[pin]
	stream: S,
	buf: bytes::BytesMut,
	datetime: DateTime<Utc>,
	scope: String,
	signing_hmac: HmacSha256,
	previous_signature: Hash,
}

impl<S> SignedPayloadStream<S>
where
	S: Stream<Item = Result<Bytes, Error>>,
{
	pub fn new(
		stream: S,
		signing_hmac: HmacSha256,
		datetime: DateTime<Utc>,
		scope: &str,
		seed_signature: Hash,
	) -> Self {
		Self {
			stream,
			buf: bytes::BytesMut::new(),
			datetime,
			scope: scope.into(),
			signing_hmac,
			previous_signature: seed_signature,
		}
	}

	fn parse_next(input: &[u8]) -> nom::IResult<&[u8], SignedPayload, SignedPayloadStreamError> {
		use nom::bytes::streaming::{tag, take};

		macro_rules! try_parse {
			($expr:expr) => {
				$expr.map_err(nom::Err::convert)?
			};
		}

		let (input, header) = try_parse!(payload::Header::parse(input));

		// 0-sized chunk is the last
		if header.size == 0 {
			return Ok((
				input,
				SignedPayload {
					header,
					data: Bytes::new(),
				},
			));
		}

		let (input, data) = try_parse!(take::<_, _, nom::error::Error<_>>(header.size)(input));
		let (input, _) = try_parse!(tag::<_, _, nom::error::Error<_>>("\r\n")(input));

		let data = Bytes::from(data.to_vec());

		Ok((input, SignedPayload { header, data }))
	}
}

impl<S> Stream for SignedPayloadStream<S>
where
	S: Stream<Item = Result<Bytes, Error>> + Unpin,
{
	type Item = Result<Bytes, SignedPayloadStreamError>;

	fn poll_next(
		self: Pin<&mut Self>,
		cx: &mut task::Context<'_>,
	) -> task::Poll<Option<Self::Item>> {
		use std::task::Poll;

		let mut this = self.project();

		loop {
			let (input, payload) = match Self::parse_next(this.buf) {
				Ok(res) => res,
				Err(nom::Err::Incomplete(_)) => {
					match futures::ready!(this.stream.as_mut().poll_next(cx)) {
						Some(Ok(bytes)) => {
							this.buf.extend(bytes);
							continue;
						}
						Some(Err(e)) => {
							return Poll::Ready(Some(Err(SignedPayloadStreamError::Stream(e))))
						}
						None => {
							return Poll::Ready(Some(Err(SignedPayloadStreamError::message(
								"Unexpected EOF",
							))));
						}
					}
				}
				Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
					return Poll::Ready(Some(Err(e)))
				}
			};

			// 0-sized chunk is the last
			if payload.data.is_empty() {
				return Poll::Ready(None);
			}

			let data_sha256sum = sha256sum(&payload.data);

			let expected_signature = compute_streaming_payload_signature(
				this.signing_hmac,
				*this.datetime,
				this.scope,
				*this.previous_signature,
				data_sha256sum,
			)
			.map_err(|e| {
				SignedPayloadStreamError::Message(format!("Could not build signature: {}", e))
			})?;

			if payload.header.signature != expected_signature {
				return Poll::Ready(Some(Err(SignedPayloadStreamError::InvalidSignature)));
			}

			*this.buf = input.into();
			*this.previous_signature = payload.header.signature;

			return Poll::Ready(Some(Ok(payload.data)));
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.stream.size_hint()
	}
}

#[cfg(test)]
mod tests {
	use futures::prelude::*;

	use super::{SignedPayloadStream, SignedPayloadStreamError};

	#[tokio::test]
	async fn test_interrupted_signed_payload_stream() {
		use chrono::{DateTime, Utc};

		use garage_util::data::Hash;

		let datetime = DateTime::parse_from_rfc3339("2021-12-13T13:12:42+01:00") // TODO UNIX 0
			.unwrap()
			.with_timezone(&Utc);
		let secret_key = "test";
		let region = "test";
		let scope = crate::signature::compute_scope(&datetime, region);
		let signing_hmac =
			crate::signature::signing_hmac(&datetime, secret_key, region, "s3").unwrap();

		let data: &[&[u8]] = &[b"1"];
		let body = futures::stream::iter(data.iter().map(|block| Ok(block.as_ref().into())));

		let seed_signature = Hash::default();

		let mut stream =
			SignedPayloadStream::new(body, signing_hmac, datetime, &scope, seed_signature).unwrap();

		assert!(stream.try_next().await.is_err());
		match stream.try_next().await {
			Err(SignedPayloadStreamError::Message(msg)) if msg == "Unexpected EOF" => {}
			item => panic!(
				"Unexpected result, expected early EOF error, got {:?}",
				item
			),
		}
	}
}
