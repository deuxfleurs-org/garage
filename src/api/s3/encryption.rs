use std::borrow::Cow;
use std::convert::TryInto;
use std::pin::Pin;

use aes_gcm::{
	aead::stream::{DecryptorLE31, EncryptorLE31, StreamLE31},
	aead::{Aead, AeadCore, KeyInit, OsRng},
	aes::cipher::crypto_common::rand_core::RngCore,
	aes::cipher::typenum::Unsigned,
	Aes256Gcm, Key, Nonce,
};
use base64::prelude::*;
use bytes::Bytes;

use futures::stream::Stream;
use futures::task;
use tokio::io::BufReader;

use http::header::{HeaderMap, HeaderName, HeaderValue};

use garage_net::bytes_buf::BytesBuf;
use garage_net::stream::{stream_asyncread, ByteStream};
use garage_rpc::rpc_helper::OrderTag;
use garage_util::data::Hash;
use garage_util::error::Error as GarageError;
use garage_util::migrate::Migrate;

use garage_model::garage::Garage;
use garage_model::s3::object_table::{ObjectVersionEncryption, ObjectVersionMetaInner};

use garage_api_common::common_error::*;
use garage_api_common::signature::checksum::Md5Checksum;

use crate::error::Error;

const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: HeaderName =
	HeaderName::from_static("x-amz-server-side-encryption-customer-algorithm");
const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: HeaderName =
	HeaderName::from_static("x-amz-server-side-encryption-customer-key");
const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: HeaderName =
	HeaderName::from_static("x-amz-server-side-encryption-customer-key-md5");

const X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: HeaderName =
	HeaderName::from_static("x-amz-copy-source-server-side-encryption-customer-algorithm");
const X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: HeaderName =
	HeaderName::from_static("x-amz-copy-source-server-side-encryption-customer-key");
const X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: HeaderName =
	HeaderName::from_static("x-amz-copy-source-server-side-encryption-customer-key-md5");

const CUSTOMER_ALGORITHM_AES256: &[u8] = b"AES256";

type Md5Output = md5::digest::Output<md5::Md5Core>;

type StreamNonceSize = aes_gcm::aead::stream::NonceSize<Aes256Gcm, StreamLE31<Aes256Gcm>>;

// Data blocks are encrypted by smaller chunks of size 4096 bytes,
// so that data can be streamed when reading.
// This size has to be known and has to be constant, or data won't be
// readable anymore. DO NOT CHANGE THIS VALUE.
const STREAM_ENC_PLAIN_CHUNK_SIZE: usize = 0x1000; // 4096 bytes
const STREAM_ENC_CYPER_CHUNK_SIZE: usize = STREAM_ENC_PLAIN_CHUNK_SIZE + 16;

#[derive(Clone, Copy)]
pub enum EncryptionParams {
	Plaintext,
	SseC {
		client_key: Key<Aes256Gcm>,
		client_key_md5: Md5Output,
		compression_level: Option<i32>,
	},
}

impl EncryptionParams {
	pub fn is_encrypted(&self) -> bool {
		!matches!(self, Self::Plaintext)
	}

	pub fn is_same(a: &Self, b: &Self) -> bool {
		let relevant_info = |x: &Self| match x {
			Self::Plaintext => None,
			Self::SseC {
				client_key,
				compression_level,
				..
			} => Some((*client_key, compression_level.is_some())),
		};
		relevant_info(a) == relevant_info(b)
	}

	pub fn new_from_headers(
		garage: &Garage,
		headers: &HeaderMap,
	) -> Result<EncryptionParams, Error> {
		let key = parse_request_headers(
			headers,
			&X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
			&X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
			&X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
		)?;
		match key {
			Some((client_key, client_key_md5)) => Ok(EncryptionParams::SseC {
				client_key,
				client_key_md5,
				compression_level: garage.config.compression_level,
			}),
			None => Ok(EncryptionParams::Plaintext),
		}
	}

	pub fn add_response_headers(&self, resp: &mut http::response::Builder) {
		if let Self::SseC { client_key_md5, .. } = self {
			let md5 = BASE64_STANDARD.encode(&client_key_md5);

			resp.headers_mut().unwrap().insert(
				X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
				HeaderValue::from_bytes(CUSTOMER_ALGORITHM_AES256).unwrap(),
			);
			resp.headers_mut().unwrap().insert(
				X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
				HeaderValue::from_bytes(md5.as_bytes()).unwrap(),
			);
		}
	}

	pub fn check_decrypt<'a>(
		garage: &Garage,
		headers: &HeaderMap,
		obj_enc: &'a ObjectVersionEncryption,
	) -> Result<(Self, Cow<'a, ObjectVersionMetaInner>), Error> {
		let key = parse_request_headers(
			headers,
			&X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
			&X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
			&X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
		)?;
		Self::check_decrypt_common(garage, key, obj_enc)
	}

	pub fn check_decrypt_for_copy_source<'a>(
		garage: &Garage,
		headers: &HeaderMap,
		obj_enc: &'a ObjectVersionEncryption,
	) -> Result<(Self, Cow<'a, ObjectVersionMetaInner>), Error> {
		let key = parse_request_headers(
			headers,
			&X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
			&X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
			&X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
		)?;
		Self::check_decrypt_common(garage, key, obj_enc)
	}

	fn check_decrypt_common<'a>(
		garage: &Garage,
		key: Option<(Key<Aes256Gcm>, Md5Output)>,
		obj_enc: &'a ObjectVersionEncryption,
	) -> Result<(Self, Cow<'a, ObjectVersionMetaInner>), Error> {
		match (key, &obj_enc) {
			(
				Some((client_key, client_key_md5)),
				ObjectVersionEncryption::SseC { inner, compressed },
			) => {
				let enc = Self::SseC {
					client_key,
					client_key_md5,
					compression_level: if *compressed {
						Some(garage.config.compression_level.unwrap_or(1))
					} else {
						None
					},
				};
				let plaintext = enc.decrypt_blob(&inner)?;
				let inner = ObjectVersionMetaInner::decode(&plaintext)
					.ok_or_internal_error("Could not decode encrypted metadata")?;
				Ok((enc, Cow::Owned(inner)))
			}
			(None, ObjectVersionEncryption::Plaintext { inner }) => {
				Ok((Self::Plaintext, Cow::Borrowed(inner)))
			}
			(_, ObjectVersionEncryption::SseC { .. }) => {
				Err(Error::bad_request("Object is encrypted"))
			}
			(Some(_), _) => {
				// TODO: should this be an OK scenario?
				Err(Error::bad_request("Trying to decrypt a plaintext object"))
			}
		}
	}

	pub fn encrypt_meta(
		&self,
		meta: ObjectVersionMetaInner,
	) -> Result<ObjectVersionEncryption, Error> {
		match self {
			Self::SseC {
				compression_level, ..
			} => {
				let plaintext = meta.encode().map_err(GarageError::from)?;
				let ciphertext = self.encrypt_blob(&plaintext)?;
				Ok(ObjectVersionEncryption::SseC {
					inner: ciphertext.into_owned(),
					compressed: compression_level.is_some(),
				})
			}
			Self::Plaintext => Ok(ObjectVersionEncryption::Plaintext { inner: meta }),
		}
	}

	// ---- generating object Etag values ----
	pub fn etag_from_md5(&self, md5sum: &Option<Md5Checksum>) -> String {
		match self {
			Self::Plaintext => md5sum
				.map(|x| hex::encode(&x[..]))
				.expect("md5 digest should have been computed"),
			Self::SseC { .. } => {
				// AWS specifies that for encrypted objects, the Etag is not
				// the md5sum of the data, but doesn't say what it is.
				// So we just put some random bytes.
				let mut random = [0u8; 16];
				OsRng.fill_bytes(&mut random);
				hex::encode(&random)
			}
		}
	}

	// ---- generic function for encrypting / decrypting blobs ----
	// Prepends a randomly-generated nonce to the encrypted value.
	// This is used for encrypting object metadata and inlined data for small objects.
	// This does not compress anything.

	pub fn encrypt_blob<'a>(&self, blob: &'a [u8]) -> Result<Cow<'a, [u8]>, Error> {
		match self {
			Self::SseC { client_key, .. } => {
				let cipher = Aes256Gcm::new(&client_key);
				let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
				let ciphertext = cipher
					.encrypt(&nonce, blob)
					.ok_or_internal_error("Encryption failed")?;
				Ok(Cow::Owned([nonce.to_vec(), ciphertext].concat()))
			}
			Self::Plaintext => Ok(Cow::Borrowed(blob)),
		}
	}

	pub fn decrypt_blob<'a>(&self, blob: &'a [u8]) -> Result<Cow<'a, [u8]>, Error> {
		match self {
			Self::SseC { client_key, .. } => {
				let cipher = Aes256Gcm::new(&client_key);
				let nonce_size = <Aes256Gcm as AeadCore>::NonceSize::to_usize();
				let nonce = Nonce::from_slice(
					blob.get(..nonce_size)
						.ok_or_internal_error("invalid encrypted data")?,
				);
				let plaintext = cipher
					.decrypt(nonce, &blob[nonce_size..])
					.ok_or_bad_request(
						"Invalid encryption key, could not decrypt object metadata.",
					)?;
				Ok(Cow::Owned(plaintext))
			}
			Self::Plaintext => Ok(Cow::Borrowed(blob)),
		}
	}

	// ----  function for encrypting / decrypting byte streams ----

	/// Get a data block from the storage node, and decrypt+decompress it
	/// if necessary. If object is plaintext, just get it without any processing.
	pub async fn get_block(
		&self,
		garage: &Garage,
		hash: &Hash,
		order: Option<OrderTag>,
	) -> Result<ByteStream, GarageError> {
		let raw_block = garage
			.block_manager
			.rpc_get_block_streaming(hash, order)
			.await?;
		Ok(self.decrypt_block_stream(raw_block))
	}

	pub fn decrypt_block_stream(&self, stream: ByteStream) -> ByteStream {
		match self {
			Self::Plaintext => stream,
			Self::SseC {
				client_key,
				compression_level,
				..
			} => {
				let plaintext = DecryptStream::new(stream, *client_key);
				if compression_level.is_some() {
					let reader = stream_asyncread(Box::pin(plaintext));
					let reader = BufReader::new(reader);
					let reader = async_compression::tokio::bufread::ZstdDecoder::new(reader);
					Box::pin(tokio_util::io::ReaderStream::new(reader))
				} else {
					Box::pin(plaintext)
				}
			}
		}
	}

	/// Encrypt a data block if encryption is set, for use before
	/// putting the data blocks into storage
	pub fn encrypt_block(&self, block: Bytes) -> Result<Bytes, Error> {
		match self {
			Self::Plaintext => Ok(block),
			Self::SseC {
				client_key,
				compression_level,
				..
			} => {
				let block = if let Some(level) = compression_level {
					Cow::Owned(
						garage_block::zstd_encode(block.as_ref(), *level)
							.ok_or_internal_error("failed to compress data block")?,
					)
				} else {
					Cow::Borrowed(block.as_ref())
				};

				let mut ret = Vec::with_capacity(block.len() + 32 + block.len() / 64);

				let mut nonce: Nonce<StreamNonceSize> = Default::default();
				OsRng.fill_bytes(&mut nonce);
				ret.extend_from_slice(nonce.as_slice());

				let mut cipher = EncryptorLE31::<Aes256Gcm>::new(&client_key, &nonce);
				let mut iter = block.chunks(STREAM_ENC_PLAIN_CHUNK_SIZE).peekable();

				if iter.peek().is_none() {
					// Empty stream: we encrypt an empty last chunk
					let chunk_enc = cipher
						.encrypt_last(&[][..])
						.ok_or_internal_error("failed to encrypt chunk")?;
					ret.extend_from_slice(&chunk_enc);
				} else {
					loop {
						let chunk = iter.next().unwrap();
						if iter.peek().is_some() {
							let chunk_enc = cipher
								.encrypt_next(chunk)
								.ok_or_internal_error("failed to encrypt chunk")?;
							assert_eq!(chunk.len(), STREAM_ENC_PLAIN_CHUNK_SIZE);
							assert_eq!(chunk_enc.len(), STREAM_ENC_CYPER_CHUNK_SIZE);
							ret.extend_from_slice(&chunk_enc);
						} else {
							// use encrypt_last for the last chunk
							let chunk_enc = cipher
								.encrypt_last(chunk)
								.ok_or_internal_error("failed to encrypt chunk")?;
							ret.extend_from_slice(&chunk_enc);
							break;
						}
					}
				}

				Ok(ret.into())
			}
		}
	}
}

fn parse_request_headers(
	headers: &HeaderMap,
	alg_header: &HeaderName,
	key_header: &HeaderName,
	md5_header: &HeaderName,
) -> Result<Option<(Key<Aes256Gcm>, Md5Output)>, Error> {
	let alg = headers.get(alg_header).map(HeaderValue::as_bytes);
	let key = headers.get(key_header).map(HeaderValue::as_bytes);
	let md5 = headers.get(md5_header).map(HeaderValue::as_bytes);

	match alg {
		Some(CUSTOMER_ALGORITHM_AES256) => {
			use md5::{Digest, Md5};

			let key_b64 =
				key.ok_or_bad_request("Missing server-side-encryption-customer-key header")?;
			let key_bytes: [u8; 32] = BASE64_STANDARD
				.decode(&key_b64)
				.ok_or_bad_request(
					"Invalid server-side-encryption-customer-key header: invalid base64",
				)?
				.try_into()
				.ok()
				.ok_or_bad_request(
					"Invalid server-side-encryption-customer-key header: invalid length",
				)?;

			let md5_b64 =
				md5.ok_or_bad_request("Missing server-side-encryption-customer-key-md5 header")?;
			let md5_bytes = BASE64_STANDARD.decode(&md5_b64).ok_or_bad_request(
				"Invalid server-side-encryption-customer-key-md5 header: invalid bass64",
			)?;

			let mut hasher = Md5::new();
			hasher.update(&key_bytes[..]);
			let our_md5 = hasher.finalize();
			if our_md5.as_slice() != md5_bytes.as_slice() {
				return Err(Error::bad_request(
					"Server-side encryption client key MD5 checksum does not match",
				));
			}

			Ok(Some((key_bytes.into(), our_md5)))
		}
		Some(alg) => Err(Error::InvalidEncryptionAlgorithm(
			String::from_utf8_lossy(alg).into_owned(),
		)),
		None => {
			if key.is_some() || md5.is_some() {
				Err(Error::bad_request(
					"Unexpected server-side-encryption-customer-key{,-md5} header(s)",
				))
			} else {
				Ok(None)
			}
		}
	}
}

// ---- encrypt & decrypt streams ----

#[pin_project::pin_project]
struct DecryptStream {
	#[pin]
	stream: ByteStream,
	done_reading: bool,
	buf: BytesBuf,
	key: Key<Aes256Gcm>,
	state: DecryptStreamState,
}

enum DecryptStreamState {
	Starting,
	Running(DecryptorLE31<Aes256Gcm>),
	Done,
}

impl DecryptStream {
	fn new(stream: ByteStream, key: Key<Aes256Gcm>) -> Self {
		Self {
			stream,
			done_reading: false,
			buf: BytesBuf::new(),
			key,
			state: DecryptStreamState::Starting,
		}
	}
}

impl Stream for DecryptStream {
	type Item = Result<Bytes, std::io::Error>;

	fn poll_next(
		self: Pin<&mut Self>,
		cx: &mut task::Context<'_>,
	) -> task::Poll<Option<Self::Item>> {
		use std::task::Poll;

		let mut this = self.project();

		// The first bytes of the stream should contain the starting nonce.
		// If we don't have a Running state, it means that we haven't
		// yet read the nonce.
		while matches!(this.state, DecryptStreamState::Starting) {
			let nonce_size = StreamNonceSize::to_usize();
			if let Some(nonce) = this.buf.take_exact(nonce_size) {
				let nonce = Nonce::from_slice(nonce.as_ref());
				*this.state = DecryptStreamState::Running(DecryptorLE31::new(&this.key, nonce));
				break;
			}

			match futures::ready!(this.stream.as_mut().poll_next(cx)) {
				Some(Ok(bytes)) => {
					this.buf.extend(bytes);
				}
				Some(Err(e)) => {
					return Poll::Ready(Some(Err(e)));
				}
				None => {
					return Poll::Ready(Some(Err(std::io::Error::new(
						std::io::ErrorKind::UnexpectedEof,
						"Decrypt: unexpected EOF, could not read nonce",
					))));
				}
			}
		}

		// Read at least one byte more than the encrypted chunk size
		// (if possible), so that we know if we are decrypting the
		// last chunk or not.
		while !*this.done_reading && this.buf.len() <= STREAM_ENC_CYPER_CHUNK_SIZE {
			match futures::ready!(this.stream.as_mut().poll_next(cx)) {
				Some(Ok(bytes)) => {
					this.buf.extend(bytes);
				}
				Some(Err(e)) => {
					return Poll::Ready(Some(Err(e)));
				}
				None => {
					*this.done_reading = true;
					break;
				}
			}
		}

		if matches!(this.state, DecryptStreamState::Done) {
			if !this.buf.is_empty() {
				return Poll::Ready(Some(Err(std::io::Error::new(
					std::io::ErrorKind::Other,
					"Decrypt: unexpected bytes after last encrypted chunk",
				))));
			}
			return Poll::Ready(None);
		}

		let res = if this.buf.len() > STREAM_ENC_CYPER_CHUNK_SIZE {
			// we have strictly more bytes than the encrypted chunk size,
			// so we know this is not the last
			let DecryptStreamState::Running(ref mut cipher) = this.state else {
				unreachable!()
			};
			let chunk = this.buf.take_exact(STREAM_ENC_CYPER_CHUNK_SIZE).unwrap();
			let chunk_dec = cipher.decrypt_next(chunk.as_ref());
			if let Ok(c) = &chunk_dec {
				assert_eq!(c.len(), STREAM_ENC_PLAIN_CHUNK_SIZE);
			}
			chunk_dec
		} else {
			// We have one encrypted chunk size or less, even though we tried
			// to read more, so this is the last chunk. Decrypt using the
			// appropriate decrypt_last() function that then destroys the cipher.
			let state = std::mem::replace(this.state, DecryptStreamState::Done);
			let DecryptStreamState::Running(cipher) = state else {
				unreachable!()
			};
			let chunk = this.buf.take_all();
			cipher.decrypt_last(chunk.as_ref())
		};

		match res {
			Ok(bytes) if bytes.is_empty() => Poll::Ready(None),
			Ok(bytes) => Poll::Ready(Some(Ok(bytes.into()))),
			Err(_) => Poll::Ready(Some(Err(std::io::Error::new(
				std::io::ErrorKind::Other,
				"Decryption failed",
			)))),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use futures::stream::StreamExt;
	use garage_net::stream::read_stream_to_end;

	fn stream() -> ByteStream {
		Box::pin(
			futures::stream::iter(16usize..1024)
				.map(|i| Ok(Bytes::from(vec![(i % 256) as u8; (i * 37) % 1024]))),
		)
	}

	async fn test_block_enc(compression_level: Option<i32>) {
		let enc = EncryptionParams::SseC {
			client_key: Aes256Gcm::generate_key(&mut OsRng),
			client_key_md5: Default::default(), // not needed
			compression_level,
		};

		let block_plain = read_stream_to_end(stream()).await.unwrap().into_bytes();

		let block_enc = enc.encrypt_block(block_plain.clone()).unwrap();

		let block_dec =
			enc.decrypt_block_stream(Box::pin(futures::stream::once(async { Ok(block_enc) })));
		let block_dec = read_stream_to_end(block_dec).await.unwrap().into_bytes();

		assert_eq!(block_plain, block_dec);
		assert!(block_dec.len() > 128000);
	}

	#[tokio::test]
	async fn test_encrypt_block() {
		test_block_enc(None).await
	}

	#[tokio::test]
	async fn test_encrypt_block_compressed() {
		test_block_enc(Some(1)).await
	}
}
