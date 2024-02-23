use std::cmp::Ordering;
use std::collections::VecDeque;

use bytes::BytesMut;

use crate::stream::ByteStream;

pub use bytes::Bytes;

/// A circular buffer of bytes, internally represented as a list of Bytes
/// for optimization, but that for all intent and purposes acts just like
/// a big byte slice which can be extended on the right and from which
/// stuff can be taken on the left.
pub struct BytesBuf {
	buf: VecDeque<Bytes>,
	buf_len: usize,
}

impl BytesBuf {
	/// Creates a new empty BytesBuf
	pub fn new() -> Self {
		Self {
			buf: VecDeque::new(),
			buf_len: 0,
		}
	}

	/// Returns the number of bytes stored in the BytesBuf
	#[inline]
	pub fn len(&self) -> usize {
		self.buf_len
	}

	/// Returns true iff the BytesBuf contains zero bytes
	#[inline]
	pub fn is_empty(&self) -> bool {
		self.buf_len == 0
	}

	/// Adds some bytes to the right of the buffer
	pub fn extend(&mut self, b: Bytes) {
		if !b.is_empty() {
			self.buf_len += b.len();
			self.buf.push_back(b);
		}
	}

	/// Takes the whole content of the buffer and returns it as a single Bytes unit
	pub fn take_all(&mut self) -> Bytes {
		if self.buf.is_empty() {
			Bytes::new()
		} else if self.buf.len() == 1 {
			self.buf_len = 0;
			self.buf.pop_back().unwrap()
		} else {
			let mut ret = BytesMut::with_capacity(self.buf_len);
			for b in self.buf.iter() {
				ret.extend_from_slice(&b[..]);
			}
			self.buf.clear();
			self.buf_len = 0;
			ret.freeze()
		}
	}

	/// Takes at most max_len bytes from the left of the buffer
	pub fn take_max(&mut self, max_len: usize) -> Bytes {
		if self.buf_len <= max_len {
			self.take_all()
		} else {
			self.take_exact_ok(max_len)
		}
	}

	/// Take exactly len bytes from the left of the buffer, returns None if
	/// the BytesBuf doesn't contain enough data
	pub fn take_exact(&mut self, len: usize) -> Option<Bytes> {
		if self.buf_len < len {
			None
		} else {
			Some(self.take_exact_ok(len))
		}
	}

	fn take_exact_ok(&mut self, len: usize) -> Bytes {
		assert!(len <= self.buf_len);
		let front = self.buf.pop_front().unwrap();
		match front.len().cmp(&len) {
			Ordering::Greater => {
				self.buf.push_front(front.slice(len..));
				self.buf_len -= len;
				front.slice(..len)
			}
			Ordering::Equal => {
				self.buf_len -= len;
				front
			}
			Ordering::Less => {
				let mut ret = BytesMut::with_capacity(len);
				ret.extend_from_slice(&front[..]);
				self.buf_len -= front.len();
				while ret.len() < len {
					let front = self.buf.pop_front().unwrap();
					if front.len() > len - ret.len() {
						let take = len - ret.len();
						ret.extend_from_slice(&front[..take]);
						self.buf.push_front(front.slice(take..));
						self.buf_len -= take;
						break;
					} else {
						ret.extend_from_slice(&front[..]);
						self.buf_len -= front.len();
					}
				}
				ret.freeze()
			}
		}
	}

	/// Return the internal sequence of Bytes slices that make up the buffer
	pub fn into_slices(self) -> VecDeque<Bytes> {
		self.buf
	}

	/// Return the entire buffer concatenated into a single big Bytes
	pub fn into_bytes(mut self) -> Bytes {
		self.take_all()
	}

	/// Return the content as a stream of individual chunks
	pub fn into_stream(self) -> ByteStream {
		use futures::stream::StreamExt;
		Box::pin(futures::stream::iter(self.buf).map(|x| Ok(x)))
	}
}

impl Default for BytesBuf {
	fn default() -> Self {
		Self::new()
	}
}

impl From<Bytes> for BytesBuf {
	fn from(b: Bytes) -> BytesBuf {
		let mut ret = BytesBuf::new();
		ret.extend(b);
		ret
	}
}

impl From<BytesBuf> for Bytes {
	fn from(mut b: BytesBuf) -> Bytes {
		b.take_all()
	}
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn test_bytes_buf() {
		let mut buf = BytesBuf::new();
		assert!(buf.len() == 0);
		assert!(buf.is_empty());

		buf.extend(Bytes::from(b"Hello, world!".to_vec()));
		assert!(buf.len() == 13);
		assert!(!buf.is_empty());

		buf.extend(Bytes::from(b"1234567890".to_vec()));
		assert!(buf.len() == 23);
		assert!(!buf.is_empty());

		assert_eq!(
			buf.take_all(),
			Bytes::from(b"Hello, world!1234567890".to_vec())
		);
		assert!(buf.len() == 0);
		assert!(buf.is_empty());

		buf.extend(Bytes::from(b"1234567890".to_vec()));
		buf.extend(Bytes::from(b"Hello, world!".to_vec()));
		assert!(buf.len() == 23);
		assert!(!buf.is_empty());

		assert_eq!(buf.take_max(12), Bytes::from(b"1234567890He".to_vec()));
		assert!(buf.len() == 11);

		assert_eq!(buf.take_exact(12), None);
		assert!(buf.len() == 11);
		assert_eq!(
			buf.take_exact(11),
			Some(Bytes::from(b"llo, world!".to_vec()))
		);
		assert!(buf.len() == 0);
		assert!(buf.is_empty());
	}
}
