use core::pin::Pin;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, Result};

// Chunk format:
// - unencrypted header
//      - u16 BE: chunk length
//          = length of encrypted payload
// - encrypted
//      - u8: flags
//      - u32 BE: request id
//      - rest:
//          - if FLAG_ERROR:
//              - u8: error kind, encoded using error::io_errorkind_to_u8
//              - rest: error message
//          - else: chunk data
//          - absent for cancel/bye message

pub(crate) type ChunkLength = u16;
pub(crate) type RequestID = u32;

pub(crate) type ChunkBuffer = [u8; u16::MAX as usize];

pub(crate) const MAX_CHUNK_LENGTH: ChunkLength = 0x8000;

/// Sending this request/response stream isn't finished, more chunks to come
pub(crate) const FLAG_HASMORE: u8 = 1 << 0;
/// Rekey the corresponding stream cipher
pub(crate) const FLAG_REKEY: u8 = 1 << 1;
/// An error happened, error message is included, this is the end of this
/// request/response stream
pub(crate) const FLAG_ERROR: u8 = 1 << 2;
/// Ask to cancel the corresponding request currently handled by the server
pub(crate) const FLAG_CANCEL: u8 = 1 << 3;
/// Close connection
pub(crate) const FLAG_BYE: u8 = 1 << 4;

pub(crate) fn new_buffer() -> Box<ChunkBuffer> {
	Box::new([0u8; u16::MAX as usize])
}

pub(crate) async fn read_chunk<R: AsyncRead>(
	mut stream: Pin<&mut R>,
	buf: &mut ChunkBuffer,
) -> Result<usize> {
	let mut msg_len_buf = [0_u8; 2];
	stream.read_exact(&mut msg_len_buf).await?;
	let chunk_len = usize::from(u16::from_be_bytes(msg_len_buf));

	stream.read_exact(&mut buf[..chunk_len]).await?;

	Ok(chunk_len)
}

pub(crate) async fn write_chunk<W: AsyncWrite>(mut stream: Pin<&mut W>, buf: &[u8]) -> Result<()> {
	assert!(buf.len() <= u16::MAX as usize);

	let msg_len = u16::to_be_bytes(buf.len() as u16);
	stream.write_all(&msg_len[..]).await?;
	stream.write_all(buf).await?;
	stream.flush().await?;

	Ok(())
}
