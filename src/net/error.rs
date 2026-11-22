use std::io;

use log::error;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
	#[error("IO error: {0}")]
	Io(#[from] io::Error),

	#[error("Messagepack encode error: {0}")]
	RMPEncode(#[from] rmp_serde::encode::Error),
	#[error("Messagepack decode error: {0}")]
	RMPDecode(#[from] rmp_serde::decode::Error),

	#[error("Tokio join error: {0}")]
	TokioJoin(#[from] tokio::task::JoinError),

	#[error("oneshot receive error: {0}")]
	OneshotRecv(#[from] tokio::sync::oneshot::error::RecvError),

	#[error("Noise protocol error: {0}")]
	NoiseProtocol(#[from] snow::error::Error),

	#[error("UTF8 error: {0}")]
	UTF8(#[from] std::string::FromUtf8Error),

	#[error("Framing protocol error")]
	Framing,

	#[error("Remote error ({0:?}): {1}")]
	Remote(io::ErrorKind, String),

	#[error("Request ID collision")]
	IdCollision,

	#[error("{0}")]
	Message(String),

	#[error("No handler / shutting down")]
	NoHandler,

	#[error("Connection closed")]
	ConnectionClosed,

	#[error("Version mismatch: {0}")]
	VersionMismatch(String),

	#[error("Handshake timeout")]
	HandshakeTimeout,
}

impl<T> From<tokio::sync::watch::error::SendError<T>> for Error {
	fn from(_e: tokio::sync::watch::error::SendError<T>) -> Error {
		Error::Message("Watch send error".into())
	}
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
	fn from(_e: tokio::sync::mpsc::error::SendError<T>) -> Error {
		Error::Message("MPSC send error".into())
	}
}

/// The trait adds a `.log_err()` method on `Result<(), E>` types,
/// which dismisses the error by logging it to stderr.
pub trait LogError {
	fn log_err(self, msg: &'static str);
}

impl<E> LogError for Result<(), E>
where
	E: Into<Error>,
{
	fn log_err(self, msg: &'static str) {
		if let Err(e) = self {
			error!("Error: {}: {}", msg, Into::<Error>::into(e));
		}
	}
}

impl<E, T> LogError for Result<T, E>
where
	T: LogError,
	E: Into<Error>,
{
	fn log_err(self, msg: &'static str) {
		match self {
			Err(e) => error!("Error: {}: {}", msg, Into::<Error>::into(e)),
			Ok(x) => x.log_err(msg),
		}
	}
}

// ---- Helpers for serializing I/O Errors

pub(crate) fn u8_to_io_errorkind(v: u8) -> std::io::ErrorKind {
	use std::io::ErrorKind;
	match v {
		101 => ErrorKind::ConnectionAborted,
		102 => ErrorKind::BrokenPipe,
		103 => ErrorKind::WouldBlock,
		104 => ErrorKind::InvalidInput,
		105 => ErrorKind::InvalidData,
		106 => ErrorKind::TimedOut,
		107 => ErrorKind::Interrupted,
		108 => ErrorKind::UnexpectedEof,
		109 => ErrorKind::OutOfMemory,
		110 => ErrorKind::ConnectionReset,
		_ => ErrorKind::Other,
	}
}

pub(crate) fn io_errorkind_to_u8(kind: std::io::ErrorKind) -> u8 {
	use std::io::ErrorKind;
	match kind {
		ErrorKind::ConnectionAborted => 101,
		ErrorKind::BrokenPipe => 102,
		ErrorKind::WouldBlock => 103,
		ErrorKind::InvalidInput => 104,
		ErrorKind::InvalidData => 105,
		ErrorKind::TimedOut => 106,
		ErrorKind::Interrupted => 107,
		ErrorKind::UnexpectedEof => 108,
		ErrorKind::OutOfMemory => 109,
		ErrorKind::ConnectionReset => 110,
		_ => 100,
	}
}
