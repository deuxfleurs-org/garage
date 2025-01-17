use std::io;

use err_derive::Error;
use log::error;

#[derive(Debug, Error)]
pub enum Error {
	#[error(display = "IO error: {}", _0)]
	Io(#[error(source)] io::Error),

	#[error(display = "Messagepack encode error: {}", _0)]
	RMPEncode(#[error(source)] rmp_serde::encode::Error),
	#[error(display = "Messagepack decode error: {}", _0)]
	RMPDecode(#[error(source)] rmp_serde::decode::Error),

	#[error(display = "Tokio join error: {}", _0)]
	TokioJoin(#[error(source)] tokio::task::JoinError),

	#[error(display = "oneshot receive error: {}", _0)]
	OneshotRecv(#[error(source)] tokio::sync::oneshot::error::RecvError),

	#[error(display = "Handshake error: {}", _0)]
	Handshake(#[error(source)] kuska_handshake::async_std::Error),

	#[error(display = "UTF8 error: {}", _0)]
	UTF8(#[error(source)] std::string::FromUtf8Error),

	#[error(display = "Framing protocol error")]
	Framing,

	#[error(display = "Remote error ({:?}): {}", _0, _1)]
	Remote(io::ErrorKind, String),

	#[error(display = "Request ID collision")]
	IdCollision,

	#[error(display = "{}", _0)]
	Message(String),

	#[error(display = "No handler / shutting down")]
	NoHandler,

	#[error(display = "Connection closed")]
	ConnectionClosed,

	#[error(display = "Version mismatch: {}", _0)]
	VersionMismatch(String),
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
		};
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
