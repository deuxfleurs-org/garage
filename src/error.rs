use err_derive::Error;
use hyper::StatusCode;
use std::io;

use crate::data::Hash;
use crate::rpc_client::RPCError;

#[derive(Debug, Error)]
pub enum Error {
	#[error(display = "IO error: {}", _0)]
	Io(#[error(source)] io::Error),

	#[error(display = "Hyper error: {}", _0)]
	Hyper(#[error(source)] hyper::Error),

	#[error(display = "HTTP error: {}", _0)]
	HTTP(#[error(source)] http::Error),

	#[error(display = "Invalid HTTP header value: {}", _0)]
	HTTPHeader(#[error(source)] http::header::ToStrError),

	#[error(display = "TLS error: {}", _0)]
	TLS(#[error(source)] rustls::TLSError),

	#[error(display = "PKI error: {}", _0)]
	PKI(#[error(source)] webpki::Error),

	#[error(display = "Sled error: {}", _0)]
	Sled(#[error(source)] sled::Error),

	#[error(display = "Messagepack encode error: {}", _0)]
	RMPEncode(#[error(source)] rmp_serde::encode::Error),
	#[error(display = "Messagepack decode error: {}", _0)]
	RMPDecode(#[error(source)] rmp_serde::decode::Error),
	#[error(display = "JSON error: {}", _0)]
	JSON(#[error(source)] serde_json::error::Error),
	#[error(display = "TOML decode error: {}", _0)]
	TomlDecode(#[error(source)] toml::de::Error),

	#[error(display = "Timeout: {}", _0)]
	RPCTimeout(#[error(source)] tokio::time::Elapsed),

	#[error(display = "Tokio join error: {}", _0)]
	TokioJoin(#[error(source)] tokio::task::JoinError),

	#[error(display = "RPC call error: {}", _0)]
	RPC(#[error(source)] RPCError),

	#[error(display = "Remote error: {} (status code {})", _0, _1)]
	RemoteError(String, StatusCode),

	#[error(display = "Bad request: {}", _0)]
	BadRequest(String),

	#[error(display = "Not found")]
	NotFound,

	#[error(display = "Corrupt data: does not match hash {:?}", _0)]
	CorruptData(Hash),

	#[error(display = "{}", _0)]
	Message(String),
}

impl Error {
	pub fn http_status_code(&self) -> StatusCode {
		match self {
			Error::BadRequest(_) => StatusCode::BAD_REQUEST,
			Error::NotFound => StatusCode::NOT_FOUND,
			_ => StatusCode::INTERNAL_SERVER_ERROR,
		}
	}
}

impl From<sled::TransactionError<Error>> for Error {
	fn from(e: sled::TransactionError<Error>) -> Error {
		match e {
			sled::TransactionError::Abort(x) => x,
			sled::TransactionError::Storage(x) => Error::Sled(x),
		}
	}
}

impl<T> From<tokio::sync::watch::error::SendError<T>> for Error {
	fn from(_e: tokio::sync::watch::error::SendError<T>) -> Error {
		Error::Message(format!("Watch send error"))
	}
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
	fn from(_e: tokio::sync::mpsc::error::SendError<T>) -> Error {
		Error::Message(format!("MPSC send error"))
	}
}
