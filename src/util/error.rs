//! Module containing error types used in Garage
use err_derive::Error;
use hyper::StatusCode;
use std::io;

use crate::data::*;

/// RPC related errors
#[derive(Debug, Error)]
pub enum RPCError {
	#[error(display = "Node is down: {:?}.", _0)]
	NodeDown(UUID),

	#[error(display = "Timeout: {}", _0)]
	Timeout(#[error(source)] tokio::time::error::Elapsed),

	#[error(display = "HTTP error: {}", _0)]
	HTTP(#[error(source)] http::Error),

	#[error(display = "Hyper error: {}", _0)]
	Hyper(#[error(source)] hyper::Error),

	#[error(display = "Messagepack encode error: {}", _0)]
	RMPEncode(#[error(source)] rmp_serde::encode::Error),

	#[error(display = "Messagepack decode error: {}", _0)]
	RMPDecode(#[error(source)] rmp_serde::decode::Error),

	#[error(display = "Too many errors: {:?}", _0)]
	TooManyErrors(Vec<String>),
}

/// Regroup all Garage errors
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

	#[error(display = "Tokio join error: {}", _0)]
	TokioJoin(#[error(source)] tokio::task::JoinError),

	#[error(display = "RPC call error: {}", _0)]
	RPC(#[error(source)] RPCError),

	#[error(display = "Remote error: {} (status code {})", _0, _1)]
	RemoteError(String, StatusCode),

	#[error(display = "Bad RPC: {}", _0)]
	BadRPC(String),

	#[error(display = "Corrupt data: does not match hash {:?}", _0)]
	CorruptData(Hash),

	#[error(display = "{}", _0)]
	Message(String),
}

impl From<sled::transaction::TransactionError<Error>> for Error {
	fn from(e: sled::transaction::TransactionError<Error>) -> Error {
		match e {
			sled::transaction::TransactionError::Abort(x) => x,
			sled::transaction::TransactionError::Storage(x) => Error::Sled(x),
		}
	}
}

impl<T> From<tokio::sync::watch::error::SendError<T>> for Error {
	fn from(_e: tokio::sync::watch::error::SendError<T>) -> Error {
		Error::Message("Watch send error".to_string())
	}
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
	fn from(_e: tokio::sync::mpsc::error::SendError<T>) -> Error {
		Error::Message("MPSC send error".to_string())
	}
}
