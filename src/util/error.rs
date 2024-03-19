//! Module containing error types used in Garage
use std::fmt;
use std::io;

use err_derive::Error;

use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};

use crate::data::*;
use crate::encode::debug_serialize;

/// Regroup all Garage errors
#[derive(Debug, Error)]
pub enum Error {
	#[error(display = "IO error: {}", _0)]
	Io(#[error(source)] io::Error),

	#[error(display = "Hyper error: {}", _0)]
	Hyper(#[error(source)] hyper::Error),

	#[error(display = "HTTP error: {}", _0)]
	Http(#[error(source)] http::Error),

	#[error(display = "Invalid HTTP header value: {}", _0)]
	HttpHeader(#[error(source)] http::header::ToStrError),

	#[error(display = "Network error: {}", _0)]
	Net(#[error(source)] garage_net::error::Error),

	#[error(display = "DB error: {}", _0)]
	Db(#[error(source)] garage_db::Error),

	#[error(display = "Messagepack encode error: {}", _0)]
	RmpEncode(#[error(source)] rmp_serde::encode::Error),
	#[error(display = "Messagepack decode error: {}", _0)]
	RmpDecode(#[error(source)] rmp_serde::decode::Error),
	#[error(display = "JSON error: {}", _0)]
	Json(#[error(source)] serde_json::error::Error),
	#[error(display = "TOML decode error: {}", _0)]
	TomlDecode(#[error(source)] toml::de::Error),

	#[error(display = "Tokio join error: {}", _0)]
	TokioJoin(#[error(source)] tokio::task::JoinError),

	#[error(display = "Tokio semaphore acquire error: {}", _0)]
	TokioSemAcquire(#[error(source)] tokio::sync::AcquireError),

	#[error(display = "Tokio broadcast receive error: {}", _0)]
	TokioBcastRecv(#[error(source)] tokio::sync::broadcast::error::RecvError),

	#[error(display = "Remote error: {}", _0)]
	RemoteError(String),

	#[error(display = "Timeout")]
	Timeout,

	#[error(
		display = "Could not reach quorum of {} (sets={:?}). {} of {} request succeeded, others returned errors: {:?}",
		_0,
		_1,
		_2,
		_3,
		_4
	)]
	Quorum(usize, Option<usize>, usize, usize, Vec<String>),

	#[error(display = "Unexpected RPC message: {}", _0)]
	UnexpectedRpcMessage(String),

	#[error(display = "Corrupt data: does not match hash {:?}", _0)]
	CorruptData(Hash),

	#[error(display = "Missing block {:?}: no node returned a valid block", _0)]
	MissingBlock(Hash),

	#[error(display = "{}", _0)]
	Message(String),
}

impl Error {
	pub fn unexpected_rpc_message<T: Serialize>(v: T) -> Self {
		Self::UnexpectedRpcMessage(debug_serialize(&v))
	}
}

impl From<garage_db::TxError<Error>> for Error {
	fn from(e: garage_db::TxError<Error>) -> Error {
		match e {
			garage_db::TxError::Abort(x) => x,
			garage_db::TxError::Db(x) => Error::Db(x),
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

impl<'a> From<&'a str> for Error {
	fn from(v: &'a str) -> Error {
		Error::Message(v.to_string())
	}
}

impl From<String> for Error {
	fn from(v: String) -> Error {
		Error::Message(v)
	}
}

pub trait ErrorContext<T, E> {
	fn err_context<C: std::borrow::Borrow<str>>(self, ctx: C) -> Result<T, Error>;
}

impl<T, E> ErrorContext<T, E> for Result<T, E>
where
	E: std::fmt::Display,
{
	#[inline]
	fn err_context<C: std::borrow::Borrow<str>>(self, ctx: C) -> Result<T, Error> {
		match self {
			Ok(x) => Ok(x),
			Err(e) => Err(Error::Message(format!("{}\n{}", ctx.borrow(), e))),
		}
	}
}

/// Trait to map any error type to Error::Message
pub trait OkOrMessage {
	type S;
	fn ok_or_message<M: Into<String>>(self, message: M) -> Result<Self::S, Error>;
}

impl<T, E> OkOrMessage for Result<T, E>
where
	E: std::fmt::Display,
{
	type S = T;
	fn ok_or_message<M: Into<String>>(self, message: M) -> Result<T, Error> {
		match self {
			Ok(x) => Ok(x),
			Err(e) => Err(Error::Message(format!("{}: {}", message.into(), e))),
		}
	}
}

impl<T> OkOrMessage for Option<T> {
	type S = T;
	fn ok_or_message<M: Into<String>>(self, message: M) -> Result<T, Error> {
		match self {
			Some(x) => Ok(x),
			None => Err(Error::Message(message.into())),
		}
	}
}

// Custom serialization for our error type, for use in RPC.
// Errors are serialized as a string of their Display representation.
// Upon deserialization, they all become a RemoteError with the
// given representation.

impl Serialize for Error {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_str(&format!("{}", self))
	}
}

impl<'de> Deserialize<'de> for Error {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_string(ErrorVisitor)
	}
}

struct ErrorVisitor;

impl<'de> Visitor<'de> for ErrorVisitor {
	type Value = Error;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		write!(formatter, "a string that represents an error value")
	}

	fn visit_str<E>(self, error_msg: &str) -> Result<Self::Value, E> {
		Ok(Error::RemoteError(error_msg.to_string()))
	}

	fn visit_string<E>(self, error_msg: String) -> Result<Self::Value, E> {
		Ok(Error::RemoteError(error_msg))
	}
}
