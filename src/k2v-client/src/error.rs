use std::borrow::Cow;

use thiserror::Error;

/// Errors returned by this crate
#[derive(Error, Debug)]
pub enum Error {
	#[error("received invalid response: {0}")]
	InvalidResponse(Cow<'static, str>),
	#[error("not found")]
	NotFound,
	#[error("io error: {0}")]
	IoError(#[from] std::io::Error),
	#[error("rusoto tls error: {0}")]
	RusotoTls(#[from] rusoto_core::request::TlsError),
	#[error("rusoto http error: {0}")]
	RusotoHttp(#[from] rusoto_core::HttpDispatchError),
	#[error("deserialization error: {0}")]
	Deserialization(#[from] serde_json::Error),
	#[error("{0}")]
	Message(Cow<'static, str>),
}
