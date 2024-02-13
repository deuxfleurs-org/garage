use std::borrow::Cow;

use thiserror::Error;

/// Errors returned by this crate
#[derive(Error, Debug)]
pub enum Error {
	#[error("{0}, {1}: {2} (path = {3})")]
	Remote(
		http::StatusCode,
		Cow<'static, str>,
		Cow<'static, str>,
		Cow<'static, str>,
	),
	#[error("received invalid response: {0}")]
	InvalidResponse(Cow<'static, str>),
	#[error("not found")]
	NotFound,
	#[error("io error: {0}")]
	IoError(#[from] std::io::Error),
	#[error("http error: {0}")]
	Http(#[from] http::Error),
	#[error("hyper error: {0}")]
	Hyper(#[from] hyper::Error),
	#[error("hyper client error: {0}")]
	HyperClient(#[from] hyper_util::client::legacy::Error),
	#[error("invalid header: {0}")]
	Header(#[from] hyper::header::ToStrError),
	#[error("deserialization error: {0}")]
	Deserialization(#[from] serde_json::Error),
	#[error("invalid signature parameters: {0}")]
	SignParameters(#[from] aws_sigv4::sign::v4::signing_params::BuildError),
	#[error("could not sign request: {0}")]
	SignRequest(#[from] aws_sigv4::http_request::SigningError),
	#[error("request timed out")]
	Timeout,
	#[error("{0}")]
	Message(Cow<'static, str>),
}
