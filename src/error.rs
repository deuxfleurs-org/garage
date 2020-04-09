use std::io;
use err_derive::Error;
use hyper::StatusCode;

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

	#[error(display = "Sled error: {}", _0)]
	Sled(#[error(source)] sled::Error),

	#[error(display = "Messagepack encode error: {}", _0)]
	RMPEncode(#[error(source)] rmp_serde::encode::Error),
	#[error(display = "Messagepack decode error: {}", _0)]
	RMPDecode(#[error(source)] rmp_serde::decode::Error),

	#[error(display = "TOML decode error: {}", _0)]
	TomlDecode(#[error(source)] toml::de::Error),

	#[error(display = "Timeout: {}", _0)]
	RPCTimeout(#[error(source)] tokio::time::Elapsed),

	#[error(display = "RPC error: {}", _0)]
	RPCError(String),

	#[error(display = "Bad request: {}", _0)]
	BadRequest(String),

	#[error(display = "Not found")]
	NotFound,

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
