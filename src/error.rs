use err_derive::Error;
use std::io;

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

	#[error(display = "{}", _0)]
	BadRequest(String),

	#[error(display = "{}", _0)]
	Message(String),
}
