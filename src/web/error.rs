use err_derive::Error;
use hyper::StatusCode;

use garage_util::error::Error as GarageError;

#[derive(Debug, Error)]
pub enum Error {
	// Category: internal error
	#[error(display = "Internal error: {}", _0)]
	InternalError(#[error(source)] GarageError),

	#[error(display = "Internal error (Hyper error): {}", _0)]
	Hyper(#[error(source)] hyper::Error),

	#[error(display = "Internal error (HTTP error): {}", _0)]
	HTTP(#[error(source)] http::Error),

	// Category: cannot process
	#[error(display = "Forbidden: {}", _0)]
	Forbidden(String),

	#[error(display = "Not found")]
	NotFound,

	// Category: bad request
	#[error(display = "Invalid UTF-8: {}", _0)]
	InvalidUTF8(#[error(source)] std::str::Utf8Error),

	#[error(display = "Invalid XML: {}", _0)]
	InvalidXML(#[error(source)] roxmltree::Error),

	#[error(display = "Invalid header value: {}", _0)]
	InvalidHeader(#[error(source)] hyper::header::ToStrError),

	#[error(display = "Bad request: {}", _0)]
	BadRequest(String),
}

impl Error {
	pub fn http_status_code(&self) -> StatusCode {
		match self {
			Error::NotFound => StatusCode::NOT_FOUND,
			Error::Forbidden(_) => StatusCode::FORBIDDEN,
			Error::InternalError(GarageError::RPC(_)) => StatusCode::SERVICE_UNAVAILABLE,
			Error::InternalError(_) | Error::Hyper(_) | Error::HTTP(_) => {
				StatusCode::INTERNAL_SERVER_ERROR
			}
			_ => StatusCode::BAD_REQUEST,
		}
	}
}
