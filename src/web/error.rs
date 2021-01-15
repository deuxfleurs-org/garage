use err_derive::Error;
use hyper::StatusCode;

use garage_util::error::Error as GarageError;

#[derive(Debug, Error)]
pub enum Error {
	#[error(display = "API error: {}", _0)]
	ApiError(#[error(source)] garage_api::error::Error),

	// Category: internal error
	#[error(display = "Internal error: {}", _0)]
	InternalError(#[error(source)] GarageError),

	#[error(display = "Not found")]
	NotFound,

	// Category: bad request
	#[error(display = "Invalid UTF-8: {}", _0)]
	InvalidUTF8(#[error(source)] std::str::Utf8Error),

	#[error(display = "Invalid header value: {}", _0)]
	InvalidHeader(#[error(source)] hyper::header::ToStrError),

	#[error(display = "Bad request: {}", _0)]
	BadRequest(String),
}

impl Error {
	pub fn http_status_code(&self) -> StatusCode {
		match self {
			Error::NotFound => StatusCode::NOT_FOUND,
			Error::ApiError(e) => e.http_status_code(),
			Error::InternalError(GarageError::RPC(_)) => StatusCode::SERVICE_UNAVAILABLE,
			Error::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
			_ => StatusCode::BAD_REQUEST,
		}
	}
}
