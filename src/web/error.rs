use err_derive::Error;
use hyper::header::HeaderValue;
use hyper::{HeaderMap, StatusCode};

use garage_api_common::generic_server::ApiError;

/// Errors of this crate
#[derive(Debug, Error)]
pub enum Error {
	/// An error received from the API crate
	#[error(display = "API error: {}", _0)]
	ApiError(garage_api_s3::error::Error),

	/// The file does not exist
	#[error(display = "Not found")]
	NotFound,

	/// The client sent a request without host, or with unsupported method
	#[error(display = "Bad request: {}", _0)]
	BadRequest(String),
}

impl<T> From<T> for Error
where
	garage_api_s3::error::Error: From<T>,
{
	fn from(err: T) -> Self {
		Error::ApiError(garage_api_s3::error::Error::from(err))
	}
}

impl Error {
	/// Transform errors into http status code
	pub fn http_status_code(&self) -> StatusCode {
		match self {
			Error::NotFound => StatusCode::NOT_FOUND,
			Error::ApiError(e) => e.http_status_code(),
			Error::BadRequest(_) => StatusCode::BAD_REQUEST,
		}
	}

	pub fn add_headers(&self, header_map: &mut HeaderMap<HeaderValue>) {
		#[allow(clippy::single_match)]
		match self {
			Error::ApiError(e) => e.add_http_headers(header_map),
			_ => (),
		}
	}
}
