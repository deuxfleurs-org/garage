use err_derive::Error;

use garage_util::error::Error as GarageError;

use crate::common_error::CommonError;
pub use crate::common_error::{OkOrBadRequest, OkOrInternalError};

/// Errors of this crate
#[derive(Debug, Error)]
pub enum Error {
	#[error(display = "{}", _0)]
	/// Error from common error
	CommonError(CommonError),

	/// Authorization Header Malformed
	#[error(display = "Authorization header malformed, expected scope: {}", _0)]
	AuthorizationHeaderMalformed(String),

	/// No proper api key was used, or the signature was invalid
	#[error(display = "Forbidden: {}", _0)]
	Forbidden(String),

	// Category: bad request
	/// The request contained an invalid UTF-8 sequence in its path or in other parameters
	#[error(display = "Invalid UTF-8: {}", _0)]
	InvalidUtf8Str(#[error(source)] std::str::Utf8Error),

	/// The client sent a header with invalid value
	#[error(display = "Invalid header value: {}", _0)]
	InvalidHeader(#[error(source)] hyper::header::ToStrError),
}

impl<T> From<T> for Error
where
	CommonError: From<T>,
{
	fn from(err: T) -> Self {
		Error::CommonError(CommonError::from(err))
	}
}


impl Error {
	pub fn internal_error<M: ToString>(msg: M) -> Self {
		Self::CommonError(CommonError::InternalError(GarageError::Message(
			msg.to_string(),
		)))
	}

	pub fn bad_request<M: ToString>(msg: M) -> Self {
		Self::CommonError(CommonError::BadRequest(msg.to_string()))
	}
}

