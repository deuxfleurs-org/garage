use err_derive::Error;

use crate::common_error::CommonError;
pub use crate::common_error::{CommonErrorDerivative, OkOrBadRequest, OkOrInternalError};

/// Errors of this crate
#[derive(Debug, Error)]
pub enum Error {
	#[error(display = "{}", _0)]
	/// Error from common error
	Common(CommonError),

	/// Authorization Header Malformed
	#[error(display = "Authorization header malformed, unexpected scope: {}", _0)]
	AuthorizationHeaderMalformed(String),

	// Category: bad request
	/// The request contained an invalid UTF-8 sequence in its path or in other parameters
	#[error(display = "Invalid UTF-8: {}", _0)]
	InvalidUtf8Str(#[error(source)] std::str::Utf8Error),

	/// The provided digest (checksum) value was invalid
	#[error(display = "Invalid digest: {}", _0)]
	InvalidDigest(String),
}

impl<T> From<T> for Error
where
	CommonError: From<T>,
{
	fn from(err: T) -> Self {
		Error::Common(CommonError::from(err))
	}
}

impl CommonErrorDerivative for Error {}
