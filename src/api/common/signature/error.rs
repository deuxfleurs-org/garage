use thiserror::Error;

use crate::common_error::CommonError;
pub use crate::common_error::{CommonErrorDerivative, OkOrBadRequest, OkOrInternalError};

/// Errors of this crate
#[derive(Debug, Error)]
pub enum Error {
	#[error("{0}")]
	/// Error from common error
	Common(CommonError),

	/// Authorization Header Malformed
	#[error(
		"Authorization header malformed, unexpected scope: '{unexpected}', expected: '{expected}'"
	)]
	AuthorizationHeaderMalformed {
		unexpected: String,
		expected: String,
	},

	// Category: bad request
	/// The request contained an invalid UTF-8 sequence in its path or in other parameters
	#[error("Invalid UTF-8: {0}")]
	InvalidUtf8Str(#[from] std::str::Utf8Error),

	/// The provided digest (checksum) value was invalid
	#[error("Invalid digest: {0}")]
	InvalidDigest(String),

	#[error("Bad digest: {0}")]
	BadDigest(String),
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
