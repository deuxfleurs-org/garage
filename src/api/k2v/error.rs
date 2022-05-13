use err_derive::Error;
use hyper::header::HeaderValue;
use hyper::{Body, HeaderMap, StatusCode};

use garage_model::helper::error::Error as HelperError;

use crate::common_error::CommonError;
pub use crate::common_error::{OkOrBadRequest, OkOrInternalError};
use crate::generic_server::ApiError;
use crate::signature::error::Error as SignatureError;

/// Errors of this crate
#[derive(Debug, Error)]
pub enum Error {
	#[error(display = "{}", _0)]
	/// Error from common error
	CommonError(CommonError),

	// Category: cannot process
	/// No proper api key was used, or the signature was invalid
	#[error(display = "Forbidden: {}", _0)]
	Forbidden(String),

	/// Authorization Header Malformed
	#[error(display = "Authorization header malformed, expected scope: {}", _0)]
	AuthorizationHeaderMalformed(String),

	/// The object requested don't exists
	#[error(display = "Key not found")]
	NoSuchKey,

	/// The bucket requested don't exists
	#[error(display = "Bucket not found")]
	NoSuchBucket,

	/// Some base64 encoded data was badly encoded
	#[error(display = "Invalid base64: {}", _0)]
	InvalidBase64(#[error(source)] base64::DecodeError),

	/// The client sent a header with invalid value
	#[error(display = "Invalid header value: {}", _0)]
	InvalidHeader(#[error(source)] hyper::header::ToStrError),

	/// The client asked for an invalid return format (invalid Accept header)
	#[error(display = "Not acceptable: {}", _0)]
	NotAcceptable(String),

	/// The request contained an invalid UTF-8 sequence in its path or in other parameters
	#[error(display = "Invalid UTF-8: {}", _0)]
	InvalidUtf8Str(#[error(source)] std::str::Utf8Error),
}

impl<T> From<T> for Error
where
	CommonError: From<T>,
{
	fn from(err: T) -> Self {
		Error::CommonError(CommonError::from(err))
	}
}

impl From<HelperError> for Error {
	fn from(err: HelperError) -> Self {
		match err {
			HelperError::Internal(i) => Self::CommonError(CommonError::InternalError(i)),
			HelperError::BadRequest(b) => Self::CommonError(CommonError::BadRequest(b)),
			e => Self::CommonError(CommonError::BadRequest(format!("{}", e))),
		}
	}
}

impl From<SignatureError> for Error {
	fn from(err: SignatureError) -> Self {
		match err {
			SignatureError::CommonError(c) => Self::CommonError(c),
			SignatureError::AuthorizationHeaderMalformed(c) => Self::AuthorizationHeaderMalformed(c),
			SignatureError::Forbidden(f) => Self::Forbidden(f),
			SignatureError::InvalidUtf8Str(i) => Self::InvalidUtf8Str(i),
			SignatureError::InvalidHeader(h) => Self::InvalidHeader(h),
		}
	}
}

impl Error {
	//pub fn internal_error<M: ToString>(msg: M) -> Self {
	//	Self::CommonError(CommonError::InternalError(GarageError::Message(
	//		msg.to_string(),
	//	)))
	//}

	pub fn bad_request<M: ToString>(msg: M) -> Self {
		Self::CommonError(CommonError::BadRequest(msg.to_string()))
	}
}

impl ApiError for Error {
	/// Get the HTTP status code that best represents the meaning of the error for the client
	fn http_status_code(&self) -> StatusCode {
		match self {
			Error::CommonError(c) => c.http_status_code(),
			Error::NoSuchKey | Error::NoSuchBucket => StatusCode::NOT_FOUND,
			Error::Forbidden(_) => StatusCode::FORBIDDEN,
			Error::NotAcceptable(_) => StatusCode::NOT_ACCEPTABLE,
			_ => StatusCode::BAD_REQUEST,
		}
	}

	fn add_http_headers(&self, _header_map: &mut HeaderMap<HeaderValue>) {
		// nothing
	}

	fn http_body(&self, garage_region: &str, path: &str) -> Body {
		Body::from(format!(
			"ERROR: {}\n\ngarage region: {}\npath: {}",
			self, garage_region, path
		))
	}
}
