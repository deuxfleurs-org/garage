use err_derive::Error;
use hyper::header::HeaderValue;
use hyper::{Body, HeaderMap, StatusCode};

pub use garage_model::helper::error::Error as HelperError;

use crate::common_error::CommonError;
pub use crate::common_error::{CommonErrorDerivative, OkOrBadRequest, OkOrInternalError};
use crate::generic_server::ApiError;
use crate::helpers::CustomApiErrorBody;

/// Errors of this crate
#[derive(Debug, Error)]
pub enum Error {
	#[error(display = "{}", _0)]
	/// Error from common error
	CommonError(CommonError),

	// Category: cannot process
	/// The API access key does not exist
	#[error(display = "Access key not found: {}", _0)]
	NoSuchAccessKey(String),
}

impl<T> From<T> for Error
where
	CommonError: From<T>,
{
	fn from(err: T) -> Self {
		Error::CommonError(CommonError::from(err))
	}
}

impl CommonErrorDerivative for Error {}

impl From<HelperError> for Error {
	fn from(err: HelperError) -> Self {
		match err {
			HelperError::Internal(i) => Self::CommonError(CommonError::InternalError(i)),
			HelperError::BadRequest(b) => Self::CommonError(CommonError::BadRequest(b)),
			HelperError::InvalidBucketName(n) => {
				Self::CommonError(CommonError::InvalidBucketName(n))
			}
			HelperError::NoSuchBucket(n) => Self::CommonError(CommonError::NoSuchBucket(n)),
			HelperError::NoSuchAccessKey(n) => Self::NoSuchAccessKey(n),
		}
	}
}

impl Error {
	fn code(&self) -> &'static str {
		match self {
			Error::CommonError(c) => c.aws_code(),
			Error::NoSuchAccessKey(_) => "NoSuchAccessKey",
		}
	}
}

impl ApiError for Error {
	/// Get the HTTP status code that best represents the meaning of the error for the client
	fn http_status_code(&self) -> StatusCode {
		match self {
			Error::CommonError(c) => c.http_status_code(),
			Error::NoSuchAccessKey(_) => StatusCode::NOT_FOUND,
		}
	}

	fn add_http_headers(&self, _header_map: &mut HeaderMap<HeaderValue>) {
		// nothing
	}

	fn http_body(&self, garage_region: &str, path: &str) -> Body {
		let error = CustomApiErrorBody {
			code: self.code().to_string(),
			message: format!("{}", self),
			path: path.to_string(),
			region: garage_region.to_string(),
		};
		Body::from(serde_json::to_string_pretty(&error).unwrap_or_else(|_| {
			r#"
{
	"code": "InternalError",
	"message": "JSON encoding of error failed"
}
			"#
			.into()
		}))
	}
}
