use std::convert::TryFrom;

use err_derive::Error;
use hyper::header::HeaderValue;
use hyper::{HeaderMap, StatusCode};

pub use garage_model::helper::error::Error as HelperError;

use garage_api_common::common_error::{commonErrorDerivative, CommonError};
pub use garage_api_common::common_error::{
	CommonErrorDerivative, OkOrBadRequest, OkOrInternalError,
};
use garage_api_common::generic_server::ApiError;
use garage_api_common::helpers::*;

/// Errors of this crate
#[derive(Debug, Error)]
pub enum Error {
	#[error(display = "{}", _0)]
	/// Error from common error
	Common(#[error(source)] CommonError),

	// Category: cannot process
	/// The API access key does not exist
	#[error(display = "Access key not found: {}", _0)]
	NoSuchAccessKey(String),

	/// In Import key, the key already exists
	#[error(
		display = "Key {} already exists in data store. Even if it is deleted, we can't let you create a new key with the same ID. Sorry.",
		_0
	)]
	KeyAlreadyExists(String),
}

commonErrorDerivative!(Error);

/// FIXME: helper errors are transformed into their corresponding variants
/// in the Error struct, but in many case a helper error should be considered
/// an internal error.
impl From<HelperError> for Error {
	fn from(err: HelperError) -> Error {
		match CommonError::try_from(err) {
			Ok(ce) => Self::Common(ce),
			Err(HelperError::NoSuchAccessKey(k)) => Self::NoSuchAccessKey(k),
			Err(_) => unreachable!(),
		}
	}
}

impl Error {
	fn code(&self) -> &'static str {
		match self {
			Error::Common(c) => c.aws_code(),
			Error::NoSuchAccessKey(_) => "NoSuchAccessKey",
			Error::KeyAlreadyExists(_) => "KeyAlreadyExists",
		}
	}
}

impl ApiError for Error {
	/// Get the HTTP status code that best represents the meaning of the error for the client
	fn http_status_code(&self) -> StatusCode {
		match self {
			Error::Common(c) => c.http_status_code(),
			Error::NoSuchAccessKey(_) => StatusCode::NOT_FOUND,
			Error::KeyAlreadyExists(_) => StatusCode::CONFLICT,
		}
	}

	fn add_http_headers(&self, header_map: &mut HeaderMap<HeaderValue>) {
		use hyper::header;
		header_map.append(header::CONTENT_TYPE, "application/json".parse().unwrap());
	}

	fn http_body(&self, garage_region: &str, path: &str) -> ErrorBody {
		let error = CustomApiErrorBody {
			code: self.code().to_string(),
			message: format!("{}", self),
			path: path.to_string(),
			region: garage_region.to_string(),
		};
		let error_str = serde_json::to_string_pretty(&error).unwrap_or_else(|_| {
			r#"
{
	"code": "InternalError",
	"message": "JSON encoding of error failed"
}
			"#
			.into()
		});
		error_body(error_str)
	}
}
