use err_derive::Error;
use hyper::header::HeaderValue;
use hyper::{Body, HeaderMap, StatusCode};

use garage_model::helper::error::Error as HelperError;
use garage_util::error::Error as GarageError;

use crate::generic_server::ApiError;
pub use crate::common_error::*;

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

	/// The API access key does not exist
	#[error(display = "Access key not found")]
	NoSuchAccessKey,

	/// The bucket requested don't exists
	#[error(display = "Bucket not found")]
	NoSuchBucket,

	/// Tried to create a bucket that already exist
	#[error(display = "Bucket already exists")]
	BucketAlreadyExists,

	/// Tried to delete a non-empty bucket
	#[error(display = "Tried to delete a non-empty bucket")]
	BucketNotEmpty,

	// Category: bad request
	/// Bucket name is not valid according to AWS S3 specs
	#[error(display = "Invalid bucket name")]
	InvalidBucketName,

	/// The client sent a request for an action not supported by garage
	#[error(display = "Unimplemented action: {}", _0)]
	NotImplemented(String),
}

impl<T> From<T> for Error
where CommonError: From<T> {
	fn from(err: T) -> Self {
		Error::CommonError(CommonError::from(err))
	}
}

impl From<HelperError> for Error {
	fn from(err: HelperError) -> Self {
		match err {
			HelperError::Internal(i) => Self::CommonError(CommonError::InternalError(i)),
			HelperError::BadRequest(b) => Self::CommonError(CommonError::BadRequest(b)),
			HelperError::InvalidBucketName(_) => Self::InvalidBucketName,
			HelperError::NoSuchAccessKey(_) => Self::NoSuchAccessKey,
			HelperError::NoSuchBucket(_) => Self::NoSuchBucket,
		}
	}
}

impl ApiError for Error {
	/// Get the HTTP status code that best represents the meaning of the error for the client
	fn http_status_code(&self) -> StatusCode {
		match self {
			Error::CommonError(c) => c.http_status_code(),
			Error::NoSuchAccessKey | Error::NoSuchBucket => StatusCode::NOT_FOUND,
			Error::BucketNotEmpty | Error::BucketAlreadyExists => StatusCode::CONFLICT,
			Error::Forbidden(_) => StatusCode::FORBIDDEN,
			Error::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
			Error::InvalidBucketName => StatusCode::BAD_REQUEST,
		}
	}

	fn add_http_headers(&self, _header_map: &mut HeaderMap<HeaderValue>) {
		// nothing
	}

	fn http_body(&self, garage_region: &str, path: &str) -> Body {
		Body::from(format!("ERROR: {}\n\ngarage region: {}\npath: {}", self, garage_region, path))
	}
}

impl Error {
	pub fn bad_request<M: ToString>(msg: M) -> Self {
		Self::CommonError(CommonError::BadRequest(msg.to_string()))
	}
}
