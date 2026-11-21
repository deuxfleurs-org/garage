use std::convert::TryFrom;

use hyper::StatusCode;
use thiserror::Error;

use garage_util::error::Error as GarageError;

use garage_model::helper::error::Error as HelperError;

/// Errors of this crate
#[derive(Debug, Error)]
pub enum CommonError {
	// ---- INTERNAL ERRORS ----
	/// Error related to deeper parts of Garage
	#[error("Internal error: {0}")]
	InternalError(#[from] GarageError),

	/// Error related to Hyper
	#[error("Internal error (Hyper error): {0}")]
	Hyper(#[from] hyper::Error),

	/// Error related to HTTP
	#[error("Internal error (HTTP error): {0}")]
	Http(#[from] http::Error),

	// ---- GENERIC CLIENT ERRORS ----
	/// Proper authentication was not provided
	#[error("Forbidden: {0}")]
	Forbidden(String),

	/// Anonymous access denied
	#[error("Anonymous access denied")]
	AnonymousAccessDenied,

	/// Generic bad request response with custom message
	#[error("Bad request: {0}")]
	BadRequest(String),

	/// The client sent a header with invalid value
	#[error("Invalid header value: {0}")]
	InvalidHeader(#[from] hyper::header::ToStrError),

	/// The client sent a request for an action not supported by garage
	#[error("Unimplemented action: {0}")]
	NotImplemented(String),

	// ---- SPECIFIC ERROR CONDITIONS ----
	// These have to be error codes referenced in the S3 spec here:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
	/// The bucket requested don't exists
	#[error("Bucket not found: {0}")]
	NoSuchBucket(String),

	/// Tried to create a bucket that already exist
	#[error("Bucket already exists")]
	BucketAlreadyExists,

	/// Tried to delete a non-empty bucket
	#[error("Tried to delete a non-empty bucket")]
	BucketNotEmpty,

	// Category: bad request
	/// Bucket name is not valid according to AWS S3 specs
	#[error("Invalid bucket name: {0}")]
	InvalidBucketName(String),

	/// Tried to create bucket that is already owned by you
	#[error("Bucket already owned by you")]
	BucketAlreadyOwnedByYou,

	/// Tag is invalid
	#[error("Invalid tag: {0}")]
	InvalidTag(String),
}

#[macro_export]
macro_rules! commonErrorDerivative {
	( $error_struct: ident ) => {
		impl From<garage_util::error::Error> for $error_struct {
			fn from(err: garage_util::error::Error) -> Self {
				Self::Common(CommonError::InternalError(err))
			}
		}
		impl From<http::Error> for $error_struct {
			fn from(err: http::Error) -> Self {
				Self::Common(CommonError::Http(err))
			}
		}
		impl From<hyper::Error> for $error_struct {
			fn from(err: hyper::Error) -> Self {
				Self::Common(CommonError::Hyper(err))
			}
		}
		impl From<hyper::header::ToStrError> for $error_struct {
			fn from(err: hyper::header::ToStrError) -> Self {
				Self::Common(CommonError::InvalidHeader(err))
			}
		}
		impl CommonErrorDerivative for $error_struct {}
	};
}

pub use commonErrorDerivative;

impl CommonError {
	pub fn http_status_code(&self) -> StatusCode {
		match self {
			CommonError::InternalError(
				GarageError::Timeout | GarageError::RemoteError(_) | GarageError::Quorum(..),
			) => StatusCode::SERVICE_UNAVAILABLE,
			CommonError::InternalError(_) | CommonError::Hyper(_) | CommonError::Http(_) => {
				StatusCode::INTERNAL_SERVER_ERROR
			}
			CommonError::BadRequest(_) => StatusCode::BAD_REQUEST,
			CommonError::AnonymousAccessDenied | CommonError::Forbidden(_) => StatusCode::FORBIDDEN,
			CommonError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
			CommonError::NoSuchBucket(_) => StatusCode::NOT_FOUND,
			CommonError::BucketNotEmpty
			| CommonError::BucketAlreadyExists
			| CommonError::BucketAlreadyOwnedByYou => StatusCode::CONFLICT,
			CommonError::InvalidTag(_)
			| CommonError::InvalidBucketName(_)
			| CommonError::InvalidHeader(_) => StatusCode::BAD_REQUEST,
		}
	}

	pub fn aws_code(&self) -> &'static str {
		match self {
			CommonError::Forbidden(_) | CommonError::AnonymousAccessDenied => "AccessDenied",
			CommonError::InternalError(
				GarageError::Timeout | GarageError::RemoteError(_) | GarageError::Quorum(..),
			) => "ServiceUnavailable",
			CommonError::InternalError(_) | CommonError::Hyper(_) | CommonError::Http(_) => {
				"InternalError"
			}
			CommonError::BadRequest(_) => "InvalidRequest",
			CommonError::NoSuchBucket(_) => "NoSuchBucket",
			CommonError::BucketAlreadyExists => "BucketAlreadyExists",
			CommonError::BucketNotEmpty => "BucketNotEmpty",
			CommonError::InvalidBucketName(_) => "InvalidBucketName",
			CommonError::InvalidHeader(_) => "InvalidHeaderValue",
			CommonError::BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
			CommonError::NotImplemented(_) => "NotImplemented",
			CommonError::InvalidTag(_) => "InvalidTag",
		}
	}

	pub fn bad_request<M: ToString>(msg: M) -> Self {
		CommonError::BadRequest(msg.to_string())
	}
}

impl TryFrom<HelperError> for CommonError {
	type Error = HelperError;

	fn try_from(err: HelperError) -> Result<Self, HelperError> {
		match err {
			HelperError::Internal(i) => Ok(Self::InternalError(i)),
			HelperError::BadRequest(b) => Ok(Self::BadRequest(b)),
			HelperError::InvalidBucketName(n) => Ok(Self::InvalidBucketName(n)),
			HelperError::NoSuchBucket(n) => Ok(Self::NoSuchBucket(n)),
			e => Err(e),
		}
	}
}

/// This function converts `HelperErrors` into `CommonErrors`,
/// for variants that exist in `CommonError`.
/// This is used for helper functions that might return `InvalidBucketName`
/// or `NoSuchBucket` for instance, and we want to pass that error
/// up to our caller.
pub fn pass_helper_error(err: HelperError) -> CommonError {
	match CommonError::try_from(err) {
		Ok(e) => e,
		Err(e) => panic!("Helper error `{}` should hot have happened here", e),
	}
}

pub fn helper_error_as_internal(err: HelperError) -> CommonError {
	match err {
		HelperError::Internal(e) => CommonError::InternalError(e),
		e => CommonError::InternalError(GarageError::Message(e.to_string())),
	}
}

pub trait CommonErrorDerivative: From<CommonError> {
	fn internal_error<M: ToString>(msg: M) -> Self {
		Self::from(CommonError::InternalError(GarageError::Message(
			msg.to_string(),
		)))
	}

	fn bad_request<M: ToString>(msg: M) -> Self {
		Self::from(CommonError::BadRequest(msg.to_string()))
	}

	fn forbidden<M: ToString>(msg: M) -> Self {
		Self::from(CommonError::Forbidden(msg.to_string()))
	}
}

/// Trait to map error to the Bad Request error code
pub trait OkOrBadRequest {
	type S;
	fn ok_or_bad_request<M: AsRef<str>>(self, reason: M) -> Result<Self::S, CommonError>;
}

impl<T, E> OkOrBadRequest for Result<T, E>
where
	E: std::fmt::Display,
{
	type S = T;
	fn ok_or_bad_request<M: AsRef<str>>(self, reason: M) -> Result<T, CommonError> {
		match self {
			Ok(x) => Ok(x),
			Err(e) => Err(CommonError::BadRequest(format!(
				"{}: {}",
				reason.as_ref(),
				e
			))),
		}
	}
}

impl<T> OkOrBadRequest for Option<T> {
	type S = T;
	fn ok_or_bad_request<M: AsRef<str>>(self, reason: M) -> Result<T, CommonError> {
		match self {
			Some(x) => Ok(x),
			None => Err(CommonError::BadRequest(reason.as_ref().to_string())),
		}
	}
}

/// Trait to map an error to an Internal Error code
pub trait OkOrInternalError {
	type S;
	fn ok_or_internal_error<M: AsRef<str>>(self, reason: M) -> Result<Self::S, CommonError>;
}

impl<T, E> OkOrInternalError for Result<T, E>
where
	E: std::fmt::Display,
{
	type S = T;
	fn ok_or_internal_error<M: AsRef<str>>(self, reason: M) -> Result<T, CommonError> {
		match self {
			Ok(x) => Ok(x),
			Err(e) => Err(CommonError::InternalError(GarageError::Message(format!(
				"{}: {}",
				reason.as_ref(),
				e
			)))),
		}
	}
}

impl<T> OkOrInternalError for Option<T> {
	type S = T;
	fn ok_or_internal_error<M: AsRef<str>>(self, reason: M) -> Result<T, CommonError> {
		match self {
			Some(x) => Ok(x),
			None => Err(CommonError::InternalError(GarageError::Message(
				reason.as_ref().to_string(),
			))),
		}
	}
}
