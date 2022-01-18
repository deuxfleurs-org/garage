use std::convert::TryInto;

use err_derive::Error;
use hyper::header::HeaderValue;
use hyper::{HeaderMap, StatusCode};

use garage_model::helper::error::Error as HelperError;
use garage_util::error::Error as GarageError;

use crate::s3_xml;

/// Errors of this crate
#[derive(Debug, Error)]
pub enum Error {
	// Category: internal error
	/// Error related to deeper parts of Garage
	#[error(display = "Internal error: {}", _0)]
	InternalError(#[error(source)] GarageError),

	/// Error related to Hyper
	#[error(display = "Internal error (Hyper error): {}", _0)]
	Hyper(#[error(source)] hyper::Error),

	/// Error related to HTTP
	#[error(display = "Internal error (HTTP error): {}", _0)]
	Http(#[error(source)] http::Error),

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

	/// The multipart upload requested don't exists
	#[error(display = "Upload not found")]
	NoSuchUpload,

	/// Tried to create a bucket that already exist
	#[error(display = "Bucket already exists")]
	BucketAlreadyExists,

	/// Tried to delete a non-empty bucket
	#[error(display = "Tried to delete a non-empty bucket")]
	BucketNotEmpty,

	/// Precondition failed (e.g. x-amz-copy-source-if-match)
	#[error(display = "At least one of the preconditions you specified did not hold")]
	PreconditionFailed,

	// Category: bad request
	/// The request contained an invalid UTF-8 sequence in its path or in other parameters
	#[error(display = "Invalid UTF-8: {}", _0)]
	InvalidUtf8Str(#[error(source)] std::str::Utf8Error),

	/// The request used an invalid path
	#[error(display = "Invalid UTF-8: {}", _0)]
	InvalidUtf8String(#[error(source)] std::string::FromUtf8Error),

	/// Some base64 encoded data was badly encoded
	#[error(display = "Invalid base64: {}", _0)]
	InvalidBase64(#[error(source)] base64::DecodeError),

	/// The client sent invalid XML data
	#[error(display = "Invalid XML: {}", _0)]
	InvalidXml(String),

	/// The client sent a header with invalid value
	#[error(display = "Invalid header value: {}", _0)]
	InvalidHeader(#[error(source)] hyper::header::ToStrError),

	/// The client sent a range header with invalid value
	#[error(display = "Invalid HTTP range: {:?}", _0)]
	InvalidRange(#[error(from)] (http_range::HttpRangeParseError, u64)),

	/// The client sent an invalid request
	#[error(display = "Bad request: {}", _0)]
	BadRequest(String),

	/// The client sent a request for an action not supported by garage
	#[error(display = "Unimplemented action: {}", _0)]
	NotImplemented(String),
}

impl From<roxmltree::Error> for Error {
	fn from(err: roxmltree::Error) -> Self {
		Self::InvalidXml(format!("{}", err))
	}
}

impl From<quick_xml::de::DeError> for Error {
	fn from(err: quick_xml::de::DeError) -> Self {
		Self::InvalidXml(format!("{}", err))
	}
}

impl From<HelperError> for Error {
	fn from(err: HelperError) -> Self {
		match err {
			HelperError::Internal(i) => Self::InternalError(i),
			HelperError::BadRequest(b) => Self::BadRequest(b),
		}
	}
}

impl Error {
	/// Get the HTTP status code that best represents the meaning of the error for the client
	pub fn http_status_code(&self) -> StatusCode {
		match self {
			Error::NoSuchKey | Error::NoSuchBucket | Error::NoSuchUpload => StatusCode::NOT_FOUND,
			Error::BucketNotEmpty | Error::BucketAlreadyExists => StatusCode::CONFLICT,
			Error::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
			Error::Forbidden(_) => StatusCode::FORBIDDEN,
			Error::InternalError(
				GarageError::Timeout
				| GarageError::RemoteError(_)
				| GarageError::Quorum(_, _, _, _),
			) => StatusCode::SERVICE_UNAVAILABLE,
			Error::InternalError(_) | Error::Hyper(_) | Error::Http(_) => {
				StatusCode::INTERNAL_SERVER_ERROR
			}
			Error::InvalidRange(_) => StatusCode::RANGE_NOT_SATISFIABLE,
			Error::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
			_ => StatusCode::BAD_REQUEST,
		}
	}

	pub fn aws_code(&self) -> &'static str {
		match self {
			Error::NoSuchKey => "NoSuchKey",
			Error::NoSuchBucket => "NoSuchBucket",
			Error::NoSuchUpload => "NoSuchUpload",
			Error::BucketAlreadyExists => "BucketAlreadyExists",
			Error::BucketNotEmpty => "BucketNotEmpty",
			Error::PreconditionFailed => "PreconditionFailed",
			Error::Forbidden(_) => "AccessDenied",
			Error::AuthorizationHeaderMalformed(_) => "AuthorizationHeaderMalformed",
			Error::NotImplemented(_) => "NotImplemented",
			Error::InternalError(
				GarageError::Timeout
				| GarageError::RemoteError(_)
				| GarageError::Quorum(_, _, _, _),
			) => "ServiceUnavailable",
			Error::InternalError(_) | Error::Hyper(_) | Error::Http(_) => "InternalError",
			_ => "InvalidRequest",
		}
	}

	pub fn aws_xml(&self, garage_region: &str, path: &str) -> String {
		let error = s3_xml::Error {
			code: s3_xml::Value(self.aws_code().to_string()),
			message: s3_xml::Value(format!("{}", self)),
			resource: Some(s3_xml::Value(path.to_string())),
			region: Some(s3_xml::Value(garage_region.to_string())),
		};
		s3_xml::to_xml_with_header(&error).unwrap_or_else(|_| {
			r#"
<?xml version="1.0" encoding="UTF-8"?>
<Error>
	<Code>InternalError</Code>
	<Message>XML encoding of error failed</Message>
</Error>
			"#
			.into()
		})
	}

	pub fn add_headers(&self, header_map: &mut HeaderMap<HeaderValue>) {
		use hyper::header;
		#[allow(clippy::single_match)]
		match self {
			Error::InvalidRange((_, len)) => {
				header_map.append(
					header::CONTENT_RANGE,
					format!("bytes */{}", len)
						.try_into()
						.expect("header value only contain ascii"),
				);
			}
			_ => (),
		}
	}
}

/// Trait to map error to the Bad Request error code
pub trait OkOrBadRequest {
	type S;
	fn ok_or_bad_request<M: AsRef<str>>(self, reason: M) -> Result<Self::S, Error>;
}

impl<T, E> OkOrBadRequest for Result<T, E>
where
	E: std::fmt::Display,
{
	type S = T;
	fn ok_or_bad_request<M: AsRef<str>>(self, reason: M) -> Result<T, Error> {
		match self {
			Ok(x) => Ok(x),
			Err(e) => Err(Error::BadRequest(format!("{}: {}", reason.as_ref(), e))),
		}
	}
}

impl<T> OkOrBadRequest for Option<T> {
	type S = T;
	fn ok_or_bad_request<M: AsRef<str>>(self, reason: M) -> Result<T, Error> {
		match self {
			Some(x) => Ok(x),
			None => Err(Error::BadRequest(reason.as_ref().to_string())),
		}
	}
}

/// Trait to map an error to an Internal Error code
pub trait OkOrInternalError {
	type S;
	fn ok_or_internal_error<M: AsRef<str>>(self, reason: M) -> Result<Self::S, Error>;
}

impl<T, E> OkOrInternalError for Result<T, E>
where
	E: std::fmt::Display,
{
	type S = T;
	fn ok_or_internal_error<M: AsRef<str>>(self, reason: M) -> Result<T, Error> {
		match self {
			Ok(x) => Ok(x),
			Err(e) => Err(Error::InternalError(GarageError::Message(format!(
				"{}: {}",
				reason.as_ref(),
				e
			)))),
		}
	}
}

impl<T> OkOrInternalError for Option<T> {
	type S = T;
	fn ok_or_internal_error<M: AsRef<str>>(self, reason: M) -> Result<T, Error> {
		match self {
			Some(x) => Ok(x),
			None => Err(Error::InternalError(GarageError::Message(
				reason.as_ref().to_string(),
			))),
		}
	}
}
