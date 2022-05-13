use std::convert::TryInto;

use err_derive::Error;
use hyper::header::HeaderValue;
use hyper::{Body, HeaderMap, StatusCode};

use garage_model::helper::error::Error as HelperError;
use garage_util::error::Error as GarageError;

use crate::common_error::CommonError;
pub use crate::common_error::{OkOrBadRequest, OkOrInternalError};
use crate::generic_server::ApiError;
use crate::s3::xml as s3_xml;
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

	/// Parts specified in CMU request do not match parts actually uploaded
	#[error(display = "Parts given to CompleteMultipartUpload do not match uploaded parts")]
	InvalidPart,

	/// Parts given to CompleteMultipartUpload were not in ascending order
	#[error(display = "Parts given to CompleteMultipartUpload were not in ascending order")]
	InvalidPartOrder,

	/// In CompleteMultipartUpload: not enough data
	/// (here we are more lenient than AWS S3)
	#[error(display = "Proposed upload is smaller than the minimum allowed object size")]
	EntityTooSmall,

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

	/// Bucket name is not valid according to AWS S3 specs
	#[error(display = "Invalid bucket name")]
	InvalidBucketName,

	/// The client sent invalid XML data
	#[error(display = "Invalid XML: {}", _0)]
	InvalidXml(String),

	/// The client sent a header with invalid value
	#[error(display = "Invalid header value: {}", _0)]
	InvalidHeader(#[error(source)] hyper::header::ToStrError),

	/// The client sent a range header with invalid value
	#[error(display = "Invalid HTTP range: {:?}", _0)]
	InvalidRange(#[error(from)] (http_range::HttpRangeParseError, u64)),

	/// The client asked for an invalid return format (invalid Accept header)
	#[error(display = "Not acceptable: {}", _0)]
	NotAcceptable(String),

	/// The client sent a request for an action not supported by garage
	#[error(display = "Unimplemented action: {}", _0)]
	NotImplemented(String),
}

impl<T> From<T> for Error
where
	CommonError: From<T>,
{
	fn from(err: T) -> Self {
		Error::CommonError(CommonError::from(err))
	}
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

impl From<multer::Error> for Error {
	fn from(err: multer::Error) -> Self {
		Self::bad_request(err)
	}
}

impl Error {
	pub fn aws_code(&self) -> &'static str {
		match self {
			Error::NoSuchKey => "NoSuchKey",
			Error::NoSuchBucket => "NoSuchBucket",
			Error::NoSuchUpload => "NoSuchUpload",
			Error::BucketAlreadyExists => "BucketAlreadyExists",
			Error::BucketNotEmpty => "BucketNotEmpty",
			Error::PreconditionFailed => "PreconditionFailed",
			Error::InvalidPart => "InvalidPart",
			Error::InvalidPartOrder => "InvalidPartOrder",
			Error::EntityTooSmall => "EntityTooSmall",
			Error::Forbidden(_) => "AccessDenied",
			Error::AuthorizationHeaderMalformed(_) => "AuthorizationHeaderMalformed",
			Error::NotImplemented(_) => "NotImplemented",
			Error::CommonError(CommonError::InternalError(
				GarageError::Timeout
				| GarageError::RemoteError(_)
				| GarageError::Quorum(_, _, _, _),
			)) => "ServiceUnavailable",
			Error::CommonError(
				CommonError::InternalError(_) | CommonError::Hyper(_) | CommonError::Http(_),
			) => "InternalError",
			_ => "InvalidRequest",
		}
	}

	pub fn internal_error<M: ToString>(msg: M) -> Self {
		Self::CommonError(CommonError::InternalError(GarageError::Message(
			msg.to_string(),
		)))
	}

	pub fn bad_request<M: ToString>(msg: M) -> Self {
		Self::CommonError(CommonError::BadRequest(msg.to_string()))
	}
}

impl ApiError for Error {
	/// Get the HTTP status code that best represents the meaning of the error for the client
	fn http_status_code(&self) -> StatusCode {
		match self {
			Error::CommonError(c) => c.http_status_code(),
			Error::NoSuchKey | Error::NoSuchBucket | Error::NoSuchUpload => StatusCode::NOT_FOUND,
			Error::BucketNotEmpty | Error::BucketAlreadyExists => StatusCode::CONFLICT,
			Error::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
			Error::Forbidden(_) => StatusCode::FORBIDDEN,
			Error::NotAcceptable(_) => StatusCode::NOT_ACCEPTABLE,
			Error::InvalidRange(_) => StatusCode::RANGE_NOT_SATISFIABLE,
			Error::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
			_ => StatusCode::BAD_REQUEST,
		}
	}

	fn add_http_headers(&self, header_map: &mut HeaderMap<HeaderValue>) {
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

	fn http_body(&self, garage_region: &str, path: &str) -> Body {
		let error = s3_xml::Error {
			code: s3_xml::Value(self.aws_code().to_string()),
			message: s3_xml::Value(format!("{}", self)),
			resource: Some(s3_xml::Value(path.to_string())),
			region: Some(s3_xml::Value(garage_region.to_string())),
		};
		Body::from(s3_xml::to_xml_with_header(&error).unwrap_or_else(|_| {
			r#"
<?xml version="1.0" encoding="UTF-8"?>
<Error>
	<Code>InternalError</Code>
	<Message>XML encoding of error failed</Message>
</Error>
			"#
			.into()
		}))
	}
}
