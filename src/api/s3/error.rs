use std::convert::TryInto;

use hyper::header::HeaderValue;
use hyper::{HeaderMap, StatusCode};
use thiserror::Error;

use garage_model::helper::error::Error as HelperError;

pub(crate) use garage_api_common::common_error::pass_helper_error;

use garage_api_common::common_error::{
	commonErrorDerivative, helper_error_as_internal, CommonError,
};

pub use garage_api_common::common_error::{
	CommonErrorDerivative, OkOrBadRequest, OkOrInternalError,
};

use garage_api_common::generic_server::ApiError;
use garage_api_common::helpers::*;
use garage_api_common::signature::error::Error as SignatureError;

use crate::xml as s3_xml;

/// Errors of this crate
#[derive(Debug, Error)]
pub enum Error {
	#[error("{0}")]
	/// Error from common error
	Common(#[from] CommonError),

	// Category: cannot process
	/// Authorization Header Malformed
	#[error(
		"Authorization header malformed, unexpected scope: '{unexpected}', expected: '{expected}'"
	)]
	AuthorizationHeaderMalformed {
		unexpected: String,
		expected: String,
	},

	/// The object requested don't exists
	#[error("Key not found")]
	NoSuchKey,

	/// The multipart upload requested don't exists
	#[error("Upload not found")]
	NoSuchUpload,

	/// CORS configuration doesn't exist for this bucket
	#[error("The CORS configuration does not exist")]
	NoSuchCORSConfiguration,

	/// CORS configuration doesn't exist for this bucket
	#[error("The lifecycle configuration does not exist")]
	NoSuchLifecycleConfiguration,

	/// Precondition failed (e.g. x-amz-copy-source-if-match)
	#[error("At least one of the preconditions you specified did not hold")]
	PreconditionFailed,

	/// Parts specified in CMU request do not match parts actually uploaded
	#[error("Parts given to CompleteMultipartUpload do not match uploaded parts")]
	InvalidPart,

	/// Parts given to `CompleteMultipartUpload` were not in ascending order
	#[error("Parts given to CompleteMultipartUpload were not in ascending order")]
	InvalidPartOrder,

	/// In `CompleteMultipartUpload`: not enough data
	/// (here we are more lenient than AWS S3)
	#[error("Proposed upload is smaller than the minimum allowed object size")]
	EntityTooSmall,

	// Category: bad request
	/// The request contained an invalid UTF-8 sequence in its path or in other parameters
	#[error("Invalid UTF-8: {0}")]
	InvalidUtf8Str(#[from] std::str::Utf8Error),

	/// The request used an invalid path
	#[error("Invalid UTF-8: {0}")]
	InvalidUtf8String(#[from] std::string::FromUtf8Error),

	/// The client sent invalid XML data
	#[error("failed to deserialize XML")]
	InvalidXml(#[from] roxmltree::Error),

	/// The client sent invalid XML data
	#[error("XML deserialization failed")]
	InvalidXmlDe(#[from] quick_xml::de::DeError),

	/// The server failed to serialize data into XML
	#[error("failed to serialize XML")]
	InvalidXmlSe(#[from] quick_xml::se::SeError),

	/// The client sent a range header with invalid value
	#[error("Invalid HTTP range: {0:?}")]
	InvalidRange((http_range::HttpRangeParseError, u64)),

	/// The client sent a range header with invalid value
	#[error("Invalid encryption algorithm: {0:?}, should be AES256")]
	InvalidEncryptionAlgorithm(String),

	/// The provided digest (checksum) value was invalid
	#[error("Invalid digest: {0}")]
	InvalidDigest(String),

	#[error("Bad digest: {0}")]
	BadDigest(String),
}

commonErrorDerivative!(Error);

// Helper errors are always passed as internal errors by default.
// To pass the specific error code back to the client, use `pass_helper_error`.
impl From<HelperError> for Error {
	fn from(err: HelperError) -> Error {
		Error::Common(helper_error_as_internal(err))
	}
}

impl From<(http_range::HttpRangeParseError, u64)> for Error {
	fn from(err: (http_range::HttpRangeParseError, u64)) -> Error {
		Error::InvalidRange(err)
	}
}

impl From<SignatureError> for Error {
	fn from(err: SignatureError) -> Self {
		match err {
			SignatureError::Common(c) => Self::Common(c),
			SignatureError::AuthorizationHeaderMalformed {
				unexpected,
				expected,
			} => Self::AuthorizationHeaderMalformed {
				unexpected,
				expected,
			},
			SignatureError::InvalidUtf8Str(i) => Self::InvalidUtf8Str(i),
			SignatureError::InvalidDigest(d) => Self::InvalidDigest(d),
			SignatureError::BadDigest(b) => Self::BadDigest(b),
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
			Error::Common(c) => c.aws_code(),
			Error::NoSuchKey => "NoSuchKey",
			Error::NoSuchUpload => "NoSuchUpload",
			Error::PreconditionFailed => "PreconditionFailed",
			Error::InvalidPart => "InvalidPart",
			Error::InvalidPartOrder => "InvalidPartOrder",
			Error::EntityTooSmall => "EntityTooSmall",
			Error::AuthorizationHeaderMalformed { .. } => "AuthorizationHeaderMalformed",
			Error::InvalidXml(_) => "MalformedXML",
			Error::InvalidXmlDe(_) => "MalformedXML",
			Error::InvalidXmlSe(_) => "InternalError",
			Error::InvalidRange(_) => "InvalidRange",
			Error::InvalidDigest(_) => "InvalidDigest",
			Error::InvalidUtf8Str(_) | Error::InvalidUtf8String(_) => "InvalidRequest",
			Error::InvalidEncryptionAlgorithm(_) => "InvalidEncryptionAlgorithmError",
			Error::NoSuchCORSConfiguration => "NoSuchCORSConfiguration",
			Error::NoSuchLifecycleConfiguration => "NoSuchLifecycleConfiguration",
			Error::BadDigest(_) => "BadDigest",
		}
	}
}

impl ApiError for Error {
	/// Get the HTTP status code that best represents the meaning of the error for the client
	fn http_status_code(&self) -> StatusCode {
		match self {
			Error::Common(c) => c.http_status_code(),
			Error::NoSuchKey
			| Error::NoSuchUpload
			| Error::NoSuchCORSConfiguration
			| Error::NoSuchLifecycleConfiguration => StatusCode::NOT_FOUND,
			Error::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
			Error::InvalidRange(_) => StatusCode::RANGE_NOT_SATISFIABLE,
			Error::InvalidXmlSe(_) => StatusCode::INTERNAL_SERVER_ERROR,
			Error::AuthorizationHeaderMalformed { .. }
			| Error::InvalidPart
			| Error::InvalidPartOrder
			| Error::EntityTooSmall
			| Error::InvalidDigest(_)
			| Error::InvalidEncryptionAlgorithm(_)
			| Error::InvalidXml(_)
			| Error::InvalidXmlDe(_)
			| Error::InvalidUtf8Str(_)
			| Error::InvalidUtf8String(_)
			| Error::BadDigest(_) => StatusCode::BAD_REQUEST,
		}
	}

	fn add_http_headers(&self, header_map: &mut HeaderMap<HeaderValue>) {
		use hyper::header;

		header_map.append(header::CONTENT_TYPE, "application/xml".parse().unwrap());
		header_map.append(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());

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

	fn http_body(&self, garage_region: &str, path: &str) -> ErrorBody {
		let error = s3_xml::Error {
			code: s3_xml::Value(self.aws_code().to_string()),
			message: s3_xml::Value(format!("{}", self)),
			resource: Some(s3_xml::Value(path.to_string())),
			region: Some(s3_xml::Value(garage_region.to_string())),
		};
		let error_str = s3_xml::to_xml_with_header(&error).unwrap_or_else(|_| {
			r#"
<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>InternalError</Code>
    <Message>XML encoding of error failed</Message>
</Error>
            "#
			.into()
		});
		error_body(error_str)
	}
}
