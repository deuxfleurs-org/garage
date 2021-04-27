use std::fmt::Write;

use err_derive::Error;
use hyper::StatusCode;

use garage_util::error::Error as GarageError;

use crate::encoding::*;

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
	HTTP(#[error(source)] http::Error),

	// Category: cannot process
	/// No proper api key was used, or the signature was invalid
	#[error(display = "Forbidden: {}", _0)]
	Forbidden(String),

	/// Authorization Header Malformed
	#[error(display = "Authorization header malformed, expected scope: {}", _0)]
	AuthorizationHeaderMalformed(String),

	/// The object requested don't exists
	#[error(display = "Not found")]
	NotFound,

	// Category: bad request
	/// The request contained an invalid UTF-8 sequence in its path or in other parameters
	#[error(display = "Invalid UTF-8: {}", _0)]
	InvalidUTF8Str(#[error(source)] std::str::Utf8Error),

	/// The request used an invalid path
	#[error(display = "Invalid UTF-8: {}", _0)]
	InvalidUTF8String(#[error(source)] std::string::FromUtf8Error),

	/// Some base64 encoded data was badly encoded
	#[error(display = "Invalid base64: {}", _0)]
	InvalidBase64(#[error(source)] base64::DecodeError),

	/// The client sent invalid XML data
	#[error(display = "Invalid XML: {}", _0)]
	InvalidXML(String),

	/// The client sent a header with invalid value
	#[error(display = "Invalid header value: {}", _0)]
	InvalidHeader(#[error(source)] hyper::header::ToStrError),

	/// The client sent a range header with invalid value
	#[error(display = "Invalid HTTP range: {:?}", _0)]
	InvalidRange(#[error(from)] http_range::HttpRangeParseError),

	/// The client sent an invalid request
	#[error(display = "Bad request: {}", _0)]
	BadRequest(String),
}

impl From<roxmltree::Error> for Error {
	fn from(err: roxmltree::Error) -> Self {
		Self::InvalidXML(format!("{}", err))
	}
}

impl Error {
	/// Get the HTTP status code that best represents the meaning of the error for the client
	pub fn http_status_code(&self) -> StatusCode {
		match self {
			Error::NotFound => StatusCode::NOT_FOUND,
			Error::Forbidden(_) => StatusCode::FORBIDDEN,
			Error::InternalError(GarageError::RPC(_)) => StatusCode::SERVICE_UNAVAILABLE,
			Error::InternalError(_) | Error::Hyper(_) | Error::HTTP(_) => {
				StatusCode::INTERNAL_SERVER_ERROR
			}
			_ => StatusCode::BAD_REQUEST,
		}
	}

	pub fn aws_code(&self) -> &'static str {
		match self {
			Error::NotFound => "NoSuchKey",
			Error::Forbidden(_) => "AccessDenied",
			Error::AuthorizationHeaderMalformed(_) => "AuthorizationHeaderMalformed",
			Error::InternalError(GarageError::RPC(_)) => "ServiceUnavailable",
			Error::InternalError(_) | Error::Hyper(_) | Error::HTTP(_) => "InternalError",
			_ => "InvalidRequest",
		}
	}

	pub fn aws_xml(&self, garage_region: &str, path: &str) -> String {
		let mut xml = String::new();
		writeln!(&mut xml, r#"<?xml version="1.0" encoding="UTF-8"?>"#).unwrap();
		writeln!(&mut xml, "<Error>").unwrap();
		writeln!(&mut xml, "\t<Code>{}</Code>", self.aws_code()).unwrap();
		writeln!(&mut xml, "\t<Message>{}</Message>", self).unwrap();
		writeln!(&mut xml, "\t<Resource>{}</Resource>", xml_escape(path)).unwrap();
		writeln!(&mut xml, "\t<Region>{}</Region>", garage_region).unwrap();
		writeln!(&mut xml, "</Error>").unwrap();
		xml
	}
}

/// Trait to map error to the Bad Request error code
pub trait OkOrBadRequest {
	type S2;
	fn ok_or_bad_request(self, reason: &'static str) -> Self::S2;
}

impl<T, E> OkOrBadRequest for Result<T, E>
where
	E: std::fmt::Display,
{
	type S2 = Result<T, Error>;
	fn ok_or_bad_request(self, reason: &'static str) -> Result<T, Error> {
		match self {
			Ok(x) => Ok(x),
			Err(e) => Err(Error::BadRequest(format!("{}: {}", reason, e))),
		}
	}
}

impl<T> OkOrBadRequest for Option<T> {
	type S2 = Result<T, Error>;
	fn ok_or_bad_request(self, reason: &'static str) -> Result<T, Error> {
		match self {
			Some(x) => Ok(x),
			None => Err(Error::BadRequest(format!("{}", reason))),
		}
	}
}

/// Trait to map an error to an Internal Error code
pub trait OkOrInternalError {
	type S2;
	fn ok_or_internal_error(self, reason: &'static str) -> Self::S2;
}

impl<T, E> OkOrInternalError for Result<T, E>
where
	E: std::fmt::Display,
{
	type S2 = Result<T, Error>;
	fn ok_or_internal_error(self, reason: &'static str) -> Result<T, Error> {
		match self {
			Ok(x) => Ok(x),
			Err(e) => Err(Error::InternalError(GarageError::Message(format!(
				"{}: {}",
				reason, e
			)))),
		}
	}
}

impl<T> OkOrInternalError for Option<T> {
	type S2 = Result<T, Error>;
	fn ok_or_internal_error(self, reason: &'static str) -> Result<T, Error> {
		match self {
			Some(x) => Ok(x),
			None => Err(Error::InternalError(GarageError::Message(format!(
				"{}",
				reason
			)))),
		}
	}
}
