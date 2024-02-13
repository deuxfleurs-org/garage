use err_derive::Error;
use serde::{Deserialize, Serialize};

use garage_util::error::Error as GarageError;

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum Error {
	#[error(display = "Internal error: {}", _0)]
	Internal(#[error(source)] GarageError),

	#[error(display = "Bad request: {}", _0)]
	BadRequest(String),

	/// Bucket name is not valid according to AWS S3 specs
	#[error(display = "Invalid bucket name: {}", _0)]
	InvalidBucketName(String),

	#[error(display = "Access key not found: {}", _0)]
	NoSuchAccessKey(String),

	#[error(display = "Bucket not found: {}", _0)]
	NoSuchBucket(String),
}

impl From<garage_net::error::Error> for Error {
	fn from(e: garage_net::error::Error) -> Self {
		Error::Internal(GarageError::Net(e))
	}
}

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
