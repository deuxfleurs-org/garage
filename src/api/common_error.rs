use err_derive::Error;
use hyper::StatusCode;

use garage_util::error::Error as GarageError;

/// Errors of this crate
#[derive(Debug, Error)]
pub enum CommonError {
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

	/// The client sent an invalid request
	#[error(display = "Bad request: {}", _0)]
	BadRequest(String),
}

impl CommonError {
	pub fn http_status_code(&self) -> StatusCode {
		match self {
			CommonError::InternalError(
				GarageError::Timeout
				| GarageError::RemoteError(_)
				| GarageError::Quorum(_, _, _, _),
			) => StatusCode::SERVICE_UNAVAILABLE,
			CommonError::InternalError(_) | CommonError::Hyper(_) | CommonError::Http(_) => {
				StatusCode::INTERNAL_SERVER_ERROR
			}
			CommonError::BadRequest(_) => StatusCode::BAD_REQUEST,
		}
	}


	pub fn bad_request<M: ToString>(msg: M) -> Self {
		CommonError::BadRequest(msg.to_string())
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
