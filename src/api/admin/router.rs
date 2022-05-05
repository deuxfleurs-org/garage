use crate::error::*;

use hyper::{Method, Request};

use crate::router_macros::router_match;

pub enum Authorization {
	MetricsToken,
	AdminToken,
}

router_match! {@func

/// List of all Admin API endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
	Options,
	Metrics,
	GetClusterStatus,
	GetClusterLayout,
	UpdateClusterLayout,
	ApplyClusterLayout,
	RevertClusterLayout,
}}

impl Endpoint {
	/// Determine which S3 endpoint a request is for using the request, and a bucket which was
	/// possibly extracted from the Host header.
	/// Returns Self plus bucket name, if endpoint is not Endpoint::ListBuckets
	pub fn from_request<T>(req: &Request<T>) -> Result<Self, Error> {
		let path = req.uri().path();

		use Endpoint::*;
		let res = match (req.method(), path) {
			(&Method::OPTIONS, _) => Options,
			(&Method::GET, "/metrics") => Metrics,
			(&Method::GET, "/status") => GetClusterStatus,
			(&Method::GET, "/layout") => GetClusterLayout,
			(&Method::POST, "/layout") => UpdateClusterLayout,
			(&Method::POST, "/layout/apply") => ApplyClusterLayout,
			(&Method::POST, "/layout/revert") => RevertClusterLayout,
			(m, p) => {
				return Err(Error::BadRequest(format!(
					"Unknown API endpoint: {} {}",
					m, p
				)))
			}
		};

		Ok(res)
	}
	/// Get the kind of authorization which is required to perform the operation.
	pub fn authorization_type(&self) -> Authorization {
		match self {
			Self::Metrics => Authorization::MetricsToken,
			_ => Authorization::AdminToken,
		}
	}
}
