#[macro_use]
extern crate tracing;

pub mod api_server;
mod error;
mod macros;

pub mod api;
pub mod openapi;
mod router_v0;
mod router_v1;
mod router_v2;

mod admin_token;
mod bucket;
mod cluster;
mod key;
mod layout;
mod special;

mod block;
mod node;
mod repair;
mod worker;

use std::sync::Arc;

use garage_model::garage::Garage;

pub use api_server::AdminApiServer as Admin;

pub enum Authorization {
	None,
	MetricsToken,
	AdminToken,
}

pub trait RequestHandler {
	type Response;

	fn handle(
		self,
		garage: &Arc<Garage>,
		admin: &Admin,
	) -> impl std::future::Future<Output = Result<Self::Response, error::Error>> + Send;
}
