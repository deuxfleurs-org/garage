#[macro_use]
extern crate tracing;

pub mod api_server;
mod error;
mod macros;

pub mod api;
mod router_v0;
mod router_v1;
mod router_v2;

mod bucket;
mod cluster;
mod key;
mod special;

use std::sync::Arc;

use async_trait::async_trait;

use garage_model::garage::Garage;

pub enum Authorization {
	None,
	MetricsToken,
	AdminToken,
}

#[async_trait]
pub trait EndpointHandler {
	type Response;

	async fn handle(self, garage: &Arc<Garage>) -> Result<Self::Response, error::Error>;
}
