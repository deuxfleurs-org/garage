pub mod api_server;
mod error;

pub mod api;
mod router_v0;
mod router_v1;

mod bucket;
mod cluster;
mod key;

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;

use garage_model::garage::Garage;

#[async_trait]
pub trait EndpointHandler {
	type Response: Serialize;

	async fn handle(self, garage: &Arc<Garage>) -> Result<Self::Response, error::Error>;
}
