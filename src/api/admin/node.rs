use std::sync::Arc;

use async_trait::async_trait;

use garage_model::garage::Garage;

use crate::api::*;
use crate::error::Error;
use crate::{Admin, RequestHandler};

#[async_trait]
impl RequestHandler for LocalCreateMetadataSnapshotRequest {
	type Response = LocalCreateMetadataSnapshotResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<LocalCreateMetadataSnapshotResponse, Error> {
		garage_model::snapshot::async_snapshot_metadata(garage).await?;
		Ok(LocalCreateMetadataSnapshotResponse)
	}
}
