use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::data::*;
use crate::error::Error;
use crate::rpc_server::*;
use crate::server::Garage;
use crate::table::*;
use crate::*;

use crate::bucket_table::*;

pub const ADMIN_RPC_PATH: &str = "_admin";

#[derive(Debug, Serialize, Deserialize)]
pub enum AdminRPC {
	BucketOperation(BucketOperation),

	// Replies
	Ok(String),
	BucketList(Vec<String>),
	BucketInfo(Bucket),
}

impl RpcMessage for AdminRPC {}

pub struct AdminRpcHandler {
	garage: Arc<Garage>,
}

impl AdminRpcHandler {
	pub fn new(garage: Arc<Garage>) -> Arc<Self> {
		Arc::new(Self { garage })
	}

	pub fn register_handler(self: Arc<Self>, rpc_server: &mut RpcServer) {
		rpc_server.add_handler::<AdminRPC, _, _>(ADMIN_RPC_PATH.to_string(), move |msg, _addr| {
			let self2 = self.clone();
			async move {
				match msg {
					AdminRPC::BucketOperation(bo) => self2.handle_bucket_cmd(bo).await,
					_ => Err(Error::Message(format!("Invalid RPC"))),
				}
			}
		});
	}

	async fn handle_bucket_cmd(&self, cmd: BucketOperation) -> Result<AdminRPC, Error> {
		match cmd {
			BucketOperation::List => {
				let bucket_names = self
					.garage
					.bucket_table
					.get_range(&EmptyKey, None, Some(()), 10000)
					.await?
					.iter()
					.map(|b| b.name.to_string())
					.collect::<Vec<_>>();
				Ok(AdminRPC::BucketList(bucket_names))
			}
			BucketOperation::Info(query) => {
				let bucket = self
					.garage
					.bucket_table
					.get(&EmptyKey, &query.name)
					.await?
					.filter(|b| !b.deleted);
				match bucket {
					Some(b) => Ok(AdminRPC::BucketInfo(b)),
					None => Err(Error::Message(format!("Bucket {} not found", query.name))),
				}
			}
			BucketOperation::Create(query) => {
				let bucket = self.garage.bucket_table.get(&EmptyKey, &query.name).await?;
				if bucket.as_ref().filter(|b| !b.deleted).is_some() {
					return Err(Error::Message(format!(
						"Bucket {} already exists",
						query.name
					)));
				}
				let new_time = match bucket {
					Some(b) => std::cmp::max(b.timestamp + 1, now_msec()),
					None => now_msec(),
				};
				self.garage
					.bucket_table
					.insert(&Bucket {
						name: query.name.clone(),
						timestamp: new_time,
						deleted: false,
						authorized_keys: vec![],
					})
					.await?;
				Ok(AdminRPC::Ok(format!("Bucket {} was created.", query.name)))
			}
			BucketOperation::Delete(query) => {
				let bucket = match self
					.garage
					.bucket_table
					.get(&EmptyKey, &query.name)
					.await?
					.filter(|b| !b.deleted)
				{
					None => {
						return Err(Error::Message(format!(
							"Bucket {} does not exist",
							query.name
						)));
					}
					Some(b) => b,
				};
				let objects = self
					.garage
					.object_table
					.get_range(&query.name, None, Some(()), 10)
					.await?;
				if !objects.is_empty() {
					return Err(Error::Message(format!(
						"Bucket {} is not empty",
						query.name
					)));
				}
				if !query.yes {
					return Err(Error::Message(format!(
						"Add --yes flag to really perform this operation"
					)));
				}
				self.garage
					.bucket_table
					.insert(&Bucket {
						name: query.name.clone(),
						timestamp: std::cmp::max(bucket.timestamp + 1, now_msec()),
						deleted: true,
						authorized_keys: vec![],
					})
					.await?;
				Ok(AdminRPC::Ok(format!("Bucket {} was deleted.", query.name)))
			}
			_ => {
				// TODO
				Err(Error::Message(format!("Not implemented")))
			}
		}
	}
}
