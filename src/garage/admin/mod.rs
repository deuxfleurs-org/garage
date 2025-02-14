mod block;
mod bucket;
mod key;

use std::collections::HashMap;
use std::fmt::Write;
use std::future::Future;
use std::sync::Arc;

use futures::future::FutureExt;

use serde::{Deserialize, Serialize};

use format_table::format_table_to_string;

use garage_util::background::BackgroundRunner;
use garage_util::data::*;
use garage_util::error::Error as GarageError;

use garage_table::replication::*;
use garage_table::*;

use garage_rpc::layout::PARTITION_BITS;
use garage_rpc::*;

use garage_block::manager::BlockResyncErrorInfo;

use garage_model::bucket_table::*;
use garage_model::garage::Garage;
use garage_model::helper::error::{Error, OkOrBadRequest};
use garage_model::key_table::*;
use garage_model::s3::mpu_table::MultipartUpload;
use garage_model::s3::version_table::Version;

use crate::cli::*;
use crate::repair::online::launch_online_repair;

pub const ADMIN_RPC_PATH: &str = "garage/admin_rpc.rs/Rpc";

#[derive(Debug, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum AdminRpc {
	BucketOperation(BucketOperation),
	KeyOperation(KeyOperation),
	LaunchRepair(RepairOpt),
	Stats(StatsOpt),
	Worker(WorkerOperation),
	BlockOperation(BlockOperation),
	MetaOperation(MetaOperation),

	// Replies
	Ok(String),
	BucketList(Vec<Bucket>),
	BucketInfo {
		bucket: Bucket,
		relevant_keys: HashMap<String, Key>,
		counters: HashMap<String, i64>,
		mpu_counters: HashMap<String, i64>,
	},
	KeyList(Vec<(String, String)>),
	KeyInfo(Key, HashMap<Uuid, Bucket>),
	WorkerList(
		HashMap<usize, garage_util::background::WorkerInfo>,
		WorkerListOpt,
	),
	WorkerVars(Vec<(Uuid, String, String)>),
	WorkerInfo(usize, garage_util::background::WorkerInfo),
	BlockErrorList(Vec<BlockResyncErrorInfo>),
	BlockInfo {
		hash: Hash,
		refcount: u64,
		versions: Vec<Result<Version, Uuid>>,
		uploads: Vec<MultipartUpload>,
	},
}

impl Rpc for AdminRpc {
	type Response = Result<AdminRpc, Error>;
}

pub struct AdminRpcHandler {
	garage: Arc<Garage>,
	background: Arc<BackgroundRunner>,
	endpoint: Arc<Endpoint<AdminRpc, Self>>,
}

impl AdminRpcHandler {
	pub fn new(garage: Arc<Garage>, background: Arc<BackgroundRunner>) -> Arc<Self> {
		let endpoint = garage.system.netapp.endpoint(ADMIN_RPC_PATH.into());
		let admin = Arc::new(Self {
			garage,
			background,
			endpoint,
		});
		admin.endpoint.set_handler(admin.clone());
		admin
	}

	// ================ REPAIR COMMANDS ====================

	async fn handle_launch_repair(self: &Arc<Self>, opt: RepairOpt) -> Result<AdminRpc, Error> {
		if !opt.yes {
			return Err(Error::BadRequest(
				"Please provide the --yes flag to initiate repair operations.".to_string(),
			));
		}
		if opt.all_nodes {
			let mut opt_to_send = opt.clone();
			opt_to_send.all_nodes = false;

			let mut failures = vec![];
			let all_nodes = self.garage.system.cluster_layout().all_nodes().to_vec();
			for node in all_nodes.iter() {
				let node = (*node).into();
				let resp = self
					.endpoint
					.call(
						&node,
						AdminRpc::LaunchRepair(opt_to_send.clone()),
						PRIO_NORMAL,
					)
					.await;
				if !matches!(resp, Ok(Ok(_))) {
					failures.push(node);
				}
			}
			if failures.is_empty() {
				Ok(AdminRpc::Ok("Repair launched on all nodes".to_string()))
			} else {
				Err(Error::BadRequest(format!(
					"Could not launch repair on nodes: {:?} (launched successfully on other nodes)",
					failures
				)))
			}
		} else {
			launch_online_repair(&self.garage, &self.background, opt).await?;
			Ok(AdminRpc::Ok(format!(
				"Repair launched on {:?}",
				self.garage.system.id
			)))
		}
	}

	// ================ STATS COMMANDS ====================

	async fn handle_stats(&self, opt: StatsOpt) -> Result<AdminRpc, Error> {
		if opt.all_nodes {
			let mut ret = String::new();
			let mut all_nodes = self.garage.system.cluster_layout().all_nodes().to_vec();
			for node in self.garage.system.get_known_nodes().iter() {
				if node.is_up && !all_nodes.contains(&node.id) {
					all_nodes.push(node.id);
				}
			}

			for node in all_nodes.iter() {
				let mut opt = opt.clone();
				opt.all_nodes = false;
				opt.skip_global = true;

				writeln!(&mut ret, "\n======================").unwrap();
				writeln!(&mut ret, "Stats for node {:?}:", node).unwrap();

				let node_id = (*node).into();
				match self
					.endpoint
					.call(&node_id, AdminRpc::Stats(opt), PRIO_NORMAL)
					.await
				{
					Ok(Ok(AdminRpc::Ok(s))) => writeln!(&mut ret, "{}", s).unwrap(),
					Ok(Ok(x)) => writeln!(&mut ret, "Bad answer: {:?}", x).unwrap(),
					Ok(Err(e)) => writeln!(&mut ret, "Remote error: {}", e).unwrap(),
					Err(e) => writeln!(&mut ret, "Network error: {}", e).unwrap(),
				}
			}

			writeln!(&mut ret, "\n======================").unwrap();
			write!(
				&mut ret,
				"Cluster statistics:\n\n{}",
				self.gather_cluster_stats()
			)
			.unwrap();

			Ok(AdminRpc::Ok(ret))
		} else {
			Ok(AdminRpc::Ok(self.gather_stats_local(opt)?))
		}
	}

	fn gather_stats_local(&self, opt: StatsOpt) -> Result<String, Error> {
		let mut ret = String::new();
		writeln!(
			&mut ret,
			"\nGarage version: {} [features: {}]\nRust compiler version: {}",
			garage_util::version::garage_version(),
			garage_util::version::garage_features()
				.map(|list| list.join(", "))
				.unwrap_or_else(|| "(unknown)".into()),
			garage_util::version::rust_version(),
		)
		.unwrap();

		writeln!(&mut ret, "\nDatabase engine: {}", self.garage.db.engine()).unwrap();

		// Gather table statistics
		let mut table = vec!["  Table\tItems\tMklItems\tMklTodo\tGcTodo".into()];
		table.push(self.gather_table_stats(&self.garage.bucket_table)?);
		table.push(self.gather_table_stats(&self.garage.key_table)?);
		table.push(self.gather_table_stats(&self.garage.object_table)?);
		table.push(self.gather_table_stats(&self.garage.version_table)?);
		table.push(self.gather_table_stats(&self.garage.block_ref_table)?);
		write!(
			&mut ret,
			"\nTable stats:\n{}",
			format_table_to_string(table)
		)
		.unwrap();

		// Gather block manager statistics
		writeln!(&mut ret, "\nBlock manager stats:").unwrap();
		let rc_len = self.garage.block_manager.rc_len()?.to_string();

		writeln!(
			&mut ret,
			"  number of RC entries (~= number of blocks): {}",
			rc_len
		)
		.unwrap();
		writeln!(
			&mut ret,
			"  resync queue length: {}",
			self.garage.block_manager.resync.queue_len()?
		)
		.unwrap();
		writeln!(
			&mut ret,
			"  blocks with resync errors: {}",
			self.garage.block_manager.resync.errors_len()?
		)
		.unwrap();

		if !opt.skip_global {
			write!(&mut ret, "\n{}", self.gather_cluster_stats()).unwrap();
		}

		Ok(ret)
	}

	fn gather_cluster_stats(&self) -> String {
		let mut ret = String::new();

		// Gather storage node and free space statistics for current nodes
		let layout = &self.garage.system.cluster_layout();
		let mut node_partition_count = HashMap::<Uuid, u64>::new();
		for short_id in layout.current().ring_assignment_data.iter() {
			let id = layout.current().node_id_vec[*short_id as usize];
			*node_partition_count.entry(id).or_default() += 1;
		}
		let node_info = self
			.garage
			.system
			.get_known_nodes()
			.into_iter()
			.map(|n| (n.id, n))
			.collect::<HashMap<_, _>>();

		let mut table = vec!["  ID\tHostname\tZone\tCapacity\tPart.\tDataAvail\tMetaAvail".into()];
		for (id, parts) in node_partition_count.iter() {
			let info = node_info.get(id);
			let status = info.map(|x| &x.status);
			let role = layout.current().roles.get(id).and_then(|x| x.0.as_ref());
			let hostname = status.and_then(|x| x.hostname.as_deref()).unwrap_or("?");
			let zone = role.map(|x| x.zone.as_str()).unwrap_or("?");
			let capacity = role
				.map(|x| x.capacity_string())
				.unwrap_or_else(|| "?".into());
			let avail_str = |x| match x {
				Some((avail, total)) => {
					let pct = (avail as f64) / (total as f64) * 100.;
					let avail = bytesize::ByteSize::b(avail);
					let total = bytesize::ByteSize::b(total);
					format!("{}/{} ({:.1}%)", avail, total, pct)
				}
				None => "?".into(),
			};
			let data_avail = avail_str(status.and_then(|x| x.data_disk_avail));
			let meta_avail = avail_str(status.and_then(|x| x.meta_disk_avail));
			table.push(format!(
				"  {:?}\t{}\t{}\t{}\t{}\t{}\t{}",
				id, hostname, zone, capacity, parts, data_avail, meta_avail
			));
		}
		write!(
			&mut ret,
			"Storage nodes:\n{}",
			format_table_to_string(table)
		)
		.unwrap();

		let meta_part_avail = node_partition_count
			.iter()
			.filter_map(|(id, parts)| {
				node_info
					.get(id)
					.and_then(|x| x.status.meta_disk_avail)
					.map(|c| c.0 / *parts)
			})
			.collect::<Vec<_>>();
		let data_part_avail = node_partition_count
			.iter()
			.filter_map(|(id, parts)| {
				node_info
					.get(id)
					.and_then(|x| x.status.data_disk_avail)
					.map(|c| c.0 / *parts)
			})
			.collect::<Vec<_>>();
		if !meta_part_avail.is_empty() && !data_part_avail.is_empty() {
			let meta_avail =
				bytesize::ByteSize(meta_part_avail.iter().min().unwrap() * (1 << PARTITION_BITS));
			let data_avail =
				bytesize::ByteSize(data_part_avail.iter().min().unwrap() * (1 << PARTITION_BITS));
			writeln!(
				&mut ret,
				"\nEstimated available storage space cluster-wide (might be lower in practice):"
			)
			.unwrap();
			if meta_part_avail.len() < node_partition_count.len()
				|| data_part_avail.len() < node_partition_count.len()
			{
				writeln!(&mut ret, "  data: < {}", data_avail).unwrap();
				writeln!(&mut ret, "  metadata: < {}", meta_avail).unwrap();
				writeln!(&mut ret, "A precise estimate could not be given as information is missing for some storage nodes.").unwrap();
			} else {
				writeln!(&mut ret, "  data: {}", data_avail).unwrap();
				writeln!(&mut ret, "  metadata: {}", meta_avail).unwrap();
			}
		}

		ret
	}

	fn gather_table_stats<F, R>(&self, t: &Arc<Table<F, R>>) -> Result<String, Error>
	where
		F: TableSchema + 'static,
		R: TableReplication + 'static,
	{
		let data_len = t.data.store.len().map_err(GarageError::from)?.to_string();
		let mkl_len = t.merkle_updater.merkle_tree_len()?.to_string();

		Ok(format!(
			"  {}\t{}\t{}\t{}\t{}",
			F::TABLE_NAME,
			data_len,
			mkl_len,
			t.merkle_updater.todo_len()?,
			t.data.gc_todo_len()?
		))
	}

	// ================ WORKER COMMANDS ====================

	async fn handle_worker_cmd(&self, cmd: &WorkerOperation) -> Result<AdminRpc, Error> {
		match cmd {
			WorkerOperation::List { opt } => {
				let workers = self.background.get_worker_info();
				Ok(AdminRpc::WorkerList(workers, *opt))
			}
			WorkerOperation::Info { tid } => {
				let info = self
					.background
					.get_worker_info()
					.get(tid)
					.ok_or_bad_request(format!("No worker with TID {}", tid))?
					.clone();
				Ok(AdminRpc::WorkerInfo(*tid, info))
			}
			WorkerOperation::Get {
				all_nodes,
				variable,
			} => self.handle_get_var(*all_nodes, variable).await,
			WorkerOperation::Set {
				all_nodes,
				variable,
				value,
			} => self.handle_set_var(*all_nodes, variable, value).await,
		}
	}

	async fn handle_get_var(
		&self,
		all_nodes: bool,
		variable: &Option<String>,
	) -> Result<AdminRpc, Error> {
		if all_nodes {
			let mut ret = vec![];
			let all_nodes = self.garage.system.cluster_layout().all_nodes().to_vec();
			for node in all_nodes.iter() {
				let node = (*node).into();
				match self
					.endpoint
					.call(
						&node,
						AdminRpc::Worker(WorkerOperation::Get {
							all_nodes: false,
							variable: variable.clone(),
						}),
						PRIO_NORMAL,
					)
					.await??
				{
					AdminRpc::WorkerVars(v) => ret.extend(v),
					m => return Err(GarageError::unexpected_rpc_message(m).into()),
				}
			}
			Ok(AdminRpc::WorkerVars(ret))
		} else {
			#[allow(clippy::collapsible_else_if)]
			if let Some(v) = variable {
				Ok(AdminRpc::WorkerVars(vec![(
					self.garage.system.id,
					v.clone(),
					self.garage.bg_vars.get(v)?,
				)]))
			} else {
				let mut vars = self.garage.bg_vars.get_all();
				vars.sort();
				Ok(AdminRpc::WorkerVars(
					vars.into_iter()
						.map(|(k, v)| (self.garage.system.id, k.to_string(), v))
						.collect(),
				))
			}
		}
	}

	async fn handle_set_var(
		&self,
		all_nodes: bool,
		variable: &str,
		value: &str,
	) -> Result<AdminRpc, Error> {
		if all_nodes {
			let mut ret = vec![];
			let all_nodes = self.garage.system.cluster_layout().all_nodes().to_vec();
			for node in all_nodes.iter() {
				let node = (*node).into();
				match self
					.endpoint
					.call(
						&node,
						AdminRpc::Worker(WorkerOperation::Set {
							all_nodes: false,
							variable: variable.to_string(),
							value: value.to_string(),
						}),
						PRIO_NORMAL,
					)
					.await??
				{
					AdminRpc::WorkerVars(v) => ret.extend(v),
					m => return Err(GarageError::unexpected_rpc_message(m).into()),
				}
			}
			Ok(AdminRpc::WorkerVars(ret))
		} else {
			self.garage.bg_vars.set(variable, value)?;
			Ok(AdminRpc::WorkerVars(vec![(
				self.garage.system.id,
				variable.to_string(),
				value.to_string(),
			)]))
		}
	}

	// ================ META DB COMMANDS ====================

	async fn handle_meta_cmd(self: &Arc<Self>, mo: &MetaOperation) -> Result<AdminRpc, Error> {
		match mo {
			MetaOperation::Snapshot { all: true } => {
				let to = self.garage.system.cluster_layout().all_nodes().to_vec();

				let resps = futures::future::join_all(to.iter().map(|to| async move {
					let to = (*to).into();
					self.endpoint
						.call(
							&to,
							AdminRpc::MetaOperation(MetaOperation::Snapshot { all: false }),
							PRIO_NORMAL,
						)
						.await?
				}))
				.await;

				let mut ret = vec![];
				for (to, resp) in to.iter().zip(resps.iter()) {
					let res_str = match resp {
						Ok(_) => "ok".to_string(),
						Err(e) => format!("error: {}", e),
					};
					ret.push(format!("{:?}\t{}", to, res_str));
				}

				if resps.iter().any(Result::is_err) {
					Err(GarageError::Message(format_table_to_string(ret)).into())
				} else {
					Ok(AdminRpc::Ok(format_table_to_string(ret)))
				}
			}
			MetaOperation::Snapshot { all: false } => {
				garage_model::snapshot::async_snapshot_metadata(&self.garage).await?;
				Ok(AdminRpc::Ok("Snapshot has been saved.".into()))
			}
		}
	}
}

impl EndpointHandler<AdminRpc> for AdminRpcHandler {
	fn handle(
		self: &Arc<Self>,
		message: &AdminRpc,
		_from: NodeID,
	) -> impl Future<Output = Result<AdminRpc, Error>> + Send {
		let self2 = self.clone();
		async move {
			match message {
				AdminRpc::BucketOperation(bo) => self2.handle_bucket_cmd(bo).await,
				AdminRpc::KeyOperation(ko) => self2.handle_key_cmd(ko).await,
				AdminRpc::LaunchRepair(opt) => self2.handle_launch_repair(opt.clone()).await,
				AdminRpc::Stats(opt) => self2.handle_stats(opt.clone()).await,
				AdminRpc::Worker(wo) => self2.handle_worker_cmd(wo).await,
				AdminRpc::BlockOperation(bo) => self2.handle_block_cmd(bo).await,
				AdminRpc::MetaOperation(mo) => self2.handle_meta_cmd(mo).await,
				m => Err(GarageError::unexpected_rpc_message(m).into()),
			}
		}
		.boxed()
	}
}
