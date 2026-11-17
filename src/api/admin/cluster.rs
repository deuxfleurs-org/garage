use std::collections::HashMap;
use std::sync::Arc;

use garage_util::data::*;

use garage_rpc::layout;
use garage_rpc::layout::PARTITION_BITS;
use garage_table::*;

use garage_model::garage::Garage;
use garage_model::s3::object_table;

use crate::api::*;
use crate::error::*;
use crate::{Admin, RequestHandler};

impl RequestHandler for GetClusterStatusRequest {
	type Response = GetClusterStatusResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<GetClusterStatusResponse, Error> {
		let layout = garage.system.cluster_layout();
		let mut nodes = garage
			.system
			.get_known_nodes()
			.into_iter()
			.map(|i| {
				(
					i.id,
					NodeResp {
						id: hex::encode(i.id),
						garage_version: i.status.garage_version,
						addr: i.addr,
						hostname: i.status.hostname,
						is_up: i.is_up,
						last_seen_secs_ago: i.last_seen_secs_ago,
						data_partition: i.status.data_disk_avail.map(|(avail, total)| {
							FreeSpaceResp {
								available: avail,
								total,
							}
						}),
						metadata_partition: i.status.meta_disk_avail.map(|(avail, total)| {
							FreeSpaceResp {
								available: avail,
								total,
							}
						}),
						resync_queue_len: i.status.resync_queue_len,
						..Default::default()
					},
				)
			})
			.collect::<HashMap<_, _>>();

		if let Ok(current_layout) = layout.current() {
			for (id, _, role) in current_layout.roles.items().iter() {
				if let layout::NodeRoleV(Some(r)) = role {
					let role = NodeAssignedRole {
						zone: r.zone.to_string(),
						capacity: r.capacity,
						tags: r.tags.clone(),
					};
					match nodes.get_mut(id) {
						None => {
							nodes.insert(
								*id,
								NodeResp {
									id: hex::encode(id),
									role: Some(role),
									..Default::default()
								},
							);
						}
						Some(n) => {
							n.role = Some(role);
						}
					}
				}
			}
		}

		if let Ok(layout_versions) = layout.versions() {
			for ver in layout_versions.iter().rev().skip(1) {
				for (id, _, role) in ver.roles.items().iter() {
					if let layout::NodeRoleV(Some(r)) = role {
						if r.capacity.is_some() {
							if let Some(n) = nodes.get_mut(id) {
								if n.role.is_none() {
									n.draining = true;
								}
							} else {
								nodes.insert(
									*id,
									NodeResp {
										id: hex::encode(id),
										draining: true,
										..Default::default()
									},
								);
							}
						}
					}
				}
			}
		}

		let mut nodes = nodes.into_values().collect::<Vec<_>>();
		nodes.sort_by(|x, y| x.id.cmp(&y.id));

		Ok(GetClusterStatusResponse {
			layout_version: layout.inner().current().version,
			nodes,
		})
	}
}

impl RequestHandler for GetClusterHealthRequest {
	type Response = GetClusterHealthResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<GetClusterHealthResponse, Error> {
		use garage_rpc::system::ClusterHealthStatus;
		let health = garage.system.health();
		let health = GetClusterHealthResponse {
			status: match health.status {
				ClusterHealthStatus::Healthy => "healthy",
				ClusterHealthStatus::Degraded => "degraded",
				ClusterHealthStatus::Unavailable => "unavailable",
			}
			.to_string(),
			known_nodes: health.known_nodes,
			connected_nodes: health.connected_nodes,
			storage_nodes: health.storage_nodes,
			// Translating storage_nodes_up (admin API context) to storage_nodes_ok (metrics context)
			// TODO: when releasing major release, consider renaming all the fields in the metrics to storage_nodes_up
			storage_nodes_up: health.storage_nodes_ok,
			partitions: health.partitions,
			partitions_quorum: health.partitions_quorum,
			partitions_all_ok: health.partitions_all_ok,
		};
		Ok(health)
	}
}

impl RequestHandler for GetClusterStatisticsRequest {
	type Response = GetClusterStatisticsResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<GetClusterStatisticsResponse, Error> {
		// Gather info on number of buckets, objects and object size
		let buckets = garage
			.bucket_table
			.get_range(
				&EmptyKey,
				None,
				Some(DeletedFilter::NotDeleted),
				1_000_000,
				EnumerationOrder::Forward,
			)
			.await?;

		let bucket_stats_opt = if buckets.len() < 1000 {
			futures::future::try_join_all(
				buckets
					.iter()
					.map(|b| garage.object_counter_table.table.get(&b.id, &EmptyKey)),
			)
			.await
			.ok()
		} else {
			None
		};

		let layout = &garage.system.cluster_layout();

		let bucket_count = buckets.len() as u64;
		let (total_object_count, total_object_bytes);
		if let Some(bucket_stats) = bucket_stats_opt {
			let bucket_stats = bucket_stats
				.into_iter()
				.filter_map(|cnt| cnt.map(|x| x.filtered_values(layout)))
				.collect::<Vec<_>>();

			total_object_count = Some(
				bucket_stats
					.iter()
					.clone()
					.map(|cnt| *cnt.get(object_table::OBJECTS).unwrap_or(&0) as u64)
					.sum(),
			);
			total_object_bytes = Some(
				bucket_stats
					.iter()
					.clone()
					.map(|cnt| *cnt.get(object_table::BYTES).unwrap_or(&0) as u64)
					.sum(),
			);
		} else {
			total_object_count = None;
			total_object_bytes = None;
		}

		// Gather storage node and free space statistics for current nodes
		let mut node_partition_count = HashMap::<Uuid, u64>::new();
		if let Ok(current_layout) = layout.current() {
			for short_id in current_layout.ring_assignment_data.iter() {
				let id = current_layout.node_id_vec[*short_id as usize];
				*node_partition_count.entry(id).or_default() += 1;
			}
		}

		let node_info = garage
			.system
			.get_known_nodes()
			.into_iter()
			.map(|n| (n.id, n))
			.collect::<HashMap<_, _>>();

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

		let metadata_avail: u64 =
			meta_part_avail.iter().min().unwrap_or(&0) * (1 << PARTITION_BITS);
		let data_avail: u64 = data_part_avail.iter().min().unwrap_or(&0) * (1 << PARTITION_BITS);

		let incomplete_avail_info = meta_part_avail.len() < node_partition_count.len()
			|| data_part_avail.len() < node_partition_count.len();

		Ok(GetClusterStatisticsResponse {
			metadata_avail,
			data_avail,
			incomplete_avail_info,
			bucket_count,
			total_object_count,
			total_object_bytes,
		})
	}
}

impl RequestHandler for ConnectClusterNodesRequest {
	type Response = ConnectClusterNodesResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<ConnectClusterNodesResponse, Error> {
		let res = futures::future::join_all(self.0.iter().map(|node| garage.system.connect(node)))
			.await
			.into_iter()
			.map(|r| match r {
				Ok(()) => ConnectNodeResponse {
					success: true,
					error: None,
				},
				Err(e) => ConnectNodeResponse {
					success: false,
					error: Some(format!("{}", e)),
				},
			})
			.collect::<Vec<_>>();
		Ok(ConnectClusterNodesResponse(res))
	}
}
