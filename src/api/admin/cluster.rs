use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use format_table::format_table_to_string;

use garage_util::data::*;

use garage_rpc::layout;
use garage_rpc::layout::PARTITION_BITS;

use garage_model::garage::Garage;

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
						..Default::default()
					},
				)
			})
			.collect::<HashMap<_, _>>();

		for (id, _, role) in layout.current().roles.items().iter() {
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

		for ver in layout.versions().iter().rev().skip(1) {
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

		let mut nodes = nodes.into_values().collect::<Vec<_>>();
		nodes.sort_by(|x, y| x.id.cmp(&y.id));

		Ok(GetClusterStatusResponse {
			layout_version: layout.current().version,
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
			storage_nodes_ok: health.storage_nodes_ok,
			partitions: health.partitions,
			partitions_quorum: health.partitions_quorum,
			partitions_all_ok: health.partitions_all_ok,
		};
		Ok(health)
	}
}

impl RequestHandler for GetClusterStatisticsRequest {
	type Response = GetClusterStatisticsResponse;

	// FIXME: return this as a JSON struct instead of text
	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<GetClusterStatisticsResponse, Error> {
		let mut ret = String::new();

		// Gather storage node and free space statistics for current nodes
		let layout = &garage.system.cluster_layout();
		let mut node_partition_count = HashMap::<Uuid, u64>::new();
		for short_id in layout.current().ring_assignment_data.iter() {
			let id = layout.current().node_id_vec[*short_id as usize];
			*node_partition_count.entry(id).or_default() += 1;
		}
		let node_info = garage
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
				ret += &format_table_to_string(vec![
					format!("  data: < {}", data_avail),
					format!("  metadata: < {}", meta_avail),
				]);
				writeln!(&mut ret, "A precise estimate could not be given as information is missing for some storage nodes.").unwrap();
			} else {
				ret += &format_table_to_string(vec![
					format!("  data: {}", data_avail),
					format!("  metadata: {}", meta_avail),
				]);
			}
		}

		Ok(GetClusterStatisticsResponse { freeform: ret })
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
