use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use opentelemetry::{global, metrics::*, KeyValue};

use crate::system::{ClusterHealthStatus, System};

/// `TableMetrics` reference all counter used for metrics
pub struct SystemMetrics {
	// Static values
	pub(crate) _garage_build_info: ObservableGauge<u64>,
	pub(crate) _replication_factor: ObservableGauge<u64>,

	// Disk space values from System::local_status
	pub(crate) _disk_avail: ObservableGauge<u64>,
	pub(crate) _disk_total: ObservableGauge<u64>,

	// Health report from System::health()
	pub(crate) _cluster_healthy: ObservableGauge<u64>,
	pub(crate) _cluster_available: ObservableGauge<u64>,
	pub(crate) _known_nodes: ObservableGauge<u64>,
	pub(crate) _connected_nodes: ObservableGauge<u64>,
	pub(crate) _storage_nodes: ObservableGauge<u64>,
	pub(crate) _storage_nodes_ok: ObservableGauge<u64>,
	pub(crate) _partitions: ObservableGauge<u64>,
	pub(crate) _partitions_quorum: ObservableGauge<u64>,
	pub(crate) _partitions_all_ok: ObservableGauge<u64>,

	// Status report for individual cluster nodes
	pub(crate) _layout_node_connected: ObservableGauge<u64>,
	pub(crate) _layout_node_disconnected_time: ObservableGauge<u64>,
}

impl SystemMetrics {
	pub fn new(system: Arc<System>) -> Self {
		let meter = global::meter("garage_system");

		let health_cache = RwLock::new((Instant::now(), system.health()));
		let system2 = system.clone();
		let get_health = Arc::new(move || {
			{
				let cache = health_cache.read().unwrap();
				if cache.0 > Instant::now() - Duration::from_secs(1) {
					return cache.1;
				}
			}

			let health = system2.health();
			*health_cache.write().unwrap() = (Instant::now(), health);
			health
		});

		Self {
			// Static values
			_garage_build_info: meter
				.u64_observable_gauge("garage_build_info")
				.with_description("Garage build info")
				.with_callback(move |observer| {
					observer.observe(
						1,
						&[
							KeyValue::new("rustversion", garage_util::version::rust_version()),
							KeyValue::new("version", garage_util::version::garage_version()),
						],
					);
				})
				.build(),
			_replication_factor: {
				let replication_factor = system.replication_factor;
				meter
					.u64_observable_gauge("garage_replication_factor")
					.with_description("Garage replication factor setting")
					.with_callback(move |observer| {
						observer.observe(usize::from(replication_factor) as u64, &[]);
					})
					.build()
			},

			// Disk space values from System::local_status
			_disk_avail: {
				let system = system.clone();
				meter
					.u64_observable_gauge("garage_local_disk_avail")
					.with_description("Garage available disk space on each node")
					.with_callback(move |observer| {
						let st = system.local_status.read().unwrap();
						if let Some((avail, _total)) = st.data_disk_avail {
							observer.observe(avail, &[KeyValue::new("volume", "data")]);
						}
						if let Some((avail, _total)) = st.meta_disk_avail {
							observer.observe(avail, &[KeyValue::new("volume", "metadata")]);
						}
					})
					.build()
			},
			_disk_total: {
				let system = system.clone();
				meter
					.u64_observable_gauge("garage_local_disk_total")
					.with_description("Garage total disk space on each node")
					.with_callback(move |observer| {
						let st = system.local_status.read().unwrap();
						if let Some((_avail, total)) = st.data_disk_avail {
							observer.observe(total, &[KeyValue::new("volume", "data")]);
						}
						if let Some((_avail, total)) = st.meta_disk_avail {
							observer.observe(total, &[KeyValue::new("volume", "metadata")]);
						}
					})
					.build()
			},

			// Health report from System::()
			_cluster_healthy: {
				let get_health = get_health.clone();
				meter
					.u64_observable_gauge("cluster_healthy")
					.with_description("Whether all storage nodes are connected")
					.with_callback(move |observer| {
						let h = get_health();
						if h.status == ClusterHealthStatus::Healthy {
							observer.observe(1, &[]);
						} else {
							observer.observe(0, &[]);
						}
					})
					.build()
			},
			_cluster_available: {
				let get_health = get_health.clone();
				meter.u64_observable_gauge("cluster_available")
				.with_description("Whether all requests can be served, even if some storage nodes are disconnected")
				.with_callback(move |observer| {
					let h = get_health();
					if h.status != ClusterHealthStatus::Unavailable {
						observer.observe(1, &[]);
					} else {
						observer.observe(0, &[]);
					}
				})
				.build()
			},
			_known_nodes: {
				let get_health = get_health.clone();
				meter
					.u64_observable_gauge("cluster_known_nodes")
					.with_description("Number of nodes already seen once in the cluster")
					.with_callback(move |observer| {
						let h = get_health();
						observer.observe(h.known_nodes as u64, &[]);
					})
					.build()
			},
			_connected_nodes: {
				let get_health = get_health.clone();
				meter
					.u64_observable_gauge("cluster_connected_nodes")
					.with_description("Number of nodes currently connected")
					.with_callback(move |observer| {
						let h = get_health();
						observer.observe(h.connected_nodes as u64, &[]);
					})
					.build()
			},
			_storage_nodes: {
				let get_health = get_health.clone();
				meter
					.u64_observable_gauge("cluster_storage_nodes")
					.with_description("Number of storage nodes declared in the current layout")
					.with_callback(move |observer| {
						let h = get_health();
						observer.observe(h.storage_nodes as u64, &[]);
					})
					.build()
			},
			_storage_nodes_ok: {
				let get_health = get_health.clone();
				meter
					.u64_observable_gauge("cluster_storage_nodes_ok")
					.with_description("Number of storage nodes currently connected")
					.with_callback(move |observer| {
						let h = get_health();
						observer.observe(h.storage_nodes_ok as u64, &[]);
					})
					.build()
			},
			_partitions: {
				let get_health = get_health.clone();
				meter
					.u64_observable_gauge("cluster_partitions")
					.with_description("Number of partitions in the layout")
					.with_callback(move |observer| {
						let h = get_health();
						observer.observe(h.partitions as u64, &[]);
					})
					.build()
			},
			_partitions_quorum: {
				let get_health = get_health.clone();
				meter
					.u64_observable_gauge("cluster_partitions_quorum")
					.with_description(
						"Number of partitions for which we have a quorum of connected nodes",
					)
					.with_callback(move |observer| {
						let h = get_health();
						observer.observe(h.partitions_quorum as u64, &[]);
					})
					.build()
			},
			_partitions_all_ok: {
				let get_health = get_health.clone();
				meter
					.u64_observable_gauge("cluster_partitions_all_ok")
					.with_description(
						"Number of partitions for which all storage nodes are connected",
					)
					.with_callback(move |observer| {
						let h = get_health();
						observer.observe(h.partitions_all_ok as u64, &[]);
					})
					.build()
			},

			// Status report for individual cluster nodes
			_layout_node_connected: {
				let system = system.clone();
				meter
					.u64_observable_gauge("cluster_layout_node_connected")
					.with_description("Connection status for nodes in the cluster layout")
					.with_callback(move |observer| {
						let layout = system.cluster_layout();
						let nodes = system.get_known_nodes();
						for id in layout.all_nodes().unwrap_or_default().iter() {
							let mut kv = vec![KeyValue::new("id", format!("{:?}", id))];
							if let Some(role) = layout
								.current()
								.ok()
								.and_then(|l| l.roles.get(id))
								.and_then(|r| r.0.as_ref())
							{
								kv.push(KeyValue::new("role_zone", role.zone.clone()));
								match role.capacity {
									Some(cap) => {
										kv.push(KeyValue::new("role_capacity", cap as i64));
										kv.push(KeyValue::new("role_gateway", 0));
									}
									None => {
										kv.push(KeyValue::new("role_gateway", 1));
									}
								}
							}

							let value;
							if let Some(node) = nodes.iter().find(|n| n.id == *id) {
								// TODO: if we add address and hostname, and those change, we
								// get duplicate metrics, due to bad otel aggregation :(
								// Can probably be fixed when we upgrade opentelemetry
								// kv.push(KeyValue::new("address", node.addr.to_string()));
								// kv.push(KeyValue::new(
								//	"hostname",
								//	node.status.hostname.clone(),
								// ));
								value = if node.is_up { 1 } else { 0 };
							} else {
								value = 0;
							}

							observer.observe(value, &kv);
						}
					})
					.build()
			},
			_layout_node_disconnected_time: {
				let system = system.clone();
				meter
					.u64_observable_gauge("cluster_layout_node_disconnected_time")
					.with_description(
						"Time (in seconds) since last connection to nodes in the cluster layout",
					)
					.with_callback(move |observer| {
						let layout = system.cluster_layout();
						let nodes = system.get_known_nodes();
						for id in layout.all_nodes().unwrap_or_default().iter() {
							let mut kv = vec![KeyValue::new("id", format!("{:?}", id))];
							if let Some(role) = layout
								.current()
								.ok()
								.and_then(|l| l.roles.get(id))
								.and_then(|r| r.0.as_ref())
							{
								kv.push(KeyValue::new("role_zone", role.zone.clone()));
								match role.capacity {
									Some(cap) => {
										kv.push(KeyValue::new("role_capacity", cap as i64));
										kv.push(KeyValue::new("role_gateway", 0));
									}
									None => {
										kv.push(KeyValue::new("role_gateway", 1));
									}
								}
							}

							if let Some(node) = nodes.iter().find(|n| n.id == *id) {
								// TODO: see comment above
								// kv.push(KeyValue::new("address", node.addr.to_string()));
								// kv.push(KeyValue::new(
								//	"hostname",
								//	node.status.hostname.clone(),
								// ));
								if node.is_up {
									observer.observe(0, &kv);
								} else if let Some(secs) = node.last_seen_secs_ago {
									observer.observe(secs, &kv);
								}
							}
						}
					})
					.build()
			},
		}
	}
}
