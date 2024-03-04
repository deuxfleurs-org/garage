use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use opentelemetry::{global, metrics::*, KeyValue};

use crate::system::{ClusterHealthStatus, System};

/// TableMetrics reference all counter used for metrics
pub struct SystemMetrics {
	// Static values
	pub(crate) _garage_build_info: ValueObserver<u64>,
	pub(crate) _replication_factor: ValueObserver<u64>,

	// Disk space values from System::local_status
	pub(crate) _disk_avail: ValueObserver<u64>,
	pub(crate) _disk_total: ValueObserver<u64>,

	// Health report from System::health()
	pub(crate) _cluster_healthy: ValueObserver<u64>,
	pub(crate) _cluster_available: ValueObserver<u64>,
	pub(crate) _known_nodes: ValueObserver<u64>,
	pub(crate) _connected_nodes: ValueObserver<u64>,
	pub(crate) _storage_nodes: ValueObserver<u64>,
	pub(crate) _storage_nodes_ok: ValueObserver<u64>,
	pub(crate) _partitions: ValueObserver<u64>,
	pub(crate) _partitions_quorum: ValueObserver<u64>,
	pub(crate) _partitions_all_ok: ValueObserver<u64>,

	// Status report for individual cluster nodes
	pub(crate) _layout_node_connected: ValueObserver<u64>,
	pub(crate) _layout_node_disconnected_time: ValueObserver<u64>,
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
				.u64_value_observer("garage_build_info", move |observer| {
					observer.observe(
						1,
						&[
							KeyValue::new("rustversion", garage_util::version::rust_version()),
							KeyValue::new("version", garage_util::version::garage_version()),
						],
					)
				})
				.with_description("Garage build info")
				.init(),
			_replication_factor: {
				let replication_factor = system.replication_factor;
				meter
					.u64_value_observer("garage_replication_factor", move |observer| {
						observer.observe(replication_factor.replication_factor() as u64, &[])
					})
					.with_description("Garage replication factor setting")
					.init()
			},

			// Disk space values from System::local_status
			_disk_avail: {
				let system = system.clone();
				meter
					.u64_value_observer("garage_local_disk_avail", move |observer| {
						let st = system.local_status.read().unwrap();
						if let Some((avail, _total)) = st.data_disk_avail {
							observer.observe(avail, &[KeyValue::new("volume", "data")]);
						}
						if let Some((avail, _total)) = st.meta_disk_avail {
							observer.observe(avail, &[KeyValue::new("volume", "metadata")]);
						}
					})
					.with_description("Garage available disk space on each node")
					.init()
			},
			_disk_total: {
				let system = system.clone();
				meter
					.u64_value_observer("garage_local_disk_total", move |observer| {
						let st = system.local_status.read().unwrap();
						if let Some((_avail, total)) = st.data_disk_avail {
							observer.observe(total, &[KeyValue::new("volume", "data")]);
						}
						if let Some((_avail, total)) = st.meta_disk_avail {
							observer.observe(total, &[KeyValue::new("volume", "metadata")]);
						}
					})
					.with_description("Garage total disk space on each node")
					.init()
			},

			// Health report from System::()
			_cluster_healthy: {
				let get_health = get_health.clone();
				meter
					.u64_value_observer("cluster_healthy", move |observer| {
						let h = get_health();
						if h.status == ClusterHealthStatus::Healthy {
							observer.observe(1, &[]);
						} else {
							observer.observe(0, &[]);
						}
					})
					.with_description("Whether all storage nodes are connected")
					.init()
			},
			_cluster_available: {
				let get_health = get_health.clone();
				meter.u64_value_observer("cluster_available", move |observer| {
					let h = get_health();
					if h.status != ClusterHealthStatus::Unavailable {
						observer.observe(1, &[]);
					} else {
						observer.observe(0, &[]);
					}
				})
				.with_description("Whether all requests can be served, even if some storage nodes are disconnected")
				.init()
			},
			_known_nodes: {
				let get_health = get_health.clone();
				meter
					.u64_value_observer("cluster_known_nodes", move |observer| {
						let h = get_health();
						observer.observe(h.known_nodes as u64, &[]);
					})
					.with_description("Number of nodes already seen once in the cluster")
					.init()
			},
			_connected_nodes: {
				let get_health = get_health.clone();
				meter
					.u64_value_observer("cluster_connected_nodes", move |observer| {
						let h = get_health();
						observer.observe(h.connected_nodes as u64, &[]);
					})
					.with_description("Number of nodes currently connected")
					.init()
			},
			_storage_nodes: {
				let get_health = get_health.clone();
				meter
					.u64_value_observer("cluster_storage_nodes", move |observer| {
						let h = get_health();
						observer.observe(h.storage_nodes as u64, &[]);
					})
					.with_description("Number of storage nodes declared in the current layout")
					.init()
			},
			_storage_nodes_ok: {
				let get_health = get_health.clone();
				meter
					.u64_value_observer("cluster_storage_nodes_ok", move |observer| {
						let h = get_health();
						observer.observe(h.storage_nodes_ok as u64, &[]);
					})
					.with_description("Number of storage nodes currently connected")
					.init()
			},
			_partitions: {
				let get_health = get_health.clone();
				meter
					.u64_value_observer("cluster_partitions", move |observer| {
						let h = get_health();
						observer.observe(h.partitions as u64, &[]);
					})
					.with_description("Number of partitions in the layout")
					.init()
			},
			_partitions_quorum: {
				let get_health = get_health.clone();
				meter
					.u64_value_observer("cluster_partitions_quorum", move |observer| {
						let h = get_health();
						observer.observe(h.partitions_quorum as u64, &[]);
					})
					.with_description(
						"Number of partitions for which we have a quorum of connected nodes",
					)
					.init()
			},
			_partitions_all_ok: {
				let get_health = get_health.clone();
				meter
					.u64_value_observer("cluster_partitions_all_ok", move |observer| {
						let h = get_health();
						observer.observe(h.partitions_all_ok as u64, &[]);
					})
					.with_description(
						"Number of partitions for which all storage nodes are connected",
					)
					.init()
			},

			// Status report for individual cluster nodes
			_layout_node_connected: {
				let system = system.clone();
				meter
					.u64_value_observer("cluster_layout_node_connected", move |observer| {
						let layout = system.cluster_layout();
						let nodes = system.get_known_nodes();
						for id in layout.all_nodes().iter() {
							let mut kv = vec![KeyValue::new("id", format!("{:?}", id))];
							if let Some(role) =
								layout.current().roles.get(id).and_then(|r| r.0.as_ref())
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
					.with_description("Connection status for nodes in the cluster layout")
					.init()
			},
			_layout_node_disconnected_time: {
				let system = system.clone();
				meter
					.u64_value_observer("cluster_layout_node_disconnected_time", move |observer| {
						let layout = system.cluster_layout();
						let nodes = system.get_known_nodes();
						for id in layout.all_nodes().iter() {
							let mut kv = vec![KeyValue::new("id", format!("{:?}", id))];
							if let Some(role) =
								layout.current().roles.get(id).and_then(|r| r.0.as_ref())
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
					.with_description(
						"Time (in seconds) since last connection to nodes in the cluster layout",
					)
					.init()
			},
		}
	}
}
