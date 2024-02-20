use std::sync::{Arc, RwLock};

use opentelemetry::{global, metrics::*, KeyValue};

use crate::system::{ClusterHealth, ClusterHealthStatus, NodeStatus};

/// TableMetrics reference all counter used for metrics
pub struct SystemMetrics {
	pub(crate) health: Arc<RwLock<Option<ClusterHealth>>>,

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
}

impl SystemMetrics {
	pub fn new(replication_factor: usize, local_status: Arc<RwLock<NodeStatus>>) -> Self {
		let meter = global::meter("garage_system");
		let health = Arc::new(RwLock::new(None));
		Self {
			health: health.clone(),

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
			_replication_factor: meter
				.u64_value_observer("garage_replication_factor", move |observer| {
					observer.observe(replication_factor as u64, &[])
				})
				.with_description("Garage replication factor setting")
				.init(),

			// Disk space values from System::local_status
			_disk_avail: {
				let status = local_status.clone();
				meter
					.u64_value_observer("garage_local_disk_avail", move |observer| {
						let st = status.read().unwrap();
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
				let status = local_status.clone();
				meter
					.u64_value_observer("garage_local_disk_total", move |observer| {
						let st = status.read().unwrap();
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

			// Health report from System::health()
			_cluster_healthy: {
				let health = health.clone();
				meter
					.u64_value_observer("cluster_healthy", move |observer| {
						if let Some(h) = health.read().unwrap().as_ref() {
							if h.status == ClusterHealthStatus::Healthy {
								observer.observe(1, &[]);
							} else {
								observer.observe(0, &[]);
							}
						}
					})
					.with_description("Whether all storage nodes are connected")
					.init()
			},
			_cluster_available: {
				let health = health.clone();
				meter.u64_value_observer("cluster_available", move |observer| {
					if let Some(h) = health.read().unwrap().as_ref() {
						if h.status != ClusterHealthStatus::Unavailable {
							observer.observe(1, &[]);
						} else {
							observer.observe(0, &[]);
						}
					}
				})
				.with_description("Whether all requests can be served, even if some storage nodes are disconnected")
				.init()
			},
			_known_nodes: {
				let health = health.clone();
				meter
					.u64_value_observer("cluster_known_nodes", move |observer| {
						if let Some(h) = health.read().unwrap().as_ref() {
							observer.observe(h.known_nodes as u64, &[]);
						}
					})
					.with_description("Number of nodes already seen once in the cluster")
					.init()
			},
			_connected_nodes: {
				let health = health.clone();
				meter
					.u64_value_observer("cluster_connected_nodes", move |observer| {
						if let Some(h) = health.read().unwrap().as_ref() {
							observer.observe(h.connected_nodes as u64, &[]);
						}
					})
					.with_description("Number of nodes currently connected")
					.init()
			},
			_storage_nodes: {
				let health = health.clone();
				meter
					.u64_value_observer("cluster_storage_nodes", move |observer| {
						if let Some(h) = health.read().unwrap().as_ref() {
							observer.observe(h.storage_nodes as u64, &[]);
						}
					})
					.with_description("Number of storage nodes declared in the current layout")
					.init()
			},
			_storage_nodes_ok: {
				let health = health.clone();
				meter
					.u64_value_observer("cluster_storage_nodes_ok", move |observer| {
						if let Some(h) = health.read().unwrap().as_ref() {
							observer.observe(h.storage_nodes_ok as u64, &[]);
						}
					})
					.with_description("Number of storage nodes currently connected")
					.init()
			},
			_partitions: {
				let health = health.clone();
				meter
					.u64_value_observer("cluster_partitions", move |observer| {
						if let Some(h) = health.read().unwrap().as_ref() {
							observer.observe(h.partitions as u64, &[]);
						}
					})
					.with_description("Number of partitions in the layout")
					.init()
			},
			_partitions_quorum: {
				let health = health.clone();
				meter
					.u64_value_observer("cluster_partitions_quorum", move |observer| {
						if let Some(h) = health.read().unwrap().as_ref() {
							observer.observe(h.partitions_quorum as u64, &[]);
						}
					})
					.with_description(
						"Number of partitions for which we have a quorum of connected nodes",
					)
					.init()
			},
			_partitions_all_ok: {
				let health = health.clone();
				meter
					.u64_value_observer("cluster_partitions_all_ok", move |observer| {
						if let Some(h) = health.read().unwrap().as_ref() {
							observer.observe(h.partitions_all_ok as u64, &[]);
						}
					})
					.with_description(
						"Number of partitions for which all storage nodes are connected",
					)
					.init()
			},
		}
	}
}
