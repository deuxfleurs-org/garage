use std::sync::{Arc, RwLock};

use opentelemetry::{global, metrics::*, KeyValue};

use crate::system::NodeStatus;

/// TableMetrics reference all counter used for metrics
pub struct SystemMetrics {
	pub(crate) _garage_build_info: ValueObserver<u64>,
	pub(crate) _replication_factor: ValueObserver<u64>,
	pub(crate) _disk_avail: ValueObserver<u64>,
	pub(crate) _disk_total: ValueObserver<u64>,
}

impl SystemMetrics {
	pub fn new(replication_factor: usize, local_status: Arc<RwLock<NodeStatus>>) -> Self {
		let meter = global::meter("garage_system");
		let st1 = local_status.clone();
		let st2 = local_status.clone();
		Self {
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
			_disk_avail: meter
				.u64_value_observer("garage_local_disk_avail", move |observer| {
					let st = st1.read().unwrap();
					if let Some((avail, _total)) = st.data_disk_avail {
						observer.observe(avail, &[KeyValue::new("volume", "data")]);
					}
					if let Some((avail, _total)) = st.meta_disk_avail {
						observer.observe(avail, &[KeyValue::new("volume", "metadata")]);
					}
				})
				.with_description("Garage available disk space on each node")
				.init(),
			_disk_total: meter
				.u64_value_observer("garage_local_disk_total", move |observer| {
					let st = st2.read().unwrap();
					if let Some((_avail, total)) = st.data_disk_avail {
						observer.observe(total, &[KeyValue::new("volume", "data")]);
					}
					if let Some((_avail, total)) = st.meta_disk_avail {
						observer.observe(total, &[KeyValue::new("volume", "metadata")]);
					}
				})
				.with_description("Garage total disk space on each node")
				.init(),
		}
	}
}
