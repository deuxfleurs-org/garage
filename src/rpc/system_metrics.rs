use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use opentelemetry::{global, metrics::*, KeyValue};

/// TableMetrics reference all counter used for metrics
pub struct SystemMetrics {
	pub(crate) _garage_build_info: ValueObserver<u64>,
	pub(crate) _replication_factor: ValueObserver<u64>,
	pub(crate) _disk_avail: ValueObserver<u64>,
	pub(crate) _disk_total: ValueObserver<u64>,
	pub(crate) values: Arc<SystemMetricsValues>,
}

#[derive(Default)]
pub struct SystemMetricsValues {
	pub(crate) data_disk_total: AtomicU64,
	pub(crate) data_disk_avail: AtomicU64,
	pub(crate) meta_disk_total: AtomicU64,
	pub(crate) meta_disk_avail: AtomicU64,
}

impl SystemMetrics {
	pub fn new(replication_factor: usize) -> Self {
		let meter = global::meter("garage_system");
		let values = Arc::new(SystemMetricsValues::default());
		let values1 = values.clone();
		let values2 = values.clone();
		Self {
			_garage_build_info: meter
				.u64_value_observer("garage_build_info", move |observer| {
					observer.observe(
						1,
						&[KeyValue::new(
							"version",
							garage_util::version::garage_version(),
						)],
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
					match values1.data_disk_avail.load(Ordering::Relaxed) {
						0 => (),
						x => observer.observe(x, &[KeyValue::new("volume", "data")]),
					};
					match values1.meta_disk_avail.load(Ordering::Relaxed) {
						0 => (),
						x => observer.observe(x, &[KeyValue::new("volume", "metadata")]),
					};
				})
				.with_description("Garage available disk space on each node")
				.init(),
			_disk_total: meter
				.u64_value_observer("garage_local_disk_total", move |observer| {
					match values2.data_disk_total.load(Ordering::Relaxed) {
						0 => (),
						x => observer.observe(x, &[KeyValue::new("volume", "data")]),
					};
					match values2.meta_disk_total.load(Ordering::Relaxed) {
						0 => (),
						x => observer.observe(x, &[KeyValue::new("volume", "metadata")]),
					};
				})
				.with_description("Garage total disk space on each node")
				.init(),
			values,
		}
	}
}
