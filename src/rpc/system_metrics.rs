use opentelemetry::{global, metrics::*, KeyValue};

/// TableMetrics reference all counter used for metrics
pub struct SystemMetrics {
	pub(crate) _garage_build_info: ValueObserver<u64>,
	pub(crate) _replication_factor: ValueObserver<u64>,
}

impl SystemMetrics {
	pub fn new(replication_factor: usize) -> Self {
		let meter = global::meter("garage_system");
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
		}
	}
}
