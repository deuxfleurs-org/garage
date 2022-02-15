use opentelemetry::{global, metrics::*, KeyValue};

/// TableMetrics reference all counter used for metrics
pub struct TableMetrics {
	merkle_updater_todo_queue_length: ValueObserver<u64>,
}
impl TableMetrics {
	pub fn new(table_name: &'static str, merkle_todo: sled::Tree) -> Self {
		let meter = global::meter(table_name);
		TableMetrics {
			merkle_updater_todo_queue_length: meter
				.u64_value_observer(
					format!("merkle_updater_todo_queue_length"),
					move |observer| {
						observer.observe(
							merkle_todo.len() as u64,
							&[KeyValue::new("table_name", table_name)],
						)
					},
				)
				.with_description("Bucket merkle updater TODO queue length")
				.init(),
		}
	}
}
