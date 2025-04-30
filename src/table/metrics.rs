use opentelemetry::{global, metrics::*, KeyValue};
use std::sync::{Arc, Mutex};

use garage_db as db;

/// TableMetrics reference all counter used for metrics
pub struct TableMetrics {
	pub(crate) _table_size: ValueObserver<u64>,
	pub(crate) _merkle_tree_size: ValueObserver<u64>,
	pub(crate) _merkle_todo_len: ValueObserver<u64>,
	pub(crate) _merkle_todo_sleep_ms: ValueObserver<f64>,
	pub(crate) _gc_todo_len: ValueObserver<u64>,

	pub(crate) get_request_counter: BoundCounter<u64>,
	pub(crate) get_request_duration: BoundValueRecorder<f64>,
	pub(crate) put_request_counter: BoundCounter<u64>,
	pub(crate) put_request_duration: BoundValueRecorder<f64>,

	pub(crate) internal_update_counter: BoundCounter<u64>,
	pub(crate) internal_delete_counter: BoundCounter<u64>,

	pub(crate) sync_items_sent: Counter<u64>,
	pub(crate) sync_items_received: Counter<u64>,
}
impl TableMetrics {
	pub fn new(
		table_name: &'static str,
		store: db::Tree,
		merkle_tree: db::Tree,
		merkle_todo: db::Tree,
		merkle_todo_sleep: Arc<Mutex<std::time::Duration>>,
		gc_todo: db::Tree,
	) -> Self {
		let meter = global::meter(table_name);
		TableMetrics {
			_table_size: meter
				.u64_value_observer(
					"table.size",
					move |observer| {
						if let Ok(value) = store.len() {
							observer.observe(
								value as u64,
								&[KeyValue::new("table_name", table_name)],
							);
						}
					},
				)
				.with_description("Number of items in table")
				.init(),
			_merkle_tree_size: meter
				.u64_value_observer(
					"table.merkle_tree_size",
					move |observer| {
						if let Ok(value) = merkle_tree.len() {
							observer.observe(
								value as u64,
								&[KeyValue::new("table_name", table_name)],
							);
						}
					},
				)
				.with_description("Number of nodes in table's Merkle tree")
				.init(),
			_merkle_todo_len: meter
				.u64_value_observer(
					"table.merkle_updater_todo_queue_length",
					move |observer| {
						if let Ok(v) = merkle_todo.len() {
							observer.observe(
								v as u64,
								&[KeyValue::new("table_name", table_name)],
							);
						}
					},
				)
				.with_description("Merkle tree updater TODO queue length")
				.init(),
                        _merkle_todo_sleep_ms: meter
                            .f64_value_observer(
                                "table.merkle_updater_todo_queue_backpressure_ms",
                                move |observer| {
                                    let bp_ref = merkle_todo_sleep.clone();
                                    let bp_val = bp_ref.lock().unwrap();
                                    let bp_millis: f64 = bp_val.as_micros() as f64 / 1000.0f64;
                                    observer.observe(bp_millis, &[KeyValue::new("table_name", table_name)])
                                }
                            )
			    .with_description("Merkle tree updater TODO sleep backpressure to apply in ms")
			    .init(),
			_gc_todo_len: meter
				.u64_value_observer(
					"table.gc_todo_queue_length",
					move |observer| {
                        if let Ok(value) = gc_todo.len() {
                            observer.observe(
                                value as u64,
                                &[KeyValue::new("table_name", table_name)],
                            );
                        }
					},
				)
				.with_description("Table garbage collector TODO queue length")
				.init(),

			get_request_counter: meter
				.u64_counter("table.get_request_counter")
				.with_description("Number of get/get_range requests internally made on this table")
				.init()
				.bind(&[KeyValue::new("table_name", table_name)]),
			get_request_duration: meter
				.f64_value_recorder("table.get_request_duration")
				.with_description("Duration of get/get_range requests internally made on this table, in seconds")
				.init()
				.bind(&[KeyValue::new("table_name", table_name)]),
			put_request_counter: meter
				.u64_counter("table.put_request_counter")
				.with_description("Number of insert/insert_many requests internally made on this table")
				.init()
				.bind(&[KeyValue::new("table_name", table_name)]),
			put_request_duration: meter
				.f64_value_recorder("table.put_request_duration")
				.with_description("Duration of insert/insert_many requests internally made on this table, in seconds")
				.init()
				.bind(&[KeyValue::new("table_name", table_name)]),

			internal_update_counter: meter
				.u64_counter("table.internal_update_counter")
				.with_description("Number of value updates where the value actually changes (includes creation of new key and update of existing key)")
				.init()
				.bind(&[KeyValue::new("table_name", table_name)]),
			internal_delete_counter: meter
				.u64_counter("table.internal_delete_counter")
				.with_description("Number of value deletions in the tree (due to GC or repartitioning)")
				.init()
				.bind(&[KeyValue::new("table_name", table_name)]),

			sync_items_sent: meter
				.u64_counter("table.sync_items_sent")
				.with_description("Number of data items sent to other nodes during resync procedures")
				.init(),
			sync_items_received: meter
				.u64_counter("table.sync_items_received")
				.with_description("Number of data items received from other nodes during resync procedures")
				.init(),
		}
	}
}
