use opentelemetry::{global, metrics::*, KeyValue};

use garage_db as db;
use garage_db::counted_tree_hack::CountedTree;

/// TableMetrics reference all counter used for metrics
pub struct TableMetrics {
	pub(crate) _merkle_todo_len: ValueObserver<u64>,
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
	pub fn new(table_name: &'static str, merkle_todo: db::Tree, gc_todo: CountedTree) -> Self {
		let meter = global::meter(table_name);
		TableMetrics {
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
			_gc_todo_len: meter
				.u64_value_observer(
					"table.gc_todo_queue_length",
					move |observer| {
						observer.observe(
							gc_todo.len() as u64,
							&[KeyValue::new("table_name", table_name)],
						);
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
