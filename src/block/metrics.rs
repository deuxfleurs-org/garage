use std::{convert::TryInto, sync::Arc};

use tokio::sync::Semaphore;

use opentelemetry::{global, metrics::*};

use garage_db as db;

use crate::resync::IndexedQueue;

/// `TableMetrics` reference all counter used for metrics
pub struct BlockManagerMetrics {
	pub(crate) _compression_level: ObservableGauge<u64>,
	pub(crate) _rc_size: ObservableGauge<u64>,
	pub(crate) _resync_queue_len: ObservableGauge<u64>,
	pub(crate) _resync_errored_blocks: ObservableGauge<u64>,
	pub(crate) _buffer_free_kb: ObservableGauge<u64>,

	pub(crate) resync_counter: Counter<u64>,
	pub(crate) resync_error_counter: Counter<u64>,
	pub(crate) resync_duration: Histogram<f64>,
	pub(crate) resync_send_counter: Counter<u64>,
	pub(crate) resync_recv_counter: Counter<u64>,

	pub(crate) bytes_read: Counter<u64>,
	pub(crate) block_read_duration: Histogram<f64>,
	pub(crate) block_read_semaphore_timeouts: Counter<u64>,
	pub(crate) bytes_written: Counter<u64>,
	pub(crate) block_write_duration: Histogram<f64>,
	pub(crate) delete_counter: Counter<u64>,

	pub(crate) corruption_counter: Counter<u64>,
}

impl BlockManagerMetrics {
	pub fn new(
		compression_level: Option<i32>,
		rc_tree: db::Tree,
		resync_queue: Arc<std::sync::Mutex<IndexedQueue>>,
		buffer_semaphore: Arc<Semaphore>,
	) -> Self {
		let meter = global::meter("garage_model/block");
		Self {
			_compression_level: meter
				.u64_observable_gauge("block.compression_level")
				.with_description("Garage compression level for node")
				.with_callback(move |observer| match compression_level {
					Some(v) => observer.observe(v as u64, &[]),
					None => observer.observe(0_u64, &[]),
				})
				.build(),
			_rc_size: meter
				.u64_observable_gauge("block.rc_size")
				.with_description("Number of blocks known to the reference counter")
				.with_callback(move |observer| {
					if let Ok(value) = rc_tree.approximate_len() {
						observer.observe(value as u64, &[]);
					}
				})
				.build(),
			_resync_queue_len: {
				let resync_queue = resync_queue.clone();
				meter
				.u64_observable_gauge("block.resync_queue_length")
				.with_description(
					"Number of block hashes queued for local check and possible resync",
				)
				.with_callback(move |observer| {
					let len = resync_queue.lock().unwrap().approximate_len().unwrap_or_default();
						observer.observe(len.try_into().unwrap(), &[]);
				})
				.build()
			},
			_resync_errored_blocks: meter
				.u64_observable_gauge("block.resync_errored_blocks")
				.with_description("Number of block hashes whose last resync resulted in an error")
				.with_callback(move |observer| {
					let errs = resync_queue.lock().unwrap().errored();
						observer.observe(errs, &[]);
				})
				.build(),

			_buffer_free_kb: meter
				.u64_observable_gauge("block.ram_buffer_free_kb")
				.with_description(
					"Available RAM in KiB to use for buffering data blocks to be written to remote nodes",
				)
				.with_callback(move |observer| {
					observer.observe(buffer_semaphore.available_permits() as u64, &[]);
				})
				.build(),

			resync_counter: meter
				.u64_counter("block.resync_counter")
				.with_description("Number of calls to resync_block")
				.build(),
			resync_error_counter: meter
				.u64_counter("block.resync_error_counter")
				.with_description("Number of calls to resync_block that returned an error")
				.build(),
			resync_duration: meter
				.f64_histogram("block.resync_duration")
				.with_description("Duration of resync_block operations")
				.build(),
			resync_send_counter: meter
				.u64_counter("block.resync_send_counter")
				.with_description("Number of blocks sent to another node in resync operations")
				.build(),
			resync_recv_counter: meter
				.u64_counter("block.resync_recv_counter")
				.with_description("Number of blocks received from other nodes in resync operations")
				.build(),

			bytes_read: meter
				.u64_counter("block.bytes_read")
				.with_description("Number of bytes read from disk")
				.build(),
			block_read_duration: meter
				.f64_histogram("block.read_duration")
				.with_description("Duration of block read operations")
				.build(),
			block_read_semaphore_timeouts: meter
				.u64_counter("block.read_semaphore_timeouts")
				.with_description("Number of block reads that failed due to semaphore acquire timeout")
				.build(),
			bytes_written: meter
				.u64_counter("block.bytes_written")
				.with_description("Number of bytes written to disk")
				.build(),
			block_write_duration: meter
				.f64_histogram("block.write_duration")
				.with_description("Duration of block write operations")
				.build(),
			delete_counter: meter
				.u64_counter("block.delete_counter")
				.with_description("Number of blocks deleted")
				.build(),

			corruption_counter: meter
				.u64_counter("block.corruption_counter")
				.with_description("Data corruptions detected on block reads")
				.build(),
		}
	}
}
