use std::sync::Arc;

use tokio::sync::Semaphore;

use opentelemetry::{global, metrics::*};

use garage_db as db;

/// `TableMetrics` reference all counter used for metrics
pub struct BlockManagerMetrics {
	pub(crate) _compression_level: ValueObserver<u64>,
	pub(crate) _rc_size: ValueObserver<u64>,
	pub(crate) _resync_queue_len: ValueObserver<u64>,
	pub(crate) _resync_errored_blocks: ValueObserver<u64>,
	pub(crate) _buffer_free_kb: ValueObserver<u64>,

	pub(crate) resync_counter: BoundCounter<u64>,
	pub(crate) resync_error_counter: BoundCounter<u64>,
	pub(crate) resync_duration: BoundValueRecorder<f64>,
	pub(crate) resync_send_counter: Counter<u64>,
	pub(crate) resync_recv_counter: BoundCounter<u64>,

	pub(crate) bytes_read: BoundCounter<u64>,
	pub(crate) block_read_duration: BoundValueRecorder<f64>,
	pub(crate) block_read_semaphore_timeouts: BoundCounter<u64>,
	pub(crate) bytes_written: BoundCounter<u64>,
	pub(crate) block_write_duration: BoundValueRecorder<f64>,
	pub(crate) delete_counter: BoundCounter<u64>,

	pub(crate) corruption_counter: BoundCounter<u64>,
}

impl BlockManagerMetrics {
	pub fn new(
		compression_level: Option<i32>,
		rc_tree: db::Tree,
		resync_queue: db::Tree,
		resync_errors: db::Tree,
		buffer_semaphore: Arc<Semaphore>,
	) -> Self {
		let meter = global::meter("garage_model/block");
		Self {
			_compression_level: meter
				.u64_value_observer("garage_block.compression_level", move |observer| {
					match compression_level {
						Some(v) => observer.observe(v as u64, &[]),
						None => observer.observe(0_u64, &[]),
					}
				})
				.with_description("Garage compression level for node")
				.init(),
			_rc_size: meter
				.u64_value_observer("garage_block.rc_size", move |observer| {
					if let Ok(value) = rc_tree.approximate_len() {
						observer.observe(value as u64, &[]);
					}
				})
				.with_description("Number of blocks known to the reference counter")
				.init(),
			_resync_queue_len: meter
				.u64_value_observer("garage_block.resync_queue_length", move |observer| {
					if let Ok(value) = resync_queue.approximate_len() {
						observer.observe(value as u64, &[]);
					}
				})
				.with_description(
					"Number of block hashes queued for local check and possible resync",
				)
				.init(),
			_resync_errored_blocks: meter
				.u64_value_observer("garage_block.resync_errored_blocks", move |observer| {
					if let Ok(value) = resync_errors.approximate_len() {
						observer.observe(value as u64, &[]);
					}
				})
				.with_description("Number of block hashes whose last resync resulted in an error")
				.init(),

			_buffer_free_kb: meter
				.u64_value_observer("garage_block.ram_buffer_free_kb", move |observer| {
					observer.observe(buffer_semaphore.available_permits() as u64, &[]);
				})
				.with_description(
					"Available RAM in KiB to use for buffering data blocks to be written to remote nodes",
				)
				.init(),

			resync_counter: meter
				.u64_counter("garage_block.resync_count")
				.with_description("Number of calls to resync_block")
				.init()
				.bind(&[]),
			resync_error_counter: meter
				.u64_counter("garage_block.resync_error_count")
				.with_description("Number of calls to resync_block that returned an error")
				.init()
				.bind(&[]),
			resync_duration: meter
				.f64_value_recorder("garage_block.resync_duration")
				.with_description("Duration of resync_block operations")
				.init()
				.bind(&[]),
			resync_send_counter: meter
				.u64_counter("garage_block.resync_send_count")
				.with_description("Number of blocks sent to another node in resync operations")
				.init(),
			resync_recv_counter: meter
				.u64_counter("garage_block.resync_recv_count")
				.with_description("Number of blocks received from other nodes in resync operations")
				.init()
				.bind(&[]),

			bytes_read: meter
				.u64_counter("garage_block.bytes_read")
				.with_description("Number of bytes read from disk")
				.init()
				.bind(&[]),
			block_read_duration: meter
				.f64_value_recorder("garage_block.read_duration")
				.with_description("Duration of block read operations")
				.init()
				.bind(&[]),
			block_read_semaphore_timeouts: meter
				.u64_counter("garage_block.read_semaphore_timeouts")
				.with_description("Number of block reads that failed due to semaphore acquire timeout")
				.init()
				.bind(&[]),
			bytes_written: meter
				.u64_counter("garage_block.bytes_written")
				.with_description("Number of bytes written to disk")
				.init()
				.bind(&[]),
			block_write_duration: meter
				.f64_value_recorder("garage_block.write_duration")
				.with_description("Duration of block write operations")
				.init()
				.bind(&[]),
			delete_counter: meter
				.u64_counter("garage_block.delete_count")
				.with_description("Number of blocks deleted")
				.init()
				.bind(&[]),

			corruption_counter: meter
				.u64_counter("garage_block.corruption_count")
				.with_description("Data corruptions detected on block reads")
				.init()
				.bind(&[]),
		}
	}
}
