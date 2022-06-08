use opentelemetry::{global, metrics::*};

use garage_db::counted_tree_hack::CountedTree;

/// TableMetrics reference all counter used for metrics
pub struct BlockManagerMetrics {
	pub(crate) _resync_queue_len: ValueObserver<u64>,
	pub(crate) _resync_errored_blocks: ValueObserver<u64>,

	pub(crate) resync_counter: BoundCounter<u64>,
	pub(crate) resync_error_counter: BoundCounter<u64>,
	pub(crate) resync_duration: BoundValueRecorder<f64>,
	pub(crate) resync_send_counter: Counter<u64>,
	pub(crate) resync_recv_counter: BoundCounter<u64>,

	pub(crate) bytes_read: BoundCounter<u64>,
	pub(crate) block_read_duration: BoundValueRecorder<f64>,
	pub(crate) bytes_written: BoundCounter<u64>,
	pub(crate) block_write_duration: BoundValueRecorder<f64>,
	pub(crate) delete_counter: BoundCounter<u64>,

	pub(crate) corruption_counter: BoundCounter<u64>,
}

impl BlockManagerMetrics {
	pub fn new(resync_queue: CountedTree, resync_errors: CountedTree) -> Self {
		let meter = global::meter("garage_model/block");
		Self {
			_resync_queue_len: meter
				.u64_value_observer("block.resync_queue_length", move |observer| {
					observer.observe(resync_queue.len() as u64, &[])
				})
				.with_description(
					"Number of block hashes queued for local check and possible resync",
				)
				.init(),
			_resync_errored_blocks: meter
				.u64_value_observer("block.resync_errored_blocks", move |observer| {
					observer.observe(resync_errors.len() as u64, &[])
				})
				.with_description("Number of block hashes whose last resync resulted in an error")
				.init(),

			resync_counter: meter
				.u64_counter("block.resync_counter")
				.with_description("Number of calls to resync_block")
				.init()
				.bind(&[]),
			resync_error_counter: meter
				.u64_counter("block.resync_error_counter")
				.with_description("Number of calls to resync_block that returned an error")
				.init()
				.bind(&[]),
			resync_duration: meter
				.f64_value_recorder("block.resync_duration")
				.with_description("Duration of resync_block operations")
				.init()
				.bind(&[]),
			resync_send_counter: meter
				.u64_counter("block.resync_send_counter")
				.with_description("Number of blocks sent to another node in resync operations")
				.init(),
			resync_recv_counter: meter
				.u64_counter("block.resync_recv_counter")
				.with_description("Number of blocks received from other nodes in resync operations")
				.init()
				.bind(&[]),

			bytes_read: meter
				.u64_counter("block.bytes_read")
				.with_description("Number of bytes read from disk")
				.init()
				.bind(&[]),
			block_read_duration: meter
				.f64_value_recorder("block.read_duration")
				.with_description("Duration of block read operations")
				.init()
				.bind(&[]),
			bytes_written: meter
				.u64_counter("block.bytes_written")
				.with_description("Number of bytes written to disk")
				.init()
				.bind(&[]),
			block_write_duration: meter
				.f64_value_recorder("block.write_duration")
				.with_description("Duration of block write operations")
				.init()
				.bind(&[]),
			delete_counter: meter
				.u64_counter("block.delete_counter")
				.with_description("Number of blocks deleted")
				.init()
				.bind(&[]),

			corruption_counter: meter
				.u64_counter("block.corruption_counter")
				.with_description("Data corruptions detected on block reads")
				.init()
				.bind(&[]),
		}
	}
}
