use std::sync::Arc;

use opentelemetry::{global, metrics::*};
use tokio::sync::Semaphore;

/// TableMetrics reference all counter used for metrics
pub struct RpcMetrics {
	pub(crate) _rpc_available_permits: ValueObserver<u64>,

	pub(crate) rpc_counter: Counter<u64>,
	pub(crate) rpc_timeout_counter: Counter<u64>,
	pub(crate) rpc_netapp_error_counter: Counter<u64>,
	pub(crate) rpc_garage_error_counter: Counter<u64>,

	pub(crate) rpc_duration: ValueRecorder<f64>,
	pub(crate) rpc_queueing_time: ValueRecorder<f64>,
}
impl RpcMetrics {
	pub fn new(sem: Arc<Semaphore>) -> Self {
		let meter = global::meter("garage_rpc");
		RpcMetrics {
			_rpc_available_permits: meter
				.u64_value_observer("rpc.available_permits", move |observer| {
					observer.observe(sem.available_permits() as u64, &[])
				})
				.with_description("Number of available RPC permits")
				.init(),

			rpc_counter: meter
				.u64_counter("rpc.request_counter")
				.with_description("Number of RPC requests emitted")
				.init(),
			rpc_timeout_counter: meter
				.u64_counter("rpc.timeout_counter")
				.with_description("Number of RPC timeouts")
				.init(),
			rpc_netapp_error_counter: meter
				.u64_counter("rpc.netapp_error_counter")
				.with_description("Number of communication errors (errors in the Netapp library)")
				.init(),
			rpc_garage_error_counter: meter
				.u64_counter("rpc.garage_error_counter")
				.with_description("Number of RPC errors (errors happening when handling the RPC)")
				.init(),
			rpc_duration: meter
				.f64_value_recorder("rpc.duration")
				.with_description("Duration of RPCs")
				.init(),
			rpc_queueing_time: meter
				.f64_value_recorder("rpc.queueing_time")
				.with_description("Time RPC requests were queued for before being sent")
				.init(),
		}
	}
}
