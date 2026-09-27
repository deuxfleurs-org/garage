use opentelemetry::{global, metrics::*};

/// `TableMetrics` reference all counter used for metrics
pub struct RpcMetrics {
	pub(crate) rpc_counter: Counter<u64>,
	pub(crate) rpc_timeout_counter: Counter<u64>,
	pub(crate) rpc_netapp_error_counter: Counter<u64>,
	pub(crate) rpc_garage_error_counter: Counter<u64>,

	pub(crate) rpc_duration: Histogram<f64>,
}
impl RpcMetrics {
	pub fn new() -> Self {
		let meter = global::meter("garage_rpc");
		RpcMetrics {
			rpc_counter: meter
				.u64_counter("rpc.request_counter")
				.with_description("Number of RPC requests emitted")
				.build(),
			rpc_timeout_counter: meter
				.u64_counter("rpc.timeout_counter")
				.with_description("Number of RPC timeouts")
				.build(),
			rpc_netapp_error_counter: meter
				.u64_counter("rpc.netapp_error_counter")
				.with_description("Number of communication errors (errors in the Netapp library)")
				.build(),
			rpc_garage_error_counter: meter
				.u64_counter("rpc.garage_error_counter")
				.with_description("Number of RPC errors (errors happening when handling the RPC)")
				.build(),
			rpc_duration: meter
				.f64_histogram("rpc.duration")
				.with_description("Duration of RPCs")
				.build(),
		}
	}
}
