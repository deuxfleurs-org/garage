use std::time::Instant;

use futures::{future::BoxFuture, Future, FutureExt};
use rand::Rng;

use opentelemetry::{metrics::*, trace::TraceId, KeyValue};

pub trait RecordDuration<'a>: 'a {
	type Output;

	fn record_duration(
		self,
		r: &'a ValueRecorder<f64>,
		attributes: &'a [KeyValue],
	) -> BoxFuture<'a, Self::Output>;
	fn bound_record_duration(self, r: &'a BoundValueRecorder<f64>) -> BoxFuture<'a, Self::Output>;
}

impl<'a, T, O> RecordDuration<'a> for T
where
	T: Future<Output = O> + Send + 'a,
{
	type Output = O;

	fn record_duration(
		self,
		r: &'a ValueRecorder<f64>,
		attributes: &'a [KeyValue],
	) -> BoxFuture<'a, Self::Output> {
		async move {
			let request_start = Instant::now();
			let res = self.await;
			r.record(
				Instant::now()
					.saturating_duration_since(request_start)
					.as_secs_f64(),
				attributes,
			);
			res
		}
		.boxed()
	}

	fn bound_record_duration(self, r: &'a BoundValueRecorder<f64>) -> BoxFuture<'a, Self::Output> {
		async move {
			let request_start = Instant::now();
			let res = self.await;
			r.record(
				Instant::now()
					.saturating_duration_since(request_start)
					.as_secs_f64(),
			);
			res
		}
		.boxed()
	}
}

// ----

pub fn gen_trace_id() -> TraceId {
	rand::thread_rng().gen::<[u8; 16]>().into()
}
