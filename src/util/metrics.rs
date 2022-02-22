use std::time::SystemTime;

use futures::{future::BoxFuture, Future, FutureExt};

use opentelemetry::{metrics::*, KeyValue};

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
			let request_start = SystemTime::now();
			let res = self.await;
			r.record(
				request_start.elapsed().map_or(0.0, |d| d.as_secs_f64()),
				attributes,
			);
			res
		}
		.boxed()
	}

	fn bound_record_duration(self, r: &'a BoundValueRecorder<f64>) -> BoxFuture<'a, Self::Output> {
		async move {
			let request_start = SystemTime::now();
			let res = self.await;
			r.record(request_start.elapsed().map_or(0.0, |d| d.as_secs_f64()));
			res
		}
		.boxed()
	}
}
