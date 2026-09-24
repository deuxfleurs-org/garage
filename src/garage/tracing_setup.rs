pub use telemetry::{init_tracing, shutdown_tracing};

#[cfg(not(feature = "telemetry-otlp"))]
mod telemetry {
	use garage_util::data::Uuid;
	use garage_util::error::Error;

	#[expect(clippy::unnecessary_wraps)]
	pub fn init_tracing(_: &str, _: Uuid) -> Result<(), Error> {
		error!("Garage was built without OTLP exporter, admin.trace_sink is ignored.");
		Ok(())
	}

	pub fn shutdown_tracing() {}
}

#[cfg(feature = "telemetry-otlp")]
mod telemetry {
	use std::sync::OnceLock;
	use std::time::Duration;

	use opentelemetry::KeyValue;
	use opentelemetry_otlp::WithExportConfig;
	use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler, SdkTracerProvider};
	use opentelemetry_sdk::Resource;

	use garage_util::data::*;
	use garage_util::error::*;

	static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

	pub fn init_tracing(export_to: &str, node_id: Uuid) -> Result<(), Error> {
		let node_id = hex::encode(&node_id.as_slice()[..8]);

		let exporter = opentelemetry_otlp::SpanExporter::builder()
			.with_tonic()
			.with_endpoint(export_to)
			.with_timeout(Duration::from_secs(3))
			.build()
			.err_context("Unable to initialize OTLP exporter")?;

		let resource = Resource::builder()
			.with_attributes([
				KeyValue::new("service.name", "garage"),
				KeyValue::new("service.instance.id", node_id),
			])
			.build();

		let provider = SdkTracerProvider::builder()
			.with_id_generator(RandomIdGenerator::default())
			.with_sampler(Sampler::AlwaysOn)
			.with_resource(resource)
			.with_batch_exporter(exporter)
			.build();

		opentelemetry::global::set_tracer_provider(provider.clone());

		let _ = TRACER_PROVIDER.set(provider);

		Ok(())
	}

	pub fn shutdown_tracing() {
		if let Some(provider) = TRACER_PROVIDER.get() {
			let _ = provider.shutdown();
		}
	}
}
