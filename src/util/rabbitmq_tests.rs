#[cfg(test)]
mod tests {
	use super::*;

	use crate::config::RabbitConfig;

	#[test]
	fn test_rabbit_config_filtering() {
		let cfg = RabbitConfig {
			uri: "amqp://localhost:5672/%2f".into(),
			exchange: "test".into(),
			routing_key_object_created: "s3.object.created".into(),
			publish_object_created: true,
			allowed_extensions: Some(vec![".m3u8".into(), ".mp4".into()]),
			ignored_extensions: Some(vec![".ts".into()]),
			filter_prefixes: Some(vec!["uploads/".into()]),
		};

		assert!(cfg.should_publish_object_created("uploads/video/master.m3u8"));
		assert!(cfg.should_publish_object_created("uploads/video/movie.mp4"));

		// Wrong prefix
		assert!(!cfg.should_publish_object_created("other/video/master.m3u8"));
		// Ignored extension
		assert!(!cfg.should_publish_object_created("uploads/video/segment0001.ts"));
		// Not in allowed extensions
		assert!(!cfg.should_publish_object_created("uploads/video/cover.jpg"));
	}
}

