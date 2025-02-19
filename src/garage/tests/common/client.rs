use aws_sdk_s3::config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config};

use super::garage::Key;
use crate::common::garage::DEFAULT_PORT;

pub fn build_client(key: &Key) -> Client {
	let credentials = Credentials::new(&key.id, &key.secret, None, None, "garage-integ-test");

	let config = Config::builder()
		.endpoint_url(format!("http://127.0.0.1:{}", DEFAULT_PORT))
		.region(super::REGION)
		.credentials_provider(credentials)
		.behavior_version(BehaviorVersion::v2024_03_28())
		.build();

	Client::from_conf(config)
}
