use aws_sdk_s3::{Client, Config, Credentials, Endpoint};

use super::garage::Instance;

pub fn build_client(instance: &Instance) -> Client {
	let credentials = Credentials::new(
		&instance.key.id,
		&instance.key.secret,
		None,
		None,
		"garage-integ-test",
	);
	let endpoint = Endpoint::immutable(instance.s3_uri());

	let config = Config::builder()
		.region(super::REGION)
		.credentials_provider(credentials)
		.endpoint_resolver(endpoint)
		.build();

	Client::from_conf(config)
}
