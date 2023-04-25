use aws_sdk_s3::{Client, Config, Credentials, Endpoint};

use super::garage::{Instance, Key};

pub fn build_client(instance: &Instance, key: &Key) -> Client {
	let credentials = Credentials::new(&key.id, &key.secret, None, None, "garage-integ-test");
	let endpoint = Endpoint::immutable(instance.s3_uri());

	let config = Config::builder()
		.region(super::REGION)
		.credentials_provider(credentials)
		.endpoint_resolver(endpoint)
		.build();

	Client::from_conf(config)
}
