use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client;
use ext::*;
#[cfg(feature = "k2v")]
use k2v_client::K2vClient;

#[macro_use]
pub mod macros;

pub mod client;
pub mod custom_requester;
pub mod ext;
pub mod garage;

use custom_requester::CustomRequester;

const REGION: Region = Region::from_static("garage-integ-test");

#[derive(Clone)]
pub struct Context {
	pub garage: &'static garage::Instance,
	pub key: garage::Key,
	pub client: Client,
	pub custom_request: CustomRequester,
	#[cfg(feature = "k2v")]
	pub k2v: K2VContext,
}

#[derive(Clone)]
pub struct K2VContext {
	pub request: CustomRequester,
}

impl Context {
	fn new() -> Self {
		let garage = garage::instance();
		let key = garage.key(None);
		let client = client::build_client(&key);
		let custom_request = CustomRequester::new_s3(garage, &key);
		#[cfg(feature = "k2v")]
		let k2v_request = CustomRequester::new_k2v(garage, &key);

		Context {
			garage,
			client,
			key,
			custom_request,
			#[cfg(feature = "k2v")]
			k2v: K2VContext {
				request: k2v_request,
			},
		}
	}

	/// Create an unique bucket with a random suffix.
	///
	/// Return the created bucket full name.
	pub fn create_bucket(&self, name: &str) -> String {
		let bucket_name = name.to_owned();

		self.garage
			.command()
			.args(["bucket", "create", &bucket_name])
			.quiet()
			.expect_success_status("Could not create bucket");
		self.garage
			.command()
			.args(["bucket", "allow"])
			.args(["--owner", "--read", "--write"])
			.arg(&bucket_name)
			.args(["--key", &self.key.id])
			.quiet()
			.expect_success_status("Could not allow key for bucket");

		bucket_name
	}

	/// Build a K2vClient for a given bucket
	#[cfg(feature = "k2v")]
	pub fn k2v_client(&self, bucket: &str) -> K2vClient {
		let config = k2v_client::K2vClientConfig {
			region: REGION.to_string(),
			endpoint: self.garage.k2v_uri().to_string(),
			aws_access_key_id: self.key.id.clone(),
			aws_secret_access_key: self.key.secret.clone(),
			bucket: bucket.to_string(),
			user_agent: None,
		};
		K2vClient::new(config).expect("Could not create K2V client")
	}
}

pub fn context() -> Context {
	Context::new()
}
