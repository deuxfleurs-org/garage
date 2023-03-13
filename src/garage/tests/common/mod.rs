use aws_sdk_s3::{Client, Region};
use ext::*;

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
		let client = client::build_client(garage, &key);
		let custom_request = CustomRequester::new_s3(garage, &key);
		let k2v_request = CustomRequester::new_k2v(garage, &key);

		Context {
			garage,
			client,
			key,
			custom_request,
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
}

pub fn context() -> Context {
	Context::new()
}
