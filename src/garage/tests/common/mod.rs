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

pub struct Context {
	pub garage: &'static garage::Instance,
	pub client: Client,
	pub custom_request: CustomRequester,
}

impl Context {
	fn new() -> Self {
		let garage = garage::instance();
		let client = client::build_client(garage);
		let custom_request = CustomRequester::new(garage);

		Context {
			garage,
			client,
			custom_request,
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
			.args(["--key", &self.garage.key.name])
			.quiet()
			.expect_success_status("Could not allow key for bucket");

		bucket_name
	}
}

pub fn context() -> Context {
	Context::new()
}
