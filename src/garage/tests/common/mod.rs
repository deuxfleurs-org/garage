use aws_sdk_s3::{Client, Region};
use ext::*;

#[macro_use]
pub mod macros;

pub mod client;
pub mod ext;
pub mod garage;
pub mod util;

const REGION: Region = Region::from_static("garage-integ-test");

pub struct Context {
	pub garage: &'static garage::Instance,
	pub client: Client,
}

impl Context {
	fn new() -> Self {
		let garage = garage::instance();
		let client = client::build_client(garage);

		Context { garage, client }
	}

	/// Create an unique bucket with a random suffix.
	///
	/// Return the created bucket full name.
	pub fn create_bucket(&self, name: &str) -> String {
		let bucket_name = format!("{}-{}", name, util::random_id(6));

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
