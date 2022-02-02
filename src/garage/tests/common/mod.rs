use aws_sdk_s3::{Client, Region};
use ext::*;

#[macro_use]
pub mod macros;

pub mod client;
pub mod ext;
pub mod garage;

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

	pub fn create_bucket(&self, name: &str) {
		self.garage
			.command()
			.args(["bucket", "create", name])
			.quiet()
			.expect_success_status("Could not create bucket");
		self.garage
			.command()
			.args(["bucket", "allow"])
			.args(["--owner", "--read", "--write"])
			.arg(name)
			.args(["--key", &self.garage.key.name])
			.quiet()
			.expect_success_status("Could not allow key for bucket");
	}
}

pub fn context() -> Context {
	Context::new()
}
