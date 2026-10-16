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

pub use garage_api_admin::api::ApiBucketKeyPerm as BucketKeyPerm;

use custom_requester::CustomRequester;

const REGION: Region = Region::from_static("garage-integ-test");

#[derive(Clone)]
pub struct Context {
	pub garage: &'static garage::Instance,
	pub key: garage::Key,
	pub client: Client,
	pub admin_client: reqwest::Client,
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
		if rustls::crypto::CryptoProvider::get_default().is_none() {
			_ = rustls::crypto::ring::default_provider().install_default();
		}

		let garage = garage::instance();
		let key = garage.key(None);
		let client = client::build_client(&key);
		let admin_client = reqwest::Client::new();
		let custom_request = CustomRequester::new_s3(garage, &key);
		#[cfg(feature = "k2v")]
		let k2v_request = CustomRequester::new_k2v(garage, &key);

		Context {
			garage,
			client,
			admin_client,
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
		let bucket_name = format!("{}-{:016x}", name.to_owned(), rand::random::<u64>());

		self.create_bucket_no_suffix(bucket_name)
	}

	pub fn create_bucket_no_suffix(&self, bucket_name: String) -> String {
		self.garage
			.command()
			.args(["bucket", "create", &bucket_name])
			.quiet()
			.expect_success_status("Could not create bucket");
		self.garage
			.command()
			.args(["bucket", "allow"])
			.args(["--read", "--write"])
			.arg(&bucket_name)
			.args(["--key", &self.key.id])
			.quiet()
			.expect_success_status("Could not allow key for bucket");

		bucket_name
	}

	pub fn bucket_allow_owner(&self, bucket_name: &str) {
		self.garage
			.command()
			.args(["bucket", "allow", "--owner"])
			.arg(bucket_name)
			.args(["--key", &self.key.id])
			.quiet()
			.expect_success_status("Could not allow key for bucket");
	}

	/// Create an unique bucket with a random suffix and given permissions
	///
	/// Return the created bucket full name.
	pub fn create_bucket_with_permissions(&self, name: &str, perm: BucketKeyPerm) -> String {
		let bucket_name = format!("{}-{:016x}", name.to_owned(), rand::random::<u64>());

		self.garage
			.command()
			.args(["bucket", "create", &bucket_name])
			.quiet()
			.expect_success_status("Could not create bucket");
		let mut perms = vec![];
		if perm.read {
			perms.push("--read");
		}
		if perm.write {
			perms.push("--write");
		}
		if perm.owner {
			perms.push("--owner");
		}
		self.garage
			.command()
			.args(["bucket", "allow"])
			.args(perms)
			.arg(&bucket_name)
			.args(["--key", &self.key.id])
			.quiet()
			.expect_success_status("Could not allow key for bucket");

		bucket_name
	}

	/// Build a `K2vClient` for a given bucket
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

	/// client to send GET request to admin endpoint
	pub fn admin_get(&self, endpoint: &str) -> reqwest::RequestBuilder {
		self.admin_client
			.get(format!("{}/v2/{}", self.garage.admin_uri(), endpoint))
			.bearer_auth(&self.garage.admin_token)
	}

	/// client to send POST request to admin endpoint
	pub fn admin_post(&self, endpoint: &str) -> reqwest::RequestBuilder {
		self.admin_client
			.post(format!("{}/v2/{}", self.garage.admin_uri(), endpoint))
			.bearer_auth(&self.garage.admin_token)
	}
}

pub fn context() -> Context {
	Context::new()
}
