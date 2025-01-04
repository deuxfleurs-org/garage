use garage_table::crdt::*;
use garage_table::*;
use garage_util::data::*;
use garage_util::time::*;

use crate::permission::BucketKeyPerm;

mod v08 {
	use crate::permission::BucketKeyPerm;
	use garage_util::crdt;
	use garage_util::data::Uuid;
	use serde::{Deserialize, Serialize};

	/// A bucket is a collection of objects
	///
	/// Its parameters are not directly accessible as:
	///  - It must be possible to merge paramaters, hence the use of a LWW CRDT.
	///  - A bucket has 2 states, Present or Deleted and parameters make sense only if present.
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct Bucket {
		/// ID of the bucket
		pub id: Uuid,
		/// State, and configuration if not deleted, of the bucket
		pub state: crdt::Deletable<BucketParams>,
	}

	/// Configuration for a bucket
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct BucketParams {
		/// Bucket's creation date
		pub creation_date: u64,
		/// Map of key with access to the bucket, and what kind of access they give
		pub authorized_keys: crdt::Map<String, BucketKeyPerm>,

		/// Map of aliases that are or have been given to this bucket
		/// in the global namespace
		/// (not authoritative: this is just used as an indication to
		/// map back to aliases when doing ListBuckets)
		pub aliases: crdt::LwwMap<String, bool>,
		/// Map of aliases that are or have been given to this bucket
		/// in namespaces local to keys
		/// key = (access key id, alias name)
		pub local_aliases: crdt::LwwMap<(String, String), bool>,

		/// Whether this bucket is allowed for website access
		/// (under all of its global alias names),
		/// and if so, the website configuration XML document
		pub website_config: crdt::Lww<Option<WebsiteConfig>>,
		/// CORS rules
		pub cors_config: crdt::Lww<Option<Vec<CorsRule>>>,
		/// Lifecycle configuration
		#[serde(default)]
		pub lifecycle_config: crdt::Lww<Option<Vec<LifecycleRule>>>,
		/// Bucket quotas
		#[serde(default)]
		pub quotas: crdt::Lww<BucketQuotas>,
	}

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct WebsiteConfig {
		pub index_document: String,
		pub error_document: Option<String>,
	}

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct CorsRule {
		pub id: Option<String>,
		pub max_age_seconds: Option<u64>,
		pub allow_origins: Vec<String>,
		pub allow_methods: Vec<String>,
		pub allow_headers: Vec<String>,
		pub expose_headers: Vec<String>,
	}

	/// Lifecycle configuration rule
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct LifecycleRule {
		/// The ID of the rule
		pub id: Option<String>,
		/// Whether the rule is active
		pub enabled: bool,
		/// The filter to check whether rule applies to a given object
		pub filter: LifecycleFilter,
		/// Number of days after which incomplete multipart uploads are aborted
		pub abort_incomplete_mpu_days: Option<usize>,
		/// Expiration policy for stored objects
		pub expiration: Option<LifecycleExpiration>,
	}

	/// A lifecycle filter is a set of conditions that must all be true.
	/// For each condition, if it is None, it is not verified (always true),
	/// and if it is Some(x), then it is verified for value x
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, Default)]
	pub struct LifecycleFilter {
		/// If Some(x), object key has to start with prefix x
		pub prefix: Option<String>,
		/// If Some(x), object size has to be more than x
		pub size_gt: Option<u64>,
		/// If Some(x), object size has to be less than x
		pub size_lt: Option<u64>,
	}

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub enum LifecycleExpiration {
		/// Objects expire x days after they were created
		AfterDays(usize),
		/// Objects expire at date x (must be in yyyy-mm-dd format)
		AtDate(String),
	}

	#[derive(Default, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
	pub struct BucketQuotas {
		/// Maximum size in bytes (bucket size = sum of sizes of objects in the bucket)
		pub max_size: Option<u64>,
		/// Maximum number of non-deleted objects in the bucket
		pub max_objects: Option<u64>,
	}

	impl garage_util::migrate::InitialFormat for Bucket {}
}

mod v2 {
	use crate::permission::BucketKeyPerm;
	use garage_util::crdt;
	use garage_util::data::Uuid;
	use serde::{Deserialize, Serialize};

	use super::v08;

	pub use v08::{BucketQuotas, CorsRule, LifecycleExpiration, LifecycleFilter, LifecycleRule};

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct Bucket {
		/// ID of the bucket
		pub id: Uuid,
		/// State, and configuration if not deleted, of the bucket
		pub state: crdt::Deletable<BucketParams>,
	}

	/// Configuration for a bucket
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct BucketParams {
		/// Bucket's creation date
		pub creation_date: u64,
		/// Map of key with access to the bucket, and what kind of access they give
		pub authorized_keys: crdt::Map<String, BucketKeyPerm>,

		/// Map of aliases that are or have been given to this bucket
		/// in the global namespace
		/// (not authoritative: this is just used as an indication to
		/// map back to aliases when doing ListBuckets)
		pub aliases: crdt::LwwMap<String, bool>,
		/// Map of aliases that are or have been given to this bucket
		/// in namespaces local to keys
		/// key = (access key id, alias name)
		pub local_aliases: crdt::LwwMap<(String, String), bool>,

		/// Whether this bucket is allowed for website access
		/// (under all of its global alias names),
		/// and if so, the website configuration XML document
		pub website_config: crdt::Lww<Option<WebsiteConfig>>,
		/// CORS rules
		pub cors_config: crdt::Lww<Option<Vec<CorsRule>>>,
		/// Lifecycle configuration
		pub lifecycle_config: crdt::Lww<Option<Vec<LifecycleRule>>>,
		/// Bucket quotas
		pub quotas: crdt::Lww<BucketQuotas>,
	}

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct WebsiteConfig {
		pub index_document: String,
		pub error_document: Option<String>,
		pub routing_rules: Vec<RoutingRule>,
	}

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct RoutingRule {
		pub condition: Option<Condition>,
		pub redirect: Redirect,
	}

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct Condition {
		pub http_error_code: Option<u16>,
		pub prefix: Option<String>,
	}

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct Redirect {
		pub hostname: Option<String>,
		pub http_redirect_code: u16,
		pub protocol: Option<String>,
		pub replace_key_prefix: Option<String>,
		pub replace_key: Option<String>,
	}

	impl garage_util::migrate::Migrate for Bucket {
		const VERSION_MARKER: &'static [u8] = b"G2bkt";

		type Previous = v08::Bucket;

		fn migrate(old: v08::Bucket) -> Bucket {
			Bucket {
				id: old.id,
				state: old.state.map(|x| BucketParams {
					creation_date: x.creation_date,
					authorized_keys: x.authorized_keys,
					aliases: x.aliases,
					local_aliases: x.local_aliases,
					website_config: x.website_config.map(|wc_opt| {
						wc_opt.map(|wc| WebsiteConfig {
							index_document: wc.index_document,
							error_document: wc.error_document,
							routing_rules: vec![],
						})
					}),
					cors_config: x.cors_config,
					lifecycle_config: x.lifecycle_config,
					quotas: x.quotas,
				}),
			}
		}
	}
}

pub use v2::*;

impl AutoCrdt for BucketQuotas {
	const WARN_IF_DIFFERENT: bool = true;
}

impl BucketParams {
	/// Create an empty BucketParams with no authorized keys and no website accesss
	fn new() -> Self {
		BucketParams {
			creation_date: now_msec(),
			authorized_keys: crdt::Map::new(),
			aliases: crdt::LwwMap::new(),
			local_aliases: crdt::LwwMap::new(),
			website_config: crdt::Lww::new(None),
			cors_config: crdt::Lww::new(None),
			lifecycle_config: crdt::Lww::new(None),
			quotas: crdt::Lww::new(BucketQuotas::default()),
		}
	}
}

impl Crdt for BucketParams {
	fn merge(&mut self, o: &Self) {
		self.creation_date = std::cmp::min(self.creation_date, o.creation_date);
		self.authorized_keys.merge(&o.authorized_keys);

		self.aliases.merge(&o.aliases);
		self.local_aliases.merge(&o.local_aliases);

		self.website_config.merge(&o.website_config);
		self.cors_config.merge(&o.cors_config);
		self.lifecycle_config.merge(&o.lifecycle_config);
		self.quotas.merge(&o.quotas);
	}
}

pub fn parse_lifecycle_date(date: &str) -> Result<chrono::NaiveDate, &'static str> {
	use chrono::prelude::*;

	if let Ok(datetime) = NaiveDateTime::parse_from_str(date, "%Y-%m-%dT%H:%M:%SZ") {
		if datetime.time() == NaiveTime::MIN {
			Ok(datetime.date())
		} else {
			Err("date must be at midnight")
		}
	} else {
		NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| "date has invalid format")
	}
}

impl Default for Bucket {
	fn default() -> Self {
		Self::new()
	}
}

impl Default for BucketParams {
	fn default() -> Self {
		Self::new()
	}
}

impl Bucket {
	/// Initializes a new instance of the Bucket struct
	pub fn new() -> Self {
		Bucket {
			id: gen_uuid(),
			state: crdt::Deletable::present(BucketParams::new()),
		}
	}

	pub fn present(id: Uuid, params: BucketParams) -> Self {
		Bucket {
			id,
			state: crdt::Deletable::present(params),
		}
	}

	/// Returns true if this represents a deleted bucket
	pub fn is_deleted(&self) -> bool {
		self.state.is_deleted()
	}

	/// Returns an option representing the parameters (None if in deleted state)
	pub fn params(&self) -> Option<&BucketParams> {
		self.state.as_option()
	}

	/// Mutable version of `.params()`
	pub fn params_mut(&mut self) -> Option<&mut BucketParams> {
		self.state.as_option_mut()
	}

	/// Return the list of authorized keys, when each was updated, and the permission associated to
	/// the key
	pub fn authorized_keys(&self) -> &[(String, BucketKeyPerm)] {
		self.params()
			.map(|s| s.authorized_keys.items())
			.unwrap_or(&[])
	}

	pub fn aliases(&self) -> &[(String, u64, bool)] {
		self.params().map(|s| s.aliases.items()).unwrap_or(&[])
	}

	pub fn local_aliases(&self) -> &[((String, String), u64, bool)] {
		self.params()
			.map(|s| s.local_aliases.items())
			.unwrap_or(&[])
	}
}

impl Entry<EmptyKey, Uuid> for Bucket {
	fn partition_key(&self) -> &EmptyKey {
		&EmptyKey
	}
	fn sort_key(&self) -> &Uuid {
		&self.id
	}
}

impl Crdt for Bucket {
	fn merge(&mut self, other: &Self) {
		self.state.merge(&other.state);
	}
}

pub struct BucketTable;

impl TableSchema for BucketTable {
	const TABLE_NAME: &'static str = "bucket_v2";

	type P = EmptyKey;
	type S = Uuid;
	type E = Bucket;
	type Filter = DeletedFilter;

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		filter.apply(entry.is_deleted())
	}
}
