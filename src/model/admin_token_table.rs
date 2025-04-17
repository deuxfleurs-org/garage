use base64::prelude::*;

use garage_util::crdt::{self, Crdt};
use garage_util::time::now_msec;

use garage_table::{EmptyKey, Entry, TableSchema};

pub use crate::key_table::KeyFilter;

mod v2 {
	use garage_util::crdt;
	use serde::{Deserialize, Serialize};

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct AdminApiToken {
		/// An admin API token is a bearer token of the following form:
		/// `<prefix>.<suffix>`
		/// Only the prefix is saved here, it is used as an identifier.
		/// The entire API token is hashed and saved in `token_hash` in `state`.
		pub prefix: String,

		/// If the token is not deleted, its parameters
		pub state: crdt::Deletable<AdminApiTokenParams>,
	}

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct AdminApiTokenParams {
		/// Creation date
		pub created: u64,

		/// The entire API token hashed as a password
		pub token_hash: String,

		/// User-defined name
		pub name: crdt::Lww<String>,

		/// The optional time of expiration of the token
		pub expiration: crdt::Lww<Option<u64>>,

		/// The scope of the token, i.e. list of authorized admin API calls
		pub scope: crdt::Lww<AdminApiTokenScope>,
	}

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct AdminApiTokenScope(pub Vec<String>);

	impl garage_util::migrate::InitialFormat for AdminApiToken {
		const VERSION_MARKER: &'static [u8] = b"G2admtok";
	}
}

pub use v2::*;

impl Crdt for AdminApiTokenParams {
	fn merge(&mut self, o: &Self) {
		self.name.merge(&o.name);
		self.expiration.merge(&o.expiration);
		self.scope.merge(&o.scope);
	}
}

impl Crdt for AdminApiToken {
	fn merge(&mut self, other: &Self) {
		self.state.merge(&other.state);
	}
}

impl Crdt for AdminApiTokenScope {
	fn merge(&mut self, other: &Self) {
		self.0.retain(|x| other.0.contains(x));
	}
}

impl AdminApiToken {
	/// Create a new admin API token.
	/// Returns the AdminApiToken object, which contains the hashed bearer token,
	/// as well as the plaintext bearer token.
	pub fn new(name: &str) -> (Self, String) {
		use argon2::{
			password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
			Argon2,
		};

		let prefix = hex::encode(&rand::random::<[u8; 12]>()[..]);
		let secret = BASE64_URL_SAFE_NO_PAD.encode(&rand::random::<[u8; 32]>()[..]);
		let token = format!("{}.{}", prefix, secret);

		let salt = SaltString::generate(&mut OsRng);
		let argon2 = Argon2::default();
		let hashed_token = argon2
			.hash_password(token.as_bytes(), &salt)
			.expect("could not hash admin API token")
			.to_string();

		let ret = AdminApiToken {
			prefix,
			state: crdt::Deletable::present(AdminApiTokenParams {
				created: now_msec(),
				token_hash: hashed_token,
				name: crdt::Lww::new(name.to_string()),
				expiration: crdt::Lww::new(None),
				scope: crdt::Lww::new(AdminApiTokenScope(vec!["*".to_string()])),
			}),
		};

		(ret, token)
	}

	pub fn delete(prefix: String) -> Self {
		Self {
			prefix,
			state: crdt::Deletable::Deleted,
		}
	}

	/// Returns true if this represents a deleted admin token
	pub fn is_deleted(&self) -> bool {
		self.state.is_deleted()
	}

	/// Returns an option representing the params (None if in deleted state)
	pub fn params(&self) -> Option<&AdminApiTokenParams> {
		self.state.as_option()
	}

	/// Mutable version of `.state()`
	pub fn params_mut(&mut self) -> Option<&mut AdminApiTokenParams> {
		self.state.as_option_mut()
	}

	/// Scope, if not deleted, or empty slice
	pub fn scope(&self) -> &[String] {
		self.state
			.as_option()
			.map(|x| &x.scope.get().0[..])
			.unwrap_or_default()
	}
}

impl AdminApiTokenParams {
	pub fn is_expired(&self, ts_now: u64) -> bool {
		match *self.expiration.get() {
			None => false,
			Some(exp) => ts_now >= exp,
		}
	}

	pub fn has_scope(&self, endpoint: &str) -> bool {
		self.scope.get().0.iter().any(|x| x == "*" || x == endpoint)
	}
}

impl Entry<EmptyKey, String> for AdminApiToken {
	fn partition_key(&self) -> &EmptyKey {
		&EmptyKey
	}
	fn sort_key(&self) -> &String {
		&self.prefix
	}
	fn is_tombstone(&self) -> bool {
		self.is_deleted()
	}
}

pub struct AdminApiTokenTable;

impl TableSchema for AdminApiTokenTable {
	const TABLE_NAME: &'static str = "admin_token";

	type P = EmptyKey;
	type S = String;
	type E = AdminApiToken;
	type Filter = KeyFilter;

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		match filter {
			KeyFilter::Deleted(df) => df.apply(entry.state.is_deleted()),
			KeyFilter::MatchesAndNotDeleted(pat) => {
				let pat = pat.to_lowercase();
				entry
					.params()
					.map(|p| {
						entry.prefix.to_lowercase().starts_with(&pat)
							|| p.name.get().to_lowercase() == pat
					})
					.unwrap_or(false)
			}
		}
	}
}
