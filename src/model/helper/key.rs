use garage_table::util::*;
use garage_util::error::OkOrMessage;

use crate::garage::Garage;
use crate::helper::error::*;
use crate::key_table::{Key, KeyFilter};

pub struct KeyHelper<'a>(pub(crate) &'a Garage);

#[allow(clippy::ptr_arg)]
impl<'a> KeyHelper<'a> {
	/// Returns a Key if it is present in key table,
	/// even if it is in deleted state. Querying a non-existing
	/// key ID returns an internal error.
	pub async fn get_internal_key(&self, key_id: &String) -> Result<Key, Error> {
		Ok(self
			.0
			.key_table
			.get(&EmptyKey, key_id)
			.await?
			.ok_or_message(format!("Key {} does not exist", key_id))?)
	}

	/// Returns a Key if it is present in key table,
	/// only if it is in non-deleted state.
	/// Querying a non-existing key ID or a deleted key
	/// returns a bad request error.
	pub async fn get_existing_key(&self, key_id: &String) -> Result<Key, Error> {
		self.0
			.key_table
			.get(&EmptyKey, key_id)
			.await?
			.filter(|b| !b.state.is_deleted())
			.ok_or_else(|| Error::NoSuchAccessKey(key_id.to_string()))
	}

	/// Returns a Key if it is present in key table,
	/// looking it up by key ID or by a match on its name,
	/// only if it is in non-deleted state.
	/// Querying a non-existing key ID or a deleted key
	/// returns a bad request error.
	pub async fn get_existing_matching_key(&self, pattern: &str) -> Result<Key, Error> {
		let candidates = self
			.0
			.key_table
			.get_range(
				&EmptyKey,
				None,
				Some(KeyFilter::MatchesAndNotDeleted(pattern.to_string())),
				10,
				EnumerationOrder::Forward,
			)
			.await?
			.into_iter()
			.collect::<Vec<_>>();
		if candidates.len() != 1 {
			Err(Error::BadRequest(format!(
				"{} matching keys",
				candidates.len()
			)))
		} else {
			Ok(candidates.into_iter().next().unwrap())
		}
	}
}
