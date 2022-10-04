use std::convert::TryInto;

use garage_db as db;

use garage_util::data::*;
use garage_util::error::*;
use garage_util::time::*;

use crate::manager::BLOCK_GC_DELAY;

pub struct BlockRc {
	pub(crate) rc: db::Tree,
}

impl BlockRc {
	pub(crate) fn new(rc: db::Tree) -> Self {
		Self { rc }
	}

	/// Increment the reference counter associated to a hash.
	/// Returns true if the RC goes from zero to nonzero.
	pub(crate) fn block_incref(
		&self,
		tx: &mut db::Transaction,
		hash: &Hash,
	) -> db::TxOpResult<bool> {
		let old_rc = RcEntry::parse_opt(tx.get(&self.rc, &hash)?);
		match old_rc.increment().serialize() {
			Some(x) => tx.insert(&self.rc, &hash, x)?,
			None => unreachable!(),
		};
		Ok(old_rc.is_zero())
	}

	/// Decrement the reference counter associated to a hash.
	/// Returns true if the RC is now zero.
	pub(crate) fn block_decref(
		&self,
		tx: &mut db::Transaction,
		hash: &Hash,
	) -> db::TxOpResult<bool> {
		let new_rc = RcEntry::parse_opt(tx.get(&self.rc, &hash)?).decrement();
		match new_rc.serialize() {
			Some(x) => tx.insert(&self.rc, &hash, x)?,
			None => tx.remove(&self.rc, &hash)?,
		};
		Ok(matches!(new_rc, RcEntry::Deletable { .. }))
	}

	/// Read a block's reference count
	pub(crate) fn get_block_rc(&self, hash: &Hash) -> Result<RcEntry, Error> {
		Ok(RcEntry::parse_opt(self.rc.get(hash.as_ref())?))
	}

	/// Delete an entry in the RC table if it is deletable and the
	/// deletion time has passed
	pub(crate) fn clear_deleted_block_rc(&self, hash: &Hash) -> Result<(), Error> {
		let now = now_msec();
		self.rc.db().transaction(|mut tx| {
			let rcval = RcEntry::parse_opt(tx.get(&self.rc, &hash)?);
			match rcval {
				RcEntry::Deletable { at_time } if now > at_time => {
					tx.remove(&self.rc, &hash)?;
				}
				_ => (),
			};
			tx.commit(())
		})?;
		Ok(())
	}
}

/// Describes the state of the reference counter for a block
#[derive(Clone, Copy, Debug)]
pub(crate) enum RcEntry {
	/// Present: the block has `count` references, with `count` > 0.
	///
	/// This is stored as u64::to_be_bytes(count)
	Present { count: u64 },

	/// Deletable: the block has zero references, and can be deleted
	/// once time (returned by now_msec) is larger than at_time
	/// (in millis since Unix epoch)
	///
	/// This is stored as [0u8; 8] followed by u64::to_be_bytes(at_time),
	/// (this allows for the data format to be backwards compatible with
	/// previous Garage versions that didn't have this intermediate state)
	Deletable { at_time: u64 },

	/// Absent: the block has zero references, and can be deleted
	/// immediately
	Absent,
}

impl RcEntry {
	fn parse(bytes: &[u8]) -> Self {
		if bytes.len() == 8 {
			RcEntry::Present {
				count: u64::from_be_bytes(bytes.try_into().unwrap()),
			}
		} else if bytes.len() == 16 {
			RcEntry::Deletable {
				at_time: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
			}
		} else {
			panic!("Invalid RC entry: {:?}, database is corrupted. This is an error Garage is currently unable to recover from. Sorry, and also please report a bug.",
				bytes
			)
		}
	}

	fn parse_opt<V: AsRef<[u8]>>(bytes: Option<V>) -> Self {
		bytes
			.map(|b| Self::parse(b.as_ref()))
			.unwrap_or(Self::Absent)
	}

	fn serialize(self) -> Option<Vec<u8>> {
		match self {
			RcEntry::Present { count } => Some(u64::to_be_bytes(count).to_vec()),
			RcEntry::Deletable { at_time } => {
				Some([u64::to_be_bytes(0), u64::to_be_bytes(at_time)].concat())
			}
			RcEntry::Absent => None,
		}
	}

	fn increment(self) -> Self {
		let old_count = match self {
			RcEntry::Present { count } => count,
			_ => 0,
		};
		RcEntry::Present {
			count: old_count + 1,
		}
	}

	fn decrement(self) -> Self {
		match self {
			RcEntry::Present { count } => {
				if count > 1 {
					RcEntry::Present { count: count - 1 }
				} else {
					RcEntry::Deletable {
						at_time: now_msec() + BLOCK_GC_DELAY.as_millis() as u64,
					}
				}
			}
			del => del,
		}
	}

	pub(crate) fn is_zero(&self) -> bool {
		matches!(self, RcEntry::Deletable { .. } | RcEntry::Absent)
	}

	pub(crate) fn is_nonzero(&self) -> bool {
		!self.is_zero()
	}

	pub(crate) fn is_deletable(&self) -> bool {
		match self {
			RcEntry::Present { .. } => false,
			RcEntry::Deletable { at_time } => now_msec() > *at_time,
			RcEntry::Absent => true,
		}
	}

	pub(crate) fn is_needed(&self) -> bool {
		!self.is_deletable()
	}
}
