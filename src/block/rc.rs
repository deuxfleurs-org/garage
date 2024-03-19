use std::convert::TryInto;

use arc_swap::ArcSwapOption;

use garage_db as db;

use garage_util::data::*;
use garage_util::error::*;
use garage_util::time::*;

use crate::manager::BLOCK_GC_DELAY;

pub type CalculateRefcount =
	Box<dyn Fn(&db::Transaction, &Hash) -> db::TxResult<usize, Error> + Send + Sync>;

pub struct BlockRc {
	pub rc_table: db::Tree,
	pub(crate) recalc_rc: ArcSwapOption<Vec<CalculateRefcount>>,
}

impl BlockRc {
	pub(crate) fn new(rc: db::Tree) -> Self {
		Self {
			rc_table: rc,
			recalc_rc: ArcSwapOption::new(None),
		}
	}

	/// Increment the reference counter associated to a hash.
	/// Returns true if the RC goes from zero to nonzero.
	pub(crate) fn block_incref(
		&self,
		tx: &mut db::Transaction,
		hash: &Hash,
	) -> db::TxOpResult<bool> {
		let old_rc = RcEntry::parse_opt(tx.get(&self.rc_table, hash)?);
		match old_rc.increment().serialize() {
			Some(x) => tx.insert(&self.rc_table, hash, x)?,
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
		let new_rc = RcEntry::parse_opt(tx.get(&self.rc_table, hash)?).decrement();
		match new_rc.serialize() {
			Some(x) => tx.insert(&self.rc_table, hash, x)?,
			None => tx.remove(&self.rc_table, hash)?,
		};
		Ok(matches!(new_rc, RcEntry::Deletable { .. }))
	}

	/// Read a block's reference count
	pub(crate) fn get_block_rc(&self, hash: &Hash) -> Result<RcEntry, Error> {
		Ok(RcEntry::parse_opt(self.rc_table.get(hash.as_ref())?))
	}

	/// Delete an entry in the RC table if it is deletable and the
	/// deletion time has passed
	pub(crate) fn clear_deleted_block_rc(&self, hash: &Hash) -> Result<(), Error> {
		let now = now_msec();
		self.rc_table.db().transaction(|tx| {
			let rcval = RcEntry::parse_opt(tx.get(&self.rc_table, hash)?);
			match rcval {
				RcEntry::Deletable { at_time } if now > at_time => {
					tx.remove(&self.rc_table, hash)?;
				}
				_ => (),
			};
			Ok(())
		})?;
		Ok(())
	}

	/// Recalculate the reference counter of a block
	/// to fix potential inconsistencies
	pub fn recalculate_rc(&self, hash: &Hash) -> Result<(usize, bool), Error> {
		if let Some(recalc_fns) = self.recalc_rc.load().as_ref() {
			trace!("Repair block RC for {:?}", hash);
			let res = self
				.rc_table
				.db()
				.transaction(|tx| {
					let mut cnt = 0;
					for f in recalc_fns.iter() {
						cnt += f(&tx, hash)?;
					}
					let old_rc = RcEntry::parse_opt(tx.get(&self.rc_table, hash)?);
					trace!(
						"Block RC for {:?}: stored={}, calculated={}",
						hash,
						old_rc.as_u64(),
						cnt
					);
					if cnt as u64 != old_rc.as_u64() {
						warn!(
							"Fixing inconsistent block RC for {:?}: was {}, should be {}",
							hash,
							old_rc.as_u64(),
							cnt
						);
						let new_rc = if cnt > 0 {
							RcEntry::Present { count: cnt as u64 }
						} else {
							RcEntry::Deletable {
								at_time: now_msec() + BLOCK_GC_DELAY.as_millis() as u64,
							}
						};
						tx.insert(&self.rc_table, hash, new_rc.serialize().unwrap())?;
						Ok((cnt, true))
					} else {
						Ok((cnt, false))
					}
				})
				.map_err(Error::from);
			if let Err(e) = &res {
				error!("Failed to fix RC for block {:?}: {}", hash, e);
			}
			res
		} else {
			Err(Error::Message(
				"Block RC recalculation is not available at this point".into(),
			))
		}
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

	pub(crate) fn as_u64(&self) -> u64 {
		match self {
			RcEntry::Present { count } => *count,
			_ => 0,
		}
	}
}
