use core::ops::Bound;

use std::path::Path;
use std::sync::Arc;

use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};

use fjall::{
	KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace,
	OptimisticWriteTx as WriteTransaction, PersistMode, Readable,
};

use crate::{
	open::{Engine, OpenOpt},
	Db, DbError, DbResult, Error, IDb, ITx, ITxFn, OnCommit, TxError, TxFnResult, TxOpError,
	TxResult, TxValueIter, Value, ValueIter,
};

pub use fjall;

// --

pub(crate) fn open_db(path: &Path, opt: &OpenOpt) -> DbResult<Db> {
	info!("Opening Fjall database at: {}", path.display());
	if opt.fsync {
		return Err(DbError(
			"metadata_fsync is not supported with the Fjall database engine".into(),
		));
	}

	let mut config = OptimisticTxDatabase::builder(path);
	if let Some(block_cache_size) = opt.fjall_block_cache_size {
		config = config.cache_size(block_cache_size as u64);
	}

	let db = config.open()?;
	Ok(FjallDb::init(db, opt.fsync))
}

// -- err

impl From<fjall::Error> for DbError {
	fn from(e: fjall::Error) -> DbError {
		DbError(format!("fjall: {}", e).into())
	}
}

impl From<fjall::LsmError> for DbError {
	fn from(e: fjall::LsmError) -> DbError {
		DbError(format!("fjall lsm_tree: {}", e).into())
	}
}

impl From<fjall::Error> for Error {
	fn from(e: fjall::Error) -> Error {
		Error::Db(DbError::from(e))
	}
}

impl From<fjall::Error> for TxOpError {
	fn from(e: fjall::Error) -> TxOpError {
		DbError::from(e).into()
	}
}

// -- db

pub struct FjallDb {
	db: OptimisticTxDatabase,
	trees: RwLock<Vec<(String, OptimisticTxKeyspace)>>,
	persist_mode: PersistMode,
}

type ByteRefRangeBound<'r> = (Bound<&'r [u8]>, Bound<&'r [u8]>);

impl FjallDb {
	pub fn init(db: OptimisticTxDatabase, fsync: bool) -> Db {
		let s = Self {
			db,
			trees: RwLock::new(Vec::new()),
			persist_mode: if fsync {
				PersistMode::SyncAll
			} else {
				PersistMode::Buffer
			},
		};
		Db(Arc::new(s))
	}

	fn get_tree(&self, i: usize) -> DbResult<MappedRwLockReadGuard<'_, OptimisticTxKeyspace>> {
		RwLockReadGuard::try_map(self.trees.read(), |trees: &Vec<_>| {
			trees.get(i).map(|tup| &tup.1)
		})
		.map_err(|_| DbError("invalid tree id".into()))
	}
}

impl IDb for FjallDb {
	fn engine(&self) -> String {
		"Fjall 3 (EXPERIMENTAL!)".into()
	}

	fn open_tree(&self, name: &str) -> DbResult<usize> {
		let mut trees = self.trees.write();
		let safe_name = encode_name(name)?;
		if let Some(i) = trees.iter().position(|(name, _)| *name == safe_name) {
			Ok(i)
		} else {
			let tree = self
				.db
				.keyspace(&safe_name, KeyspaceCreateOptions::default)?;
			let i = trees.len();
			trees.push((safe_name, tree));
			Ok(i)
		}
	}

	fn list_trees(&self) -> DbResult<Vec<String>> {
		self.db
			.list_keyspace_names()
			.iter()
			.map(|n| decode_name(n))
			.collect::<DbResult<Vec<_>>>()
	}

	fn snapshot(&self, base_path: &Path) -> DbResult<()> {
		std::fs::create_dir_all(base_path)?;
		let path = Engine::Fjall.db_path(base_path);

		let source_state = self.db.read_tx();
		let copy_keyspace = fjall::OptimisticTxDatabase::builder(path).open()?;

		for tree_name in self.db.list_keyspace_names() {
			let source_tree = self
				.db
				.keyspace(&tree_name, KeyspaceCreateOptions::default)?;

			let copy_tree = copy_keyspace.keyspace(&tree_name, KeyspaceCreateOptions::default)?;

			for entry in source_state.iter(&source_tree) {
				let (key, value) = entry.into_inner()?;
				copy_tree.insert(key, value)?;
			}
		}

		copy_keyspace.persist(PersistMode::SyncAll)?;
		Ok(())
	}

	// ----

	fn get(&self, tree_idx: usize, key: &[u8]) -> DbResult<Option<Value>> {
		let tree = self.get_tree(tree_idx)?;
		let tx = self.db.read_tx();
		let val = tx.get(&*tree, key)?;
		match val {
			None => Ok(None),
			Some(v) => Ok(Some(v.to_vec())),
		}
	}

	fn approximate_len(&self, tree_idx: usize) -> DbResult<usize> {
		let tree = self.get_tree(tree_idx)?;
		Ok(tree.approximate_len())
	}
	fn is_empty(&self, tree_idx: usize) -> DbResult<bool> {
		let tree = self.get_tree(tree_idx)?;
		let tx = self.db.read_tx();
		Ok(tx.is_empty(&*tree)?)
	}

	fn insert(&self, tree_idx: usize, key: &[u8], value: &[u8]) -> DbResult<()> {
		let tree = self.get_tree(tree_idx)?;

		loop {
			let mut tx = self.db.write_tx()?.durability(Some(self.persist_mode));
			tx.insert(&*tree, key, value);

			if tx.commit()?.is_ok() {
				break;
			}
		}

		Ok(())
	}

	fn remove(&self, tree_idx: usize, key: &[u8]) -> DbResult<()> {
		let tree = self.get_tree(tree_idx)?;

		loop {
			let mut tx = self.db.write_tx()?.durability(Some(self.persist_mode));
			tx.remove(&*tree, key);

			if tx.commit()?.is_ok() {
				break;
			}
		}

		Ok(())
	}

	fn clear(&self, tree_idx: usize) -> DbResult<()> {
		let mut trees = self.trees.write();

		if tree_idx >= trees.len() {
			return Err(DbError("invalid tree id".into()));
		}
		let (name, tree) = trees.remove(tree_idx);

		self.db.inner().delete_keyspace(tree.inner().clone())?;
		let tree = self.db.keyspace(&name, KeyspaceCreateOptions::default)?;
		trees.insert(tree_idx, (name, tree));

		Ok(())
	}

	fn iter(&self, tree_idx: usize) -> DbResult<ValueIter<'_>> {
		let tree = self.get_tree(tree_idx)?;
		let tx = self.db.read_tx();
		Ok(Box::new(tx.iter(&*tree).map(iterator_remap)))
	}

	fn iter_rev(&self, tree_idx: usize) -> DbResult<ValueIter<'_>> {
		let tree = self.get_tree(tree_idx)?;
		let tx = self.db.read_tx();
		Ok(Box::new(tx.iter(&*tree).rev().map(iterator_remap)))
	}

	fn range<'r>(
		&self,
		tree_idx: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> DbResult<ValueIter<'_>> {
		let tree = self.get_tree(tree_idx)?;
		let tx = self.db.read_tx();
		Ok(Box::new(
			tx.range::<&'r [u8], ByteRefRangeBound>(&*tree, (low, high))
				.map(iterator_remap),
		))
	}
	fn range_rev<'r>(
		&self,
		tree_idx: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> DbResult<ValueIter<'_>> {
		let tree = self.get_tree(tree_idx)?;
		let tx = self.db.read_tx();
		Ok(Box::new(
			tx.range::<&'r [u8], ByteRefRangeBound>(&*tree, (low, high))
				.rev()
				.map(iterator_remap),
		))
	}

	// ----

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<OnCommit, ()> {
		let trees = self.trees.read();

		loop {
			let mut tx = FjallTx {
				trees: &*trees,
				tx: self
					.db
					.write_tx()
					.map_err(Error::from)
					.map_err(TxError::Db)?
					.durability(Some(self.persist_mode)),
			};

			match f.try_on(&mut tx) {
				TxFnResult::Ok(on_commit) => {
					if tx
						.tx
						.commit()
						.map_err(Error::from)
						.map_err(TxError::Db)?
						.is_ok()
					{
						return Ok(on_commit);
					}
				}
				TxFnResult::Abort => {
					tx.tx.rollback();
					return Err(TxError::Abort(()));
				}
				TxFnResult::DbErr => {
					tx.tx.rollback();
					return Err(TxError::Db(Error::Db(DbError(
						"(this message will be discarded)".into(),
					))));
				}
			}
		}
	}
}

// ----

struct FjallTx<'a> {
	trees: &'a [(String, OptimisticTxKeyspace)],
	tx: WriteTransaction,
}

impl<'a> FjallTx<'a> {
	fn get_tree(&self, i: usize) -> DbResult<&OptimisticTxKeyspace> {
		self.trees.get(i).map(|tup| &tup.1).ok_or_else(|| {
			DbError(
				"invalid tree id (it might have been opened after the transaction started)".into(),
			)
		})
	}
}

impl<'a> ITx for FjallTx<'a> {
	fn get(&self, tree_idx: usize, key: &[u8]) -> DbResult<Option<Value>> {
		let tree = self.get_tree(tree_idx)?;
		match self.tx.get(tree, key)? {
			Some(v) => Ok(Some(v.to_vec())),
			None => Ok(None),
		}
	}
	fn len(&self, tree_idx: usize) -> DbResult<usize> {
		let tree = self.get_tree(tree_idx)?;
		Ok(self.tx.len(tree)?)
	}

	fn insert(&mut self, tree_idx: usize, key: &[u8], value: &[u8]) -> DbResult<()> {
		let tree = self.get_tree(tree_idx)?.clone();
		self.tx.insert(&tree, key, value);
		Ok(())
	}
	fn remove(&mut self, tree_idx: usize, key: &[u8]) -> DbResult<()> {
		let tree = self.get_tree(tree_idx)?.clone();
		self.tx.remove(&tree, key);
		Ok(())
	}
	fn clear(&mut self, _tree_idx: usize) -> DbResult<()> {
		unimplemented!("LSM tree clearing in cross-partition transaction is not supported")
	}

	fn iter(&self, tree_idx: usize) -> DbResult<TxValueIter<'_>> {
		let tree = self.get_tree(tree_idx)?.clone();
		Ok(Box::new(self.tx.iter(&tree).map(iterator_remap)))
	}
	fn iter_rev(&self, tree_idx: usize) -> DbResult<TxValueIter<'_>> {
		let tree = self.get_tree(tree_idx)?.clone();
		Ok(Box::new(self.tx.iter(&tree).rev().map(iterator_remap)))
	}

	fn range<'r>(
		&self,
		tree_idx: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> DbResult<TxValueIter<'_>> {
		let tree = self.get_tree(tree_idx)?;
		let low = clone_bound(low);
		let high = clone_bound(high);
		Ok(Box::new(
			self.tx
				.range::<Vec<u8>, ByteVecRangeBounds>(tree, (low, high))
				.map(iterator_remap),
		))
	}
	fn range_rev<'r>(
		&self,
		tree_idx: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> DbResult<TxValueIter<'_>> {
		let tree = self.get_tree(tree_idx)?;
		let low = clone_bound(low);
		let high = clone_bound(high);
		Ok(Box::new(
			self.tx
				.range::<Vec<u8>, ByteVecRangeBounds>(tree, (low, high))
				.rev()
				.map(iterator_remap),
		))
	}
}

// -- maps fjall's (k, v) to ours

fn iterator_remap(r: fjall::Guard) -> DbResult<(Value, Value)> {
	r.into_inner()
		.map(|(k, v)| (k.to_vec(), v.to_vec()))
		.map_err(DbError::from)
}

// -- utils to deal with Garage's tightness on Bound lifetimes

type ByteVecBound = Bound<Vec<u8>>;
type ByteVecRangeBounds = (ByteVecBound, ByteVecBound);

fn clone_bound(bound: Bound<&[u8]>) -> ByteVecBound {
	let value = match bound {
		Bound::Excluded(v) | Bound::Included(v) => v.to_vec(),
		Bound::Unbounded => vec![],
	};

	match bound {
		Bound::Included(_) => Bound::Included(value),
		Bound::Excluded(_) => Bound::Excluded(value),
		Bound::Unbounded => Bound::Unbounded,
	}
}

// -- utils to encode table names --

fn encode_name(s: &str) -> DbResult<String> {
	let base = 'A' as u32;

	let mut ret = String::with_capacity(s.len() + 10);
	for c in s.chars() {
		if c.is_alphanumeric() || c == '_' || c == '-' || c == '#' {
			ret.push(c);
		} else if c <= u8::MAX as char {
			ret.push('$');
			let c_hi = c as u32 / 16;
			let c_lo = c as u32 % 16;
			ret.push(char::from_u32(base + c_hi).unwrap());
			ret.push(char::from_u32(base + c_lo).unwrap());
		} else {
			return Err(DbError(
				format!("table name {} could not be safely encoded", s).into(),
			));
		}
	}
	Ok(ret)
}

fn decode_name(s: &str) -> DbResult<String> {
	use std::convert::TryFrom;

	let errfn = || DbError(format!("encoded table name {} is invalid", s).into());
	let c_map = |c: char| {
		let c = c as u32;
		let base = 'A' as u32;
		if (base..base + 16).contains(&c) {
			Some(c - base)
		} else {
			None
		}
	};

	let mut ret = String::with_capacity(s.len());
	let mut it = s.chars();
	while let Some(c) = it.next() {
		if c == '$' {
			let c_hi = it.next().and_then(c_map).ok_or_else(errfn)?;
			let c_lo = it.next().and_then(c_map).ok_or_else(errfn)?;
			let c_dec = char::try_from(c_hi * 16 + c_lo).map_err(|_| errfn())?;
			ret.push(c_dec);
		} else {
			ret.push(c);
		}
	}
	Ok(ret)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_encdec_name() {
		for name in [
			"testname",
			"test_name",
			"test name",
			"test$name",
			"test:name@help.me$get/this**right",
		] {
			let encname = encode_name(name).unwrap();
			assert!(!encname.contains(' '));
			assert!(!encname.contains('.'));
			assert!(!encname.contains('*'));
			assert_eq!(*name, decode_name(&encname).unwrap());
		}
	}
}
