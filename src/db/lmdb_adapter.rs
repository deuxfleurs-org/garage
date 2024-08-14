use core::ops::Bound;
use core::ptr::NonNull;

use std::collections::HashMap;
use std::convert::TryInto;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use heed::types::ByteSlice;
use heed::{BytesDecode, Env, RoTxn, RwTxn, UntypedDatabase as Database};

use crate::{
	Db, Error, IDb, ITx, ITxFn, OnCommit, Result, TxError, TxFnResult, TxOpError, TxOpResult,
	TxResult, TxValueIter, Value, ValueIter,
};

pub use heed;

// -- err

impl From<heed::Error> for Error {
	fn from(e: heed::Error) -> Error {
		Error(format!("LMDB: {}", e).into())
	}
}

impl From<heed::Error> for TxOpError {
	fn from(e: heed::Error) -> TxOpError {
		TxOpError(e.into())
	}
}

// -- db

pub struct LmdbDb {
	db: heed::Env,
	trees: RwLock<(Vec<Database>, HashMap<String, usize>)>,
}

impl LmdbDb {
	pub fn init(db: Env) -> Db {
		let s = Self {
			db,
			trees: RwLock::new((Vec::new(), HashMap::new())),
		};
		Db(Arc::new(s))
	}

	fn get_tree(&self, i: usize) -> Result<Database> {
		self.trees
			.read()
			.unwrap()
			.0
			.get(i)
			.cloned()
			.ok_or_else(|| Error("invalid tree id".into()))
	}
}

impl IDb for LmdbDb {
	fn engine(&self) -> String {
		"LMDB (using Heed crate)".into()
	}

	fn open_tree(&self, name: &str) -> Result<usize> {
		let mut trees = self.trees.write().unwrap();
		if let Some(i) = trees.1.get(name) {
			Ok(*i)
		} else {
			let tree = self.db.create_database(Some(name))?;
			let i = trees.0.len();
			trees.0.push(tree);
			trees.1.insert(name.to_string(), i);
			Ok(i)
		}
	}

	fn list_trees(&self) -> Result<Vec<String>> {
		let tree0 = match self.db.open_database::<heed::types::Str, ByteSlice>(None)? {
			Some(x) => x,
			None => return Ok(vec![]),
		};

		let mut ret = vec![];
		let tx = self.db.read_txn()?;
		for item in tree0.iter(&tx)? {
			let (tree_name, _) = item?;
			ret.push(tree_name.to_string());
		}
		drop(tx);

		let mut ret2 = vec![];
		for tree_name in ret {
			if self
				.db
				.open_database::<ByteSlice, ByteSlice>(Some(&tree_name))?
				.is_some()
			{
				ret2.push(tree_name);
			}
		}

		Ok(ret2)
	}

	fn snapshot(&self, to: &PathBuf) -> Result<()> {
		std::fs::create_dir_all(to)?;
		let mut path = to.clone();
		path.push("data.mdb");
		self.db
			.copy_to_path(path, heed::CompactionOption::Disabled)?;
		Ok(())
	}

	// ----

	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value>> {
		let tree = self.get_tree(tree)?;

		let tx = self.db.read_txn()?;
		let val = tree.get(&tx, key)?;
		match val {
			None => Ok(None),
			Some(v) => Ok(Some(v.to_vec())),
		}
	}

	fn len(&self, tree: usize) -> Result<usize> {
		let tree = self.get_tree(tree)?;
		let tx = self.db.read_txn()?;
		Ok(tree.len(&tx)?.try_into().unwrap())
	}

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<Option<Value>> {
		let tree = self.get_tree(tree)?;
		let mut tx = self.db.write_txn()?;
		let old_val = tree.get(&tx, key)?.map(Vec::from);
		tree.put(&mut tx, key, value)?;
		tx.commit()?;
		Ok(old_val)
	}

	fn remove(&self, tree: usize, key: &[u8]) -> Result<Option<Value>> {
		let tree = self.get_tree(tree)?;
		let mut tx = self.db.write_txn()?;
		let old_val = tree.get(&tx, key)?.map(Vec::from);
		tree.delete(&mut tx, key)?;
		tx.commit()?;
		Ok(old_val)
	}

	fn clear(&self, tree: usize) -> Result<()> {
		let tree = self.get_tree(tree)?;
		let mut tx = self.db.write_txn()?;
		tree.clear(&mut tx)?;
		tx.commit()?;
		Ok(())
	}

	fn iter(&self, tree: usize) -> Result<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let tx = self.db.read_txn()?;
		TxAndIterator::make(tx, |tx| Ok(tree.iter(tx)?))
	}

	fn iter_rev(&self, tree: usize) -> Result<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let tx = self.db.read_txn()?;
		TxAndIterator::make(tx, |tx| Ok(tree.rev_iter(tx)?))
	}

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let tx = self.db.read_txn()?;
		TxAndIterator::make(tx, |tx| Ok(tree.range(tx, &(low, high))?))
	}
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let tx = self.db.read_txn()?;
		TxAndIterator::make(tx, |tx| Ok(tree.rev_range(tx, &(low, high))?))
	}

	// ----

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<OnCommit, ()> {
		let trees = self.trees.read().unwrap();
		let mut tx = LmdbTx {
			trees: &trees.0[..],
			tx: self
				.db
				.write_txn()
				.map_err(Error::from)
				.map_err(TxError::Db)?,
		};

		let res = f.try_on(&mut tx);
		match res {
			TxFnResult::Ok(on_commit) => {
				tx.tx.commit().map_err(Error::from).map_err(TxError::Db)?;
				Ok(on_commit)
			}
			TxFnResult::Abort => {
				tx.tx.abort().map_err(Error::from).map_err(TxError::Db)?;
				Err(TxError::Abort(()))
			}
			TxFnResult::DbErr => {
				tx.tx.abort().map_err(Error::from).map_err(TxError::Db)?;
				Err(TxError::Db(Error(
					"(this message will be discarded)".into(),
				)))
			}
		}
	}
}

// ----

pub(crate) struct LmdbTx<'a> {
	trees: &'a [Database],
	tx: RwTxn<'a, 'a>,
}

impl<'a> LmdbTx<'a> {
	fn get_tree(&self, i: usize) -> TxOpResult<&Database> {
		self.trees.get(i).ok_or_else(|| {
			TxOpError(Error(
				"invalid tree id (it might have been openned after the transaction started)".into(),
			))
		})
	}
}

impl<'a> ITx for LmdbTx<'a> {
	fn get(&self, tree: usize, key: &[u8]) -> TxOpResult<Option<Value>> {
		let tree = self.get_tree(tree)?;
		match tree.get(&self.tx, key)? {
			Some(v) => Ok(Some(v.to_vec())),
			None => Ok(None),
		}
	}
	fn len(&self, tree: usize) -> TxOpResult<usize> {
		let tree = self.get_tree(tree)?;
		Ok(tree.len(&self.tx)? as usize)
	}

	fn insert(&mut self, tree: usize, key: &[u8], value: &[u8]) -> TxOpResult<Option<Value>> {
		let tree = *self.get_tree(tree)?;
		let old_val = tree.get(&self.tx, key)?.map(Vec::from);
		tree.put(&mut self.tx, key, value)?;
		Ok(old_val)
	}
	fn remove(&mut self, tree: usize, key: &[u8]) -> TxOpResult<Option<Value>> {
		let tree = *self.get_tree(tree)?;
		let old_val = tree.get(&self.tx, key)?.map(Vec::from);
		tree.delete(&mut self.tx, key)?;
		Ok(old_val)
	}
	fn clear(&mut self, tree: usize) -> TxOpResult<()> {
		let tree = *self.get_tree(tree)?;
		tree.clear(&mut self.tx)?;
		Ok(())
	}

	fn iter(&self, tree: usize) -> TxOpResult<TxValueIter<'_>> {
		let tree = *self.get_tree(tree)?;
		Ok(Box::new(tree.iter(&self.tx)?.map(tx_iter_item)))
	}
	fn iter_rev(&self, tree: usize) -> TxOpResult<TxValueIter<'_>> {
		let tree = *self.get_tree(tree)?;
		Ok(Box::new(tree.rev_iter(&self.tx)?.map(tx_iter_item)))
	}

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> TxOpResult<TxValueIter<'_>> {
		let tree = *self.get_tree(tree)?;
		Ok(Box::new(
			tree.range(&self.tx, &(low, high))?.map(tx_iter_item),
		))
	}
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> TxOpResult<TxValueIter<'_>> {
		let tree = *self.get_tree(tree)?;
		Ok(Box::new(
			tree.rev_range(&self.tx, &(low, high))?.map(tx_iter_item),
		))
	}
}

// ---- iterators outside transactions ----
// complicated, they must hold the transaction object
// therefore a bit of unsafe code (it is a self-referential struct)

type IteratorItem<'a> = heed::Result<(
	<ByteSlice as BytesDecode<'a>>::DItem,
	<ByteSlice as BytesDecode<'a>>::DItem,
)>;

struct TxAndIterator<'a, I>
where
	I: Iterator<Item = IteratorItem<'a>> + 'a,
{
	tx: RoTxn<'a>,
	iter: Option<I>,
}

impl<'a, I> TxAndIterator<'a, I>
where
	I: Iterator<Item = IteratorItem<'a>> + 'a,
{
	fn make<F>(tx: RoTxn<'a>, iterfun: F) -> Result<ValueIter<'a>>
	where
		F: FnOnce(&'a RoTxn<'a>) -> Result<I>,
	{
		let res = TxAndIterator { tx, iter: None };
		let mut boxed = Box::pin(res);

		// This unsafe allows us to bypass lifetime checks
		let tx = unsafe { NonNull::from(&boxed.tx).as_ref() };
		let iter = iterfun(tx)?;

		let mut_ref = Pin::as_mut(&mut boxed);
		// This unsafe allows us to write in a field of the pinned struct
		unsafe {
			Pin::get_unchecked_mut(mut_ref).iter = Some(iter);
		}

		Ok(Box::new(TxAndIteratorPin(boxed)))
	}
}

impl<'a, I> Drop for TxAndIterator<'a, I>
where
	I: Iterator<Item = IteratorItem<'a>> + 'a,
{
	fn drop(&mut self) {
		// ensure the iterator is dropped before the RoTxn it references
		drop(self.iter.take());
	}
}

struct TxAndIteratorPin<'a, I>(Pin<Box<TxAndIterator<'a, I>>>)
where
	I: Iterator<Item = IteratorItem<'a>> + 'a;

impl<'a, I> Iterator for TxAndIteratorPin<'a, I>
where
	I: Iterator<Item = IteratorItem<'a>> + 'a,
{
	type Item = Result<(Value, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		let mut_ref = Pin::as_mut(&mut self.0);
		// This unsafe allows us to mutably access the iterator field
		let next = unsafe { Pin::get_unchecked_mut(mut_ref).iter.as_mut()?.next() };
		match next {
			None => None,
			Some(Err(e)) => Some(Err(e.into())),
			Some(Ok((k, v))) => Some(Ok((k.to_vec(), v.to_vec()))),
		}
	}
}

// ---- iterators within transactions ----

fn tx_iter_item<'a>(
	item: std::result::Result<(&'a [u8], &'a [u8]), heed::Error>,
) -> TxOpResult<(Vec<u8>, Vec<u8>)> {
	item.map(|(k, v)| (k.to_vec(), v.to_vec()))
		.map_err(|e| TxOpError(Error::from(e)))
}

// ---- utility ----

#[cfg(target_pointer_width = "64")]
pub fn recommended_map_size() -> usize {
	1usize << 40
}

#[cfg(target_pointer_width = "32")]
pub fn recommended_map_size() -> usize {
	tracing::warn!("LMDB is not recommended on 32-bit systems, database size will be limited");
	1usize << 30
}
