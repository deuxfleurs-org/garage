use core::ops::Bound;

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use sled::transaction::{
	ConflictableTransactionError, TransactionError, Transactional, TransactionalTree,
	UnabortableTransactionError,
};

use crate::{
	Db, Error, IDb, ITx, ITxFn, Result, TxError, TxFnResult, TxOpError, TxOpResult, TxResult,
	TxValueIter, Value, ValueIter,
};

pub use sled;

// -- err

impl From<sled::Error> for Error {
	fn from(e: sled::Error) -> Error {
		Error(format!("Sled: {}", e).into())
	}
}

impl From<sled::Error> for TxOpError {
	fn from(e: sled::Error) -> TxOpError {
		TxOpError(e.into())
	}
}

// -- db

pub struct SledDb {
	db: sled::Db,
	trees: RwLock<(Vec<sled::Tree>, HashMap<String, usize>)>,
}

impl SledDb {
	pub fn init(db: sled::Db) -> Db {
		let s = Self {
			db,
			trees: RwLock::new((Vec::new(), HashMap::new())),
		};
		Db(Arc::new(s))
	}

	fn get_tree(&self, i: usize) -> Result<sled::Tree> {
		self.trees
			.read()
			.unwrap()
			.0
			.get(i)
			.cloned()
			.ok_or_else(|| Error("invalid tree id".into()))
	}
}

impl IDb for SledDb {
	fn engine(&self) -> String {
		"Sled".into()
	}

	fn open_tree(&self, name: &str) -> Result<usize> {
		let mut trees = self.trees.write().unwrap();
		if let Some(i) = trees.1.get(name) {
			Ok(*i)
		} else {
			let tree = self.db.open_tree(name)?;
			let i = trees.0.len();
			trees.0.push(tree);
			trees.1.insert(name.to_string(), i);
			Ok(i)
		}
	}

	fn list_trees(&self) -> Result<Vec<String>> {
		let mut trees = vec![];
		for name in self.db.tree_names() {
			let name = std::str::from_utf8(&name)
				.map_err(|e| Error(format!("{}", e).into()))?
				.to_string();
			if name != "__sled__default" {
				trees.push(name);
			}
		}
		Ok(trees)
	}

	// ----

	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value>> {
		let tree = self.get_tree(tree)?;
		let val = tree.get(key)?;
		Ok(val.map(|x| x.to_vec()))
	}

	fn len(&self, tree: usize) -> Result<usize> {
		let tree = self.get_tree(tree)?;
		Ok(tree.len())
	}

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<Option<Value>> {
		let tree = self.get_tree(tree)?;
		let old_val = tree.insert(key, value)?;
		Ok(old_val.map(|x| x.to_vec()))
	}

	fn remove(&self, tree: usize, key: &[u8]) -> Result<Option<Value>> {
		let tree = self.get_tree(tree)?;
		let old_val = tree.remove(key)?;
		Ok(old_val.map(|x| x.to_vec()))
	}

	fn iter(&self, tree: usize) -> Result<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		Ok(Box::new(tree.iter().map(|v| {
			v.map(|(x, y)| (x.to_vec(), y.to_vec())).map_err(Into::into)
		})))
	}

	fn iter_rev(&self, tree: usize) -> Result<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		Ok(Box::new(tree.iter().rev().map(|v| {
			v.map(|(x, y)| (x.to_vec(), y.to_vec())).map_err(Into::into)
		})))
	}

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		Ok(Box::new(tree.range::<&'r [u8], _>((low, high)).map(|v| {
			v.map(|(x, y)| (x.to_vec(), y.to_vec())).map_err(Into::into)
		})))
	}
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		Ok(Box::new(tree.range::<&'r [u8], _>((low, high)).rev().map(
			|v| v.map(|(x, y)| (x.to_vec(), y.to_vec())).map_err(Into::into),
		)))
	}

	// ----

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<(), ()> {
		let trees = self.trees.read().unwrap();
		let res = trees.0.transaction(|txtrees| {
			let mut tx = SledTx {
				trees: txtrees,
				err: Cell::new(None),
			};
			match f.try_on(&mut tx) {
				TxFnResult::Ok => {
					assert!(tx.err.into_inner().is_none());
					Ok(())
				}
				TxFnResult::Abort => {
					assert!(tx.err.into_inner().is_none());
					Err(ConflictableTransactionError::Abort(()))
				}
				TxFnResult::DbErr => {
					let e = tx.err.into_inner().expect("No DB error");
					Err(e.into())
				}
			}
		});
		match res {
			Ok(()) => Ok(()),
			Err(TransactionError::Abort(())) => Err(TxError::Abort(())),
			Err(TransactionError::Storage(s)) => Err(TxError::Db(s.into())),
		}
	}
}

// ----

struct SledTx<'a> {
	trees: &'a [TransactionalTree],
	err: Cell<Option<UnabortableTransactionError>>,
}

impl<'a> SledTx<'a> {
	fn get_tree(&self, i: usize) -> TxOpResult<&TransactionalTree> {
		self.trees.get(i).ok_or_else(|| {
			TxOpError(Error(
				"invalid tree id (it might have been openned after the transaction started)".into(),
			))
		})
	}

	fn save_error<R>(
		&self,
		v: std::result::Result<R, UnabortableTransactionError>,
	) -> TxOpResult<R> {
		match v {
			Ok(x) => Ok(x),
			Err(e) => {
				let txt = format!("{}", e);
				self.err.set(Some(e));
				Err(TxOpError(Error(txt.into())))
			}
		}
	}
}

impl<'a> ITx for SledTx<'a> {
	fn get(&self, tree: usize, key: &[u8]) -> TxOpResult<Option<Value>> {
		let tree = self.get_tree(tree)?;
		let tmp = self.save_error(tree.get(key))?;
		Ok(tmp.map(|x| x.to_vec()))
	}
	fn len(&self, _tree: usize) -> TxOpResult<usize> {
		unimplemented!(".len() in transaction not supported with Sled backend")
	}

	fn insert(&mut self, tree: usize, key: &[u8], value: &[u8]) -> TxOpResult<Option<Value>> {
		let tree = self.get_tree(tree)?;
		let old_val = self.save_error(tree.insert(key, value))?;
		Ok(old_val.map(|x| x.to_vec()))
	}
	fn remove(&mut self, tree: usize, key: &[u8]) -> TxOpResult<Option<Value>> {
		let tree = self.get_tree(tree)?;
		let old_val = self.save_error(tree.remove(key))?;
		Ok(old_val.map(|x| x.to_vec()))
	}

	fn iter(&self, _tree: usize) -> TxOpResult<TxValueIter<'_>> {
		unimplemented!("Iterators in transactions not supported with Sled backend");
	}
	fn iter_rev(&self, _tree: usize) -> TxOpResult<TxValueIter<'_>> {
		unimplemented!("Iterators in transactions not supported with Sled backend");
	}

	fn range<'r>(
		&self,
		_tree: usize,
		_low: Bound<&'r [u8]>,
		_high: Bound<&'r [u8]>,
	) -> TxOpResult<TxValueIter<'_>> {
		unimplemented!("Iterators in transactions not supported with Sled backend");
	}
	fn range_rev<'r>(
		&self,
		_tree: usize,
		_low: Bound<&'r [u8]>,
		_high: Bound<&'r [u8]>,
	) -> TxOpResult<TxValueIter<'_>> {
		unimplemented!("Iterators in transactions not supported with Sled backend");
	}
}
