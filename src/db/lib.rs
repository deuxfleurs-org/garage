#[macro_use]
extern crate tracing;

#[cfg(feature = "fjall")]
pub mod fjall_adapter;
#[cfg(feature = "lmdb")]
pub mod lmdb_adapter;
#[cfg(feature = "sqlite")]
pub mod sqlite_adapter;

pub mod open;
pub mod typed;

#[cfg(test)]
pub mod test;

use core::ops::{Bound, RangeBounds};

use std::borrow::Cow;
use std::cell::Cell;
use std::path::Path;
use std::sync::Arc;

use thiserror::Error;

pub use open::*;
pub use typed::{DbBytes, DbOrdKey, TypedIter, TypedTree, TypedTxIter};

pub(crate) type OnCommit = Vec<Box<dyn FnOnce()>>;

#[derive(Clone)]
pub struct Db(pub(crate) Arc<dyn IDb>);

pub struct Transaction<'a> {
	tx: &'a mut dyn ITx,
	on_commit: OnCommit,
}

#[derive(Clone)]
pub struct Tree(Arc<dyn IDb>, usize);

pub type Value = Vec<u8>;
pub type ValueIter<'a> = Box<dyn std::iter::Iterator<Item = DbResult<(Value, Value)>> + 'a>;
pub type TxValueIter<'a> = Box<dyn std::iter::Iterator<Item = DbResult<(Value, Value)>> + 'a>;

// ----

#[derive(Debug, Error)]
#[error("database error: {0}")]
pub struct DbError(pub Cow<'static, str>);

#[derive(Debug, Error)]
#[error("decode error: {0}")]
pub struct DecodeError(pub Cow<'static, str>);

#[derive(Debug, Error)]
pub enum Error {
	#[error(transparent)]
	Db(#[from] DbError),
	#[error(transparent)]
	Decode(#[from] DecodeError),
}

impl From<std::io::Error> for DbError {
	fn from(e: std::io::Error) -> DbError {
		DbError(format!("IO: {}", e).into())
	}
}

pub type DbResult<T> = std::result::Result<T, DbError>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct TxOpError(pub(crate) Error);
pub type TxOpResult<T> = std::result::Result<T, TxOpError>;

impl From<DbError> for TxOpError {
	fn from(e: DbError) -> TxOpError {
		TxOpError(e.into())
	}
}

impl From<DecodeError> for TxOpError {
	fn from(e: DecodeError) -> TxOpError {
		TxOpError(e.into())
	}
}

#[derive(Debug)]
/// An empty enum used to represent errors in transaction that cannot abort.
///
/// Such transactions would have error type `TxError<Unabortable>`
pub enum Unabortable {}

#[derive(Debug)]
/// An error representing whether a transaction was aborted or if an underlying DB error happened.
///
/// The aborted case carry a payload of type `E`.
pub enum TxError<E> {
	Abort(E),
	Db(Error),
}
pub type TxResult<R, E> = std::result::Result<R, TxError<E>>;

impl TxError<Unabortable> {
	/// If the transaction is statically known to not have aborted, unwraps the underlying DB error.
	pub fn cannot_have_aborted(self) -> Error {
		match self {
			TxError::Db(e) => e,
		}
	}
}

impl<E> From<TxOpError> for TxError<E> {
	fn from(e: TxOpError) -> TxError<E> {
		TxError::Db(e.0)
	}
}

impl<E> From<DbError> for TxError<E> {
	fn from(e: DbError) -> TxError<E> {
		TxError::Db(e.into())
	}
}

pub fn unabort<R, E>(res: TxResult<R, E>) -> TxOpResult<std::result::Result<R, E>> {
	match res {
		Ok(v) => Ok(Ok(v)),
		Err(TxError::Abort(e)) => Ok(Err(e)),
		Err(TxError::Db(e)) => Err(TxOpError(e)),
	}
}

// ----

impl Db {
	pub fn engine(&self) -> String {
		self.0.engine()
	}

	pub fn open_tree<S: AsRef<str>>(&self, name: S) -> DbResult<Tree> {
		let tree_id = self.0.open_tree(name.as_ref())?;
		Ok(Tree(self.0.clone(), tree_id))
	}

	pub fn list_trees(&self) -> DbResult<Vec<String>> {
		self.0.list_trees()
	}

	pub fn transaction<R, E, F>(&self, fun: F) -> TxResult<R, E>
	where
		F: Fn(&mut Transaction<'_>) -> TxResult<R, E>,
	{
		let f = TxFn {
			function: fun,
			result: Cell::new(None),
		};
		let tx_res = self.0.transaction(&f);
		let fn_res = f.result.into_inner();

		match (tx_res, fn_res) {
			(Ok(on_commit), Some(Ok(value))) => {
				// Transaction succeeded
				// TxFn stored the value to return to the user in fn_res
				// tx_res contains the on_commit list of callbacks, run them now
				on_commit.into_iter().for_each(|f| f());
				Ok(value)
			}
			(Err(TxError::Abort(())), Some(Err(TxError::Abort(e)))) => {
				// Transaction was aborted by user code
				// The abort error value is stored in fn_res
				Err(TxError::Abort(e))
			}
			(Err(TxError::Db(_tx_e)), Some(Err(TxError::Db(fn_e)))) => {
				// Transaction encountered a DB error in user code
				// The error value encountered is the one in fn_res,
				// tx_res contains only a dummy error message
				Err(TxError::Db(fn_e))
			}
			(Err(TxError::Db(tx_e)), None) => {
				// Transaction encounterred a DB error when initializing the transaction,
				// before user code was called
				Err(TxError::Db(tx_e))
			}
			(Err(TxError::Db(tx_e)), Some(Ok(_))) => {
				// Transaction encounterred a DB error when committing the transaction,
				// after user code was called
				Err(TxError::Db(tx_e))
			}
			(tx_res, fn_res) => {
				panic!(
					"unexpected error case: tx_res={:?}, fn_res={:?}",
					tx_res.map(|_| "..."),
					fn_res.map(|x| x.map(|_| "...").map_err(|_| "..."))
				);
			}
		}
	}

	pub fn snapshot(&self, path: &Path) -> DbResult<()> {
		self.0.snapshot(path)
	}

	pub fn import(&self, other: &Db) -> Result<()> {
		let existing_trees = self.list_trees()?;
		if !existing_trees.is_empty() {
			return Err(DbError(
				format!(
					"destination database already contains data: {:?}",
					existing_trees
				)
				.into(),
			)
			.into());
		}

		let tree_names = other.list_trees()?;
		for name in tree_names {
			let tree = self.open_tree(&name)?;
			if !tree.is_empty()? {
				return Err(DbError(format!("tree {} already contains data", name).into()).into());
			}

			let ex_tree = other.open_tree(&name)?;

			let tx_res = self.transaction(|tx| {
				let mut i = 0;
				for item in ex_tree.iter().map_err(TxError::Abort)? {
					let (k, v) = item.map_err(TxError::Abort)?;
					tx.insert(&tree, k, v)?;
					i += 1;
					if i % 1000 == 0 {
						println!("{}: imported {}", name, i);
					}
				}
				Ok(i)
			});
			let total = match tx_res {
				Err(TxError::Db(e)) => return Err(e),
				Err(TxError::Abort(e)) => return Err(e.into()),
				Ok(x) => x,
			};

			println!("{}: finished importing, {} items", name, total);
		}
		Ok(())
	}
}

#[allow(clippy::len_without_is_empty)]
impl Tree {
	#[inline]
	pub fn db(&self) -> Db {
		Db(self.0.clone())
	}

	#[inline]
	pub fn get<T: AsRef<[u8]>>(&self, key: T) -> DbResult<Option<Value>> {
		self.0.get(self.1, key.as_ref())
	}
	#[inline]
	pub fn approximate_len(&self) -> DbResult<usize> {
		self.0.approximate_len(self.1)
	}
	#[inline]
	pub fn is_empty(&self) -> DbResult<bool> {
		self.0.is_empty(self.1)
	}

	#[inline]
	pub fn first(&self) -> DbResult<Option<(Value, Value)>> {
		self.iter()?.next().transpose()
	}
	#[inline]
	pub fn get_gt<T: AsRef<[u8]>>(&self, from: T) -> DbResult<Option<(Value, Value)>> {
		if from.as_ref().is_empty() {
			self.iter()?.next().transpose()
		} else {
			self.range((Bound::Excluded(from), Bound::Unbounded))?
				.next()
				.transpose()
		}
	}

	/// Returns the old value if there was one
	#[inline]
	pub fn insert<T: AsRef<[u8]>, U: AsRef<[u8]>>(&self, key: T, value: U) -> DbResult<()> {
		self.0.insert(self.1, key.as_ref(), value.as_ref())
	}
	/// Returns the old value if there was one
	#[inline]
	pub fn remove<T: AsRef<[u8]>>(&self, key: T) -> DbResult<()> {
		self.0.remove(self.1, key.as_ref())
	}
	/// Clears all values from the tree
	#[inline]
	pub fn clear(&self) -> DbResult<()> {
		self.0.clear(self.1)
	}

	#[inline]
	pub fn iter(&self) -> DbResult<ValueIter<'_>> {
		self.0.iter(self.1)
	}
	#[inline]
	pub fn iter_rev(&self) -> DbResult<ValueIter<'_>> {
		self.0.iter_rev(self.1)
	}

	#[inline]
	pub fn range<K, R>(&self, range: R) -> DbResult<ValueIter<'_>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.0.range(self.1, get_bound(sb), get_bound(eb))
	}
	#[inline]
	pub fn range_rev<K, R>(&self, range: R) -> DbResult<ValueIter<'_>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.0.range_rev(self.1, get_bound(sb), get_bound(eb))
	}
}

#[allow(clippy::len_without_is_empty)]
impl<'a> Transaction<'a> {
	#[inline]
	pub fn get<T: AsRef<[u8]>>(&self, tree: &Tree, key: T) -> TxOpResult<Option<Value>> {
		self.tx.get(tree.1, key.as_ref()).map_err(Into::into)
	}
	#[inline]
	pub fn len(&self, tree: &Tree) -> TxOpResult<usize> {
		self.tx.len(tree.1).map_err(Into::into)
	}

	/// Returns the old value if there was one
	#[inline]
	pub fn insert<T: AsRef<[u8]>, U: AsRef<[u8]>>(
		&mut self,
		tree: &Tree,
		key: T,
		value: U,
	) -> TxOpResult<()> {
		self.tx
			.insert(tree.1, key.as_ref(), value.as_ref())
			.map_err(Into::into)
	}
	/// Returns the old value if there was one
	#[inline]
	pub fn remove<T: AsRef<[u8]>>(&mut self, tree: &Tree, key: T) -> TxOpResult<()> {
		self.tx.remove(tree.1, key.as_ref()).map_err(Into::into)
	}
	/// Clears all values in a tree
	#[inline]
	pub fn clear(&mut self, tree: &Tree) -> TxOpResult<()> {
		self.tx.clear(tree.1).map_err(Into::into)
	}

	#[inline]
	pub fn iter(&self, tree: &Tree) -> TxOpResult<TxValueIter<'_>> {
		self.tx.iter(tree.1).map_err(Into::into)
	}
	#[inline]
	pub fn iter_rev(&self, tree: &Tree) -> TxOpResult<TxValueIter<'_>> {
		self.tx.iter_rev(tree.1).map_err(Into::into)
	}

	#[inline]
	pub fn range<K, R>(&self, tree: &Tree, range: R) -> TxOpResult<TxValueIter<'_>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.tx
			.range(tree.1, get_bound(sb), get_bound(eb))
			.map_err(Into::into)
	}
	#[inline]
	pub fn range_rev<K, R>(&self, tree: &Tree, range: R) -> TxOpResult<TxValueIter<'_>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.tx
			.range_rev(tree.1, get_bound(sb), get_bound(eb))
			.map_err(Into::into)
	}

	#[inline]
	pub fn on_commit<F: FnOnce() + 'static>(&mut self, f: F) {
		self.on_commit.push(Box::new(f));
	}
}

// ---- Internal interfaces

pub(crate) trait IDb: Send + Sync {
	fn engine(&self) -> String;
	fn open_tree(&self, name: &str) -> DbResult<usize>;
	fn list_trees(&self) -> DbResult<Vec<String>>;
	fn snapshot(&self, path: &Path) -> DbResult<()>;

	fn get(&self, tree: usize, key: &[u8]) -> DbResult<Option<Value>>;
	fn approximate_len(&self, tree: usize) -> DbResult<usize>;
	fn is_empty(&self, tree: usize) -> DbResult<bool>;

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> DbResult<()>;
	fn remove(&self, tree: usize, key: &[u8]) -> DbResult<()>;
	fn clear(&self, tree: usize) -> DbResult<()>;

	fn iter(&self, tree: usize) -> DbResult<ValueIter<'_>>;
	fn iter_rev(&self, tree: usize) -> DbResult<ValueIter<'_>>;

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> DbResult<ValueIter<'_>>;
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> DbResult<ValueIter<'_>>;

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<OnCommit, ()>;
}

pub(crate) trait ITx {
	fn get(&self, tree: usize, key: &[u8]) -> DbResult<Option<Value>>;
	fn len(&self, tree: usize) -> DbResult<usize>;

	fn insert(&mut self, tree: usize, key: &[u8], value: &[u8]) -> DbResult<()>;
	fn remove(&mut self, tree: usize, key: &[u8]) -> DbResult<()>;
	fn clear(&mut self, tree: usize) -> DbResult<()>;

	fn iter(&self, tree: usize) -> DbResult<TxValueIter<'_>>;
	fn iter_rev(&self, tree: usize) -> DbResult<TxValueIter<'_>>;

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> DbResult<TxValueIter<'_>>;
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> DbResult<TxValueIter<'_>>;
}

pub(crate) trait ITxFn {
	fn try_on(&self, tx: &mut dyn ITx) -> TxFnResult;
}

pub(crate) enum TxFnResult {
	Ok(OnCommit),
	Abort,
	DbErr,
}

struct TxFn<F, R, E>
where
	F: Fn(&mut Transaction<'_>) -> TxResult<R, E>,
{
	function: F,
	result: Cell<Option<TxResult<R, E>>>,
}

impl<F, R, E> ITxFn for TxFn<F, R, E>
where
	F: Fn(&mut Transaction<'_>) -> TxResult<R, E>,
{
	fn try_on(&self, tx: &mut dyn ITx) -> TxFnResult {
		let mut tx = Transaction {
			tx,
			on_commit: vec![],
		};
		let res = (self.function)(&mut tx);
		let res2 = match &res {
			Ok(_) => TxFnResult::Ok(tx.on_commit),
			Err(TxError::Abort(_)) => TxFnResult::Abort,
			Err(TxError::Db(_)) => TxFnResult::DbErr,
		};
		self.result.set(Some(res));
		res2
	}
}

// ----

fn get_bound<K: AsRef<[u8]>>(b: Bound<&K>) -> Bound<&[u8]> {
	match b {
		Bound::Included(v) => Bound::Included(v.as_ref()),
		Bound::Excluded(v) => Bound::Excluded(v.as_ref()),
		Bound::Unbounded => Bound::Unbounded,
	}
}
