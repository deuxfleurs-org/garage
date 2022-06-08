pub mod lmdb_adapter;
pub mod sled_adapter;
pub mod sqlite_adapter;

pub mod counted_tree_hack;

#[cfg(test)]
pub mod test;

use core::ops::{Bound, RangeBounds};

use std::borrow::Cow;
use std::cell::Cell;
use std::sync::Arc;

use err_derive::Error;

#[derive(Clone)]
pub struct Db(pub(crate) Arc<dyn IDb>);

pub struct Transaction<'a>(&'a mut dyn ITx);

#[derive(Clone)]
pub struct Tree(Arc<dyn IDb>, usize);

pub type Value = Vec<u8>;
pub type ValueIter<'a> = Box<dyn std::iter::Iterator<Item = Result<(Value, Value)>> + 'a>;
pub type TxValueIter<'a> = Box<dyn std::iter::Iterator<Item = TxOpResult<(Value, Value)>> + 'a>;

// ----

#[derive(Debug, Error)]
#[error(display = "{}", _0)]
pub struct Error(pub Cow<'static, str>);

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
#[error(display = "{}", _0)]
pub struct TxOpError(pub(crate) Error);
pub type TxOpResult<T> = std::result::Result<T, TxOpError>;

pub enum TxError<E> {
	Abort(E),
	Db(Error),
}
pub type TxResult<R, E> = std::result::Result<R, TxError<E>>;

impl<E> From<TxOpError> for TxError<E> {
	fn from(e: TxOpError) -> TxError<E> {
		TxError::Db(e.0)
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

	pub fn open_tree<S: AsRef<str>>(&self, name: S) -> Result<Tree> {
		let tree_id = self.0.open_tree(name.as_ref())?;
		Ok(Tree(self.0.clone(), tree_id))
	}

	pub fn list_trees(&self) -> Result<Vec<String>> {
		self.0.list_trees()
	}

	pub fn transaction<R, E, F>(&self, fun: F) -> TxResult<R, E>
	where
		F: Fn(Transaction<'_>) -> TxResult<R, E>,
	{
		let f = TxFn {
			function: fun,
			result: Cell::new(None),
		};
		let tx_res = self.0.transaction(&f);
		let ret = f
			.result
			.into_inner()
			.expect("Transaction did not store result");

		match tx_res {
			Ok(()) => {
				assert!(matches!(ret, Ok(_)));
				ret
			}
			Err(TxError::Abort(())) => {
				assert!(matches!(ret, Err(TxError::Abort(_))));
				ret
			}
			Err(TxError::Db(e2)) => match ret {
				// Ok was stored -> the error occured when finalizing
				// transaction
				Ok(_) => Err(TxError::Db(e2)),
				// An error was already stored: that's the one we want to
				// return
				Err(TxError::Db(e)) => Err(TxError::Db(e)),
				_ => unreachable!(),
			},
		}
	}

	pub fn import(&self, other: &Db) -> Result<()> {
		let existing_trees = self.list_trees()?;
		if !existing_trees.is_empty() {
			return Err(Error(
				format!(
					"destination database already contains data: {:?}",
					existing_trees
				)
				.into(),
			));
		}

		let tree_names = other.list_trees()?;
		for name in tree_names {
			let tree = self.open_tree(&name)?;
			if tree.len()? > 0 {
				return Err(Error(format!("tree {} already contains data", name).into()));
			}

			let ex_tree = other.open_tree(&name)?;

			let tx_res = self.transaction(|mut tx| {
				let mut i = 0;
				for item in ex_tree.iter().map_err(TxError::Abort)? {
					let (k, v) = item.map_err(TxError::Abort)?;
					tx.insert(&tree, k, v)?;
					i += 1;
					if i % 1000 == 0 {
						println!("{}: imported {}", name, i);
					}
				}
				tx.commit(i)
			});
			let total = match tx_res {
				Err(TxError::Db(e)) => return Err(e),
				Err(TxError::Abort(e)) => return Err(e),
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
	pub fn get<T: AsRef<[u8]>>(&self, key: T) -> Result<Option<Value>> {
		self.0.get(self.1, key.as_ref())
	}
	#[inline]
	pub fn len(&self) -> Result<usize> {
		self.0.len(self.1)
	}

	#[inline]
	pub fn first(&self) -> Result<Option<(Value, Value)>> {
		self.iter()?.next().transpose()
	}
	#[inline]
	pub fn get_gt<T: AsRef<[u8]>>(&self, from: T) -> Result<Option<(Value, Value)>> {
		self.range((Bound::Excluded(from), Bound::Unbounded))?
			.next()
			.transpose()
	}

	/// Returns the old value if there was one
	#[inline]
	pub fn insert<T: AsRef<[u8]>, U: AsRef<[u8]>>(
		&self,
		key: T,
		value: U,
	) -> Result<Option<Value>> {
		self.0.insert(self.1, key.as_ref(), value.as_ref())
	}
	/// Returns the old value if there was one
	#[inline]
	pub fn remove<T: AsRef<[u8]>>(&self, key: T) -> Result<Option<Value>> {
		self.0.remove(self.1, key.as_ref())
	}

	#[inline]
	pub fn iter(&self) -> Result<ValueIter<'_>> {
		self.0.iter(self.1)
	}
	#[inline]
	pub fn iter_rev(&self) -> Result<ValueIter<'_>> {
		self.0.iter_rev(self.1)
	}

	#[inline]
	pub fn range<K, R>(&self, range: R) -> Result<ValueIter<'_>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.0.range(self.1, get_bound(sb), get_bound(eb))
	}
	#[inline]
	pub fn range_rev<K, R>(&self, range: R) -> Result<ValueIter<'_>>
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
		self.0.get(tree.1, key.as_ref())
	}
	#[inline]
	pub fn len(&self, tree: &Tree) -> TxOpResult<usize> {
		self.0.len(tree.1)
	}

	/// Returns the old value if there was one
	#[inline]
	pub fn insert<T: AsRef<[u8]>, U: AsRef<[u8]>>(
		&mut self,
		tree: &Tree,
		key: T,
		value: U,
	) -> TxOpResult<Option<Value>> {
		self.0.insert(tree.1, key.as_ref(), value.as_ref())
	}
	/// Returns the old value if there was one
	#[inline]
	pub fn remove<T: AsRef<[u8]>>(&mut self, tree: &Tree, key: T) -> TxOpResult<Option<Value>> {
		self.0.remove(tree.1, key.as_ref())
	}

	#[inline]
	pub fn iter(&self, tree: &Tree) -> TxOpResult<TxValueIter<'_>> {
		self.0.iter(tree.1)
	}
	#[inline]
	pub fn iter_rev(&self, tree: &Tree) -> TxOpResult<TxValueIter<'_>> {
		self.0.iter_rev(tree.1)
	}

	#[inline]
	pub fn range<K, R>(&self, tree: &Tree, range: R) -> TxOpResult<TxValueIter<'_>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.0.range(tree.1, get_bound(sb), get_bound(eb))
	}
	#[inline]
	pub fn range_rev<K, R>(&self, tree: &Tree, range: R) -> TxOpResult<TxValueIter<'_>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.0.range_rev(tree.1, get_bound(sb), get_bound(eb))
	}

	// ----

	#[inline]
	pub fn abort<R, E>(self, e: E) -> TxResult<R, E> {
		Err(TxError::Abort(e))
	}

	#[inline]
	pub fn commit<R, E>(self, r: R) -> TxResult<R, E> {
		Ok(r)
	}
}

// ---- Internal interfaces

pub(crate) trait IDb: Send + Sync {
	fn engine(&self) -> String;
	fn open_tree(&self, name: &str) -> Result<usize>;
	fn list_trees(&self) -> Result<Vec<String>>;

	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value>>;
	fn len(&self, tree: usize) -> Result<usize>;

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<Option<Value>>;
	fn remove(&self, tree: usize, key: &[u8]) -> Result<Option<Value>>;

	fn iter(&self, tree: usize) -> Result<ValueIter<'_>>;
	fn iter_rev(&self, tree: usize) -> Result<ValueIter<'_>>;

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>>;
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>>;

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<(), ()>;
}

pub(crate) trait ITx {
	fn get(&self, tree: usize, key: &[u8]) -> TxOpResult<Option<Value>>;
	fn len(&self, tree: usize) -> TxOpResult<usize>;

	fn insert(&mut self, tree: usize, key: &[u8], value: &[u8]) -> TxOpResult<Option<Value>>;
	fn remove(&mut self, tree: usize, key: &[u8]) -> TxOpResult<Option<Value>>;

	fn iter(&self, tree: usize) -> TxOpResult<TxValueIter<'_>>;
	fn iter_rev(&self, tree: usize) -> TxOpResult<TxValueIter<'_>>;

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> TxOpResult<TxValueIter<'_>>;
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> TxOpResult<TxValueIter<'_>>;
}

pub(crate) trait ITxFn {
	fn try_on(&self, tx: &mut dyn ITx) -> TxFnResult;
}

pub(crate) enum TxFnResult {
	Ok,
	Abort,
	DbErr,
}

struct TxFn<F, R, E>
where
	F: Fn(Transaction<'_>) -> TxResult<R, E>,
{
	function: F,
	result: Cell<Option<TxResult<R, E>>>,
}

impl<F, R, E> ITxFn for TxFn<F, R, E>
where
	F: Fn(Transaction<'_>) -> TxResult<R, E>,
{
	fn try_on(&self, tx: &mut dyn ITx) -> TxFnResult {
		let res = (self.function)(Transaction(tx));
		let res2 = match &res {
			Ok(_) => TxFnResult::Ok,
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
