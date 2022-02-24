use std::sync::{
	atomic::{AtomicUsize, Ordering},
	Arc,
};

use sled::{CompareAndSwapError, IVec, Iter, Result, Tree};

#[derive(Clone)]
pub struct SledCountedTree(Arc<SledCountedTreeInternal>);

struct SledCountedTreeInternal {
	tree: Tree,
	len: AtomicUsize,
}

impl SledCountedTree {
	pub fn new(tree: Tree) -> Self {
		let len = tree.len();
		Self(Arc::new(SledCountedTreeInternal {
			tree,
			len: AtomicUsize::new(len),
		}))
	}

	pub fn len(&self) -> usize {
		self.0.len.load(Ordering::Relaxed)
	}

	pub fn is_empty(&self) -> bool {
		self.0.tree.is_empty()
	}

	pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
		self.0.tree.get(key)
	}

	pub fn iter(&self) -> Iter {
		self.0.tree.iter()
	}

	// ---- writing functions ----

	pub fn insert<K, V>(&self, key: K, value: V) -> Result<Option<IVec>>
	where
		K: AsRef<[u8]>,
		V: Into<IVec>,
	{
		let res = self.0.tree.insert(key, value);
		if res == Ok(None) {
			self.0.len.fetch_add(1, Ordering::Relaxed);
		}
		res
	}

	pub fn pop_min(&self) -> Result<Option<(IVec, IVec)>> {
		let res = self.0.tree.pop_min();
		if let Ok(Some(_)) = &res {
			self.0.len.fetch_sub(1, Ordering::Relaxed);
		};
		res
	}

	pub fn compare_and_swap<K, OV, NV>(
		&self,
		key: K,
		old: Option<OV>,
		new: Option<NV>,
	) -> Result<std::result::Result<(), CompareAndSwapError>>
	where
		K: AsRef<[u8]>,
		OV: AsRef<[u8]>,
		NV: Into<IVec>,
	{
		let old_some = old.is_some();
		let new_some = new.is_some();

		let res = self.0.tree.compare_and_swap(key, old, new);

		if res == Ok(Ok(())) {
			match (old_some, new_some) {
				(false, true) => {
					self.0.len.fetch_add(1, Ordering::Relaxed);
				}
				(true, false) => {
					self.0.len.fetch_sub(1, Ordering::Relaxed);
				}
				_ => (),
			}
		}
		res
	}
}
