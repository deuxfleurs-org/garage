use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use garage_db as db;

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::Error;

use garage_rpc::ring::*;

use crate::data::*;
use crate::replication::*;
use crate::schema::*;

// This modules partitions the data in 2**16 partitions, based on the top
// 16 bits (two bytes) of item's partition keys' hashes.
// It builds one Merkle tree for each of these 2**16 partitions.

pub struct MerkleUpdater<F: TableSchema, R: TableReplication> {
	data: Arc<TableData<F, R>>,

	// Content of the todo tree: items where
	// - key = the key of an item in the main table, ie hash(partition_key)+sort_key
	// - value = the hash of the full serialized item, if present,
	//			 or an empty vec if item is absent (deleted)
	// Fields in data:
	//		pub(crate) merkle_todo: sled::Tree,
	//		pub(crate) merkle_todo_notify: Notify,

	// Content of the merkle tree: items where
	// - key = .bytes() for MerkleNodeKey
	// - value = serialization of a MerkleNode, assumed to be MerkleNode::empty if not found
	// Field in data:
	//		pub(crate) merkle_tree: sled::Tree,
	empty_node_hash: Hash,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MerkleNodeKey {
	// partition number
	pub partition: Partition,

	// prefix: a prefix for the hash of full keys, i.e. hash(hash(partition_key)+sort_key)
	#[serde(with = "serde_bytes")]
	pub prefix: Vec<u8>,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum MerkleNode {
	// The empty Merkle node
	Empty,

	// An intermediate Merkle tree node for a prefix
	// Contains the hashes of the 256 possible next prefixes
	Intermediate(Vec<(u8, Hash)>),

	// A final node for an item
	// Contains the full key of the item and the hash of the value
	Leaf(Vec<u8>, Hash),
}

impl<F, R> MerkleUpdater<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	pub(crate) fn launch(background: &BackgroundRunner, data: Arc<TableData<F, R>>) -> Arc<Self> {
		let empty_node_hash = blake2sum(&rmp_to_vec_all_named(&MerkleNode::Empty).unwrap()[..]);

		let ret = Arc::new(Self {
			data,
			empty_node_hash,
		});

		background.spawn_worker(MerkleWorker(ret.clone()));

		ret
	}

	fn updater_loop_iter(&self) -> Result<WorkerState, Error> {
		if let Some((key, valhash)) = self.data.merkle_todo.first()? {
			self.update_item(&key, &valhash)?;
			Ok(WorkerState::Busy)
		} else {
			Ok(WorkerState::Idle)
		}
	}

	fn update_item(&self, k: &[u8], vhash_by: &[u8]) -> Result<(), Error> {
		let khash = blake2sum(k);

		let new_vhash = if vhash_by.is_empty() {
			None
		} else {
			Some(Hash::try_from(vhash_by).unwrap())
		};

		let key = MerkleNodeKey {
			partition: self
				.data
				.replication
				.partition_of(&Hash::try_from(&k[0..32]).unwrap()),
			prefix: vec![],
		};
		self.data
			.merkle_tree
			.db()
			.transaction(|mut tx| self.update_item_rec(&mut tx, k, &khash, &key, new_vhash))?;

		let deleted = self.data.merkle_todo.db().transaction(|mut tx| {
			let remove = matches!(tx.get(&self.data.merkle_todo, k)?, Some(ov) if ov == vhash_by);
			if remove {
				tx.remove(&self.data.merkle_todo, k)?;
			}
			Ok(remove)
		})?;

		if !deleted {
			debug!(
				"({}) Item not deleted from Merkle todo because it changed: {:?}",
				F::TABLE_NAME,
				k
			);
		}
		Ok(())
	}

	fn update_item_rec(
		&self,
		tx: &mut db::Transaction<'_>,
		k: &[u8],
		khash: &Hash,
		key: &MerkleNodeKey,
		new_vhash: Option<Hash>,
	) -> db::TxResult<Option<Hash>, Error> {
		let i = key.prefix.len();

		// Read node at current position (defined by the prefix stored in key)
		// Calculate an update to apply to this node
		// This update is an Option<_>, so that it is None if the update is a no-op
		// and we can thus skip recalculating and re-storing everything
		let mutate = match self.read_node_txn(tx, key)? {
			MerkleNode::Empty => new_vhash.map(|vhv| MerkleNode::Leaf(k.to_vec(), vhv)),
			MerkleNode::Intermediate(mut children) => {
				let key2 = key.next_key(khash);
				if let Some(subhash) = self.update_item_rec(tx, k, khash, &key2, new_vhash)? {
					// Subtree changed, update this node as well
					if subhash == self.empty_node_hash {
						intermediate_rm_child(&mut children, key2.prefix[i]);
					} else {
						intermediate_set_child(&mut children, key2.prefix[i], subhash);
					}

					if children.is_empty() {
						// should not happen
						warn!(
							"({}) Replacing intermediate node with empty node, should not happen.",
							F::TABLE_NAME
						);
						Some(MerkleNode::Empty)
					} else if children.len() == 1 {
						// We now have a single node (case when the update deleted one of only two
						// children). If that node is a leaf, move it to this level.
						let key_sub = key.add_byte(children[0].0);
						let subnode = self.read_node_txn(tx, &key_sub)?;
						match subnode {
							MerkleNode::Empty => {
								warn!(
									"({}) Single subnode in tree is empty Merkle node",
									F::TABLE_NAME
								);
								Some(MerkleNode::Empty)
							}
							MerkleNode::Intermediate(_) => Some(MerkleNode::Intermediate(children)),
							x @ MerkleNode::Leaf(_, _) => {
								tx.remove(&self.data.merkle_tree, key_sub.encode())?;
								Some(x)
							}
						}
					} else {
						Some(MerkleNode::Intermediate(children))
					}
				} else {
					// Subtree not changed, nothing to do
					None
				}
			}
			MerkleNode::Leaf(exlf_k, exlf_vhash) => {
				if exlf_k == k {
					// This leaf is for the same key that the one we are updating
					match new_vhash {
						Some(vhv) if vhv == exlf_vhash => None,
						Some(vhv) => Some(MerkleNode::Leaf(k.to_vec(), vhv)),
						None => Some(MerkleNode::Empty),
					}
				} else {
					// This is an only leaf for another key
					if new_vhash.is_some() {
						// Move that other key to a subnode, create another subnode for our
						// insertion and replace current node by an intermediary node
						let mut int = vec![];

						let exlf_khash = blake2sum(&exlf_k[..]);
						assert_eq!(khash.as_slice()[..i], exlf_khash.as_slice()[..i]);

						{
							let exlf_subkey = key.next_key(&exlf_khash);
							let exlf_sub_hash = self
								.update_item_rec(
									tx,
									&exlf_k[..],
									&exlf_khash,
									&exlf_subkey,
									Some(exlf_vhash),
								)?
								.unwrap();
							intermediate_set_child(&mut int, exlf_subkey.prefix[i], exlf_sub_hash);
							assert_eq!(int.len(), 1);
						}

						{
							let key2 = key.next_key(khash);
							let subhash = self
								.update_item_rec(tx, k, khash, &key2, new_vhash)?
								.unwrap();
							intermediate_set_child(&mut int, key2.prefix[i], subhash);
							if exlf_khash.as_slice()[i] == khash.as_slice()[i] {
								assert_eq!(int.len(), 1);
							} else {
								assert_eq!(int.len(), 2);
							}
						}
						Some(MerkleNode::Intermediate(int))
					} else {
						// Nothing to do, we don't want to insert this value because it is None,
						// and we don't want to change the other value because it's for something
						// else
						None
					}
				}
			}
		};

		if let Some(new_node) = mutate {
			let hash = self.put_node_txn(tx, key, &new_node)?;
			Ok(Some(hash))
		} else {
			Ok(None)
		}
	}

	// Merkle tree node manipulation

	fn read_node_txn(
		&self,
		tx: &mut db::Transaction<'_>,
		k: &MerkleNodeKey,
	) -> db::TxResult<MerkleNode, Error> {
		let ent = tx.get(&self.data.merkle_tree, k.encode())?;
		MerkleNode::decode_opt(&ent).map_err(db::TxError::Abort)
	}

	fn put_node_txn(
		&self,
		tx: &mut db::Transaction<'_>,
		k: &MerkleNodeKey,
		v: &MerkleNode,
	) -> db::TxResult<Hash, Error> {
		trace!("Put Merkle node: {:?} => {:?}", k, v);
		if *v == MerkleNode::Empty {
			tx.remove(&self.data.merkle_tree, k.encode())?;
			Ok(self.empty_node_hash)
		} else {
			let vby = rmp_to_vec_all_named(v).map_err(|e| db::TxError::Abort(e.into()))?;
			let rethash = blake2sum(&vby[..]);
			tx.insert(&self.data.merkle_tree, k.encode(), vby)?;
			Ok(rethash)
		}
	}

	// Access a node in the Merkle tree, used by the sync protocol
	pub(crate) fn read_node(&self, k: &MerkleNodeKey) -> Result<MerkleNode, Error> {
		let ent = self.data.merkle_tree.get(k.encode())?;
		MerkleNode::decode_opt(&ent)
	}

	pub fn merkle_tree_len(&self) -> Result<usize, Error> {
		Ok(self.data.merkle_tree.len()?)
	}

	pub fn todo_len(&self) -> Result<usize, Error> {
		Ok(self.data.merkle_todo.len()?)
	}
}

struct MerkleWorker<F, R>(Arc<MerkleUpdater<F, R>>)
where
	F: TableSchema + 'static,
	R: TableReplication + 'static;

#[async_trait]
impl<F, R> Worker for MerkleWorker<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	fn name(&self) -> String {
		format!("{} Merkle tree updater", F::TABLE_NAME)
	}

	fn info(&self) -> Option<String> {
		let l = self.0.todo_len().unwrap_or(0);
		if l > 0 {
			Some(format!("{} items in queue", l))
		} else {
			None
		}
	}

	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		let updater = self.0.clone();
		tokio::task::spawn_blocking(move || {
			for _i in 0..100 {
				let s = updater.updater_loop_iter();
				if !matches!(s, Ok(WorkerState::Busy)) {
					return s;
				}
			}
			Ok(WorkerState::Busy)
		})
		.await
		.unwrap()
	}

	async fn wait_for_work(&mut self, must_exit: &watch::Receiver<bool>) -> WorkerState {
		if *must_exit.borrow() {
			return WorkerState::Done;
		}
		tokio::time::sleep(Duration::from_secs(10)).await;
		WorkerState::Busy
	}
}

impl MerkleNodeKey {
	fn encode(&self) -> Vec<u8> {
		let mut ret = Vec::with_capacity(2 + self.prefix.len());
		ret.extend(&u16::to_be_bytes(self.partition)[..]);
		ret.extend(&self.prefix[..]);
		ret
	}

	pub fn next_key(&self, h: &Hash) -> Self {
		assert_eq!(h.as_slice()[0..self.prefix.len()], self.prefix[..]);
		let mut s2 = self.clone();
		s2.prefix.push(h.as_slice()[self.prefix.len()]);
		s2
	}

	pub fn add_byte(&self, b: u8) -> Self {
		let mut s2 = self.clone();
		s2.prefix.push(b);
		s2
	}
}

impl MerkleNode {
	fn decode_opt(ent: &Option<db::Value>) -> Result<Self, Error> {
		match ent {
			None => Ok(MerkleNode::Empty),
			Some(v) => Ok(rmp_serde::decode::from_read_ref::<_, MerkleNode>(&v[..])?),
		}
	}

	pub fn is_empty(&self) -> bool {
		*self == MerkleNode::Empty
	}
}

fn intermediate_set_child(ch: &mut Vec<(u8, Hash)>, pos: u8, v: Hash) {
	for i in 0..ch.len() {
		if ch[i].0 == pos {
			ch[i].1 = v;
			return;
		} else if ch[i].0 > pos {
			ch.insert(i, (pos, v));
			return;
		}
	}
	ch.push((pos, v));
}

fn intermediate_rm_child(ch: &mut Vec<(u8, Hash)>, pos: u8) {
	for i in 0..ch.len() {
		if ch[i].0 == pos {
			ch.remove(i);
			return;
		}
	}
}

#[test]
fn test_intermediate_aux() {
	let mut v = vec![];

	intermediate_set_child(&mut v, 12u8, [12u8; 32].into());
	assert_eq!(v, vec![(12u8, [12u8; 32].into())]);

	intermediate_set_child(&mut v, 42u8, [42u8; 32].into());
	assert_eq!(
		v,
		vec![(12u8, [12u8; 32].into()), (42u8, [42u8; 32].into())]
	);

	intermediate_set_child(&mut v, 4u8, [4u8; 32].into());
	assert_eq!(
		v,
		vec![
			(4u8, [4u8; 32].into()),
			(12u8, [12u8; 32].into()),
			(42u8, [42u8; 32].into())
		]
	);

	intermediate_set_child(&mut v, 12u8, [8u8; 32].into());
	assert_eq!(
		v,
		vec![
			(4u8, [4u8; 32].into()),
			(12u8, [8u8; 32].into()),
			(42u8, [42u8; 32].into())
		]
	);

	intermediate_set_child(&mut v, 6u8, [6u8; 32].into());
	assert_eq!(
		v,
		vec![
			(4u8, [4u8; 32].into()),
			(6u8, [6u8; 32].into()),
			(12u8, [8u8; 32].into()),
			(42u8, [42u8; 32].into())
		]
	);

	intermediate_rm_child(&mut v, 42u8);
	assert_eq!(
		v,
		vec![
			(4u8, [4u8; 32].into()),
			(6u8, [6u8; 32].into()),
			(12u8, [8u8; 32].into())
		]
	);

	intermediate_rm_child(&mut v, 11u8);
	assert_eq!(
		v,
		vec![
			(4u8, [4u8; 32].into()),
			(6u8, [6u8; 32].into()),
			(12u8, [8u8; 32].into())
		]
	);

	intermediate_rm_child(&mut v, 6u8);
	assert_eq!(v, vec![(4u8, [4u8; 32].into()), (12u8, [8u8; 32].into())]);

	intermediate_set_child(&mut v, 6u8, [7u8; 32].into());
	assert_eq!(
		v,
		vec![
			(4u8, [4u8; 32].into()),
			(6u8, [7u8; 32].into()),
			(12u8, [8u8; 32].into())
		]
	);
}
