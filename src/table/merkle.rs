use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;

use futures::select;
use futures_util::future::*;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use sled::transaction::{
	ConflictableTransactionError, ConflictableTransactionResult, TransactionalTree,
};
use tokio::sync::{watch, Notify};

use garage_util::background::BackgroundRunner;
use garage_util::data::*;
use garage_util::error::Error;

// This modules partitions the data in 2**16 partitions, based on the top
// 16 bits (two bytes) of item's partition keys' hashes.
// It builds one Merkle tree for each of these 2**16 partitions.

pub(crate) struct MerkleUpdater {
	table_name: String,
	background: Arc<BackgroundRunner>,

	// Content of the todo tree: items where
	// - key = the key of an item in the main table, ie hash(partition_key)+sort_key
	// - value = the hash of the full serialized item, if present,
	//			 or an empty vec if item is absent (deleted)
	pub(crate) todo: sled::Tree,
	pub(crate) todo_notify: Notify,

	// Content of the merkle tree: items where
	// - key = .bytes() for MerkleNodeKey
	// - value = serialization of a MerkleNode, assumed to be MerkleNode::empty if not found
	pub(crate) merkle_tree: sled::Tree,
	empty_node_hash: Hash,
}

#[derive(Clone)]
pub struct MerkleNodeKey {
	// partition: first 16 bits (two bytes) of the partition_key's hash
	pub partition: [u8; 2],

	// prefix: a prefix for the hash of full keys, i.e. hash(hash(partition_key)+sort_key)
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

impl MerkleUpdater {
	pub(crate) fn new(
		table_name: String,
		background: Arc<BackgroundRunner>,
		todo: sled::Tree,
		merkle_tree: sled::Tree,
	) -> Arc<Self> {
		let empty_node_hash = blake2sum(&rmp_to_vec_all_named(&MerkleNode::Empty).unwrap()[..]);

		Arc::new(Self {
			table_name,
			background,
			todo,
			todo_notify: Notify::new(),
			merkle_tree,
			empty_node_hash,
		})
	}

	pub(crate) fn launch(self: &Arc<Self>) {
		let self2 = self.clone();
		self.background.spawn_worker(
			format!("Merkle tree updater for {}", self.table_name),
			|must_exit: watch::Receiver<bool>| self2.updater_loop(must_exit),
		);
	}

	async fn updater_loop(
		self: Arc<Self>,
		mut must_exit: watch::Receiver<bool>,
	) -> Result<(), Error> {
		while !*must_exit.borrow() {
			if let Some(x) = self.todo.iter().next() {
				match x {
					Ok((key, valhash)) => {
						if let Err(e) = self.update_item(&key[..], &valhash[..]) {
							warn!("Error while updating Merkle tree item: {}", e);
						}
					}
					Err(e) => {
						warn!("Error while iterating on Merkle todo tree: {}", e);
						tokio::time::delay_for(Duration::from_secs(10)).await;
					}
				}
			} else {
				select! {
					_ = self.todo_notify.notified().fuse() => (),
					_ = must_exit.recv().fuse() => (),
				}
			}
		}
		Ok(())
	}

	fn update_item(&self, k: &[u8], vhash_by: &[u8]) -> Result<(), Error> {
		let khash = blake2sum(k);

		let new_vhash = if vhash_by.len() == 0 {
			None
		} else {
			let vhash_by: [u8; 32] = vhash_by
				.try_into()
				.map_err(|_| Error::Message(format!("Invalid value in Merkle todo table")))?;
			Some(Hash::from(vhash_by))
		};

		let key = MerkleNodeKey {
			partition: k[0..2].try_into().unwrap(),
			prefix: vec![],
		};
		self.merkle_tree
			.transaction(|tx| self.update_item_rec(tx, k, khash, &key, new_vhash))?;

		let deleted = self
			.todo
			.compare_and_swap::<_, _, Vec<u8>>(k, Some(vhash_by), None)?
			.is_ok();

		if !deleted {
			info!(
				"Item not deleted from Merkle todo because it changed: {:?}",
				k
			);
		}
		Ok(())
	}

	fn update_item_rec(
		&self,
		tx: &TransactionalTree,
		k: &[u8],
		khash: Hash,
		key: &MerkleNodeKey,
		new_vhash: Option<Hash>,
	) -> ConflictableTransactionResult<Option<Hash>, Error> {
		let i = key.prefix.len();
		let mutate = match self.read_node_txn(tx, &key)? {
			MerkleNode::Empty => {
				if let Some(vhv) = new_vhash {
					Some(MerkleNode::Leaf(k.to_vec(), vhv))
				} else {
					None
				}
			}
			MerkleNode::Intermediate(mut children) => {
				let key2 = key.next_key(khash);
				if let Some(subhash) = self.update_item_rec(tx, k, khash, &key2, new_vhash)? {
					if subhash == self.empty_node_hash {
						intermediate_rm_child(&mut children, key2.prefix[i]);
					} else {
						intermediate_set_child(&mut children, key2.prefix[i], subhash);
					}
					if children.len() == 0 {
						// should not happen
						warn!("Replacing intermediate node with empty node, should not happen.");
						Some(MerkleNode::Empty)
					} else if children.len() == 1 {
						// move node down to this level
						let key_sub = key.add_byte(children[0].0);
						let subnode = self.read_node_txn(tx, &key_sub)?;
						tx.remove(key_sub.encode())?;
						Some(subnode)
					} else {
						Some(MerkleNode::Intermediate(children))
					}
				} else {
					None
				}
			}
			MerkleNode::Leaf(exlf_key, exlf_hash) => {
				if exlf_key == k {
					match new_vhash {
						Some(vhv) if vhv == exlf_hash => None,
						Some(vhv) => Some(MerkleNode::Leaf(k.to_vec(), vhv)),
						None => Some(MerkleNode::Empty),
					}
				} else {
					if let Some(vhv) = new_vhash {
						// Create two sub-nodes and replace by intermediary node
						let (pos1, h1) = {
							let key2 = key.next_key(blake2sum(&exlf_key[..]));
							let subhash =
								self.put_node_txn(tx, &key2, &MerkleNode::Leaf(exlf_key, exlf_hash))?;
							(key2.prefix[i], subhash)
						};
						let (pos2, h2) = {
							let key2 = key.next_key(khash);
							let subhash =
								self.put_node_txn(tx, &key2, &MerkleNode::Leaf(k.to_vec(), vhv))?;
							(key2.prefix[i], subhash)
						};
						let mut int = vec![];
						intermediate_set_child(&mut int, pos1, h1);
						intermediate_set_child(&mut int, pos2, h2);
						Some(MerkleNode::Intermediate(int))
					} else {
						None
					}
				}
			}
		};

		if let Some(new_node) = mutate {
			let hash = self.put_node_txn(tx, &key, &new_node)?;
			Ok(Some(hash))
		} else {
			Ok(None)
		}
	}

	// Merkle tree node manipulation

	fn read_node_txn(
		&self,
		tx: &TransactionalTree,
		k: &MerkleNodeKey,
	) -> ConflictableTransactionResult<MerkleNode, Error> {
		let ent = tx.get(k.encode())?;
		match ent {
			None => Ok(MerkleNode::Empty),
			Some(v) => Ok(rmp_serde::decode::from_read_ref::<_, MerkleNode>(&v[..])
				.map_err(|e| ConflictableTransactionError::Abort(e.into()))?),
		}
	}

	fn put_node_txn(
		&self,
		tx: &TransactionalTree,
		k: &MerkleNodeKey,
		v: &MerkleNode,
	) -> ConflictableTransactionResult<Hash, Error> {
		if *v == MerkleNode::Empty {
			tx.remove(k.encode())?;
			Ok(self.empty_node_hash)
		} else {
			let vby = rmp_to_vec_all_named(v)
				.map_err(|e| ConflictableTransactionError::Abort(e.into()))?;
			let rethash = blake2sum(&vby[..]);
			tx.insert(k.encode(), vby)?;
			Ok(rethash)
		}
	}

	pub(crate) fn read_node(
		&self,
		k: &MerkleNodeKey,
	) -> Result<MerkleNode, Error> {
		let ent = self.merkle_tree.get(k.encode())?;
		match ent {
			None => Ok(MerkleNode::Empty),
			Some(v) => Ok(rmp_serde::decode::from_read_ref::<_, MerkleNode>(&v[..])?)
		}
	}
}

impl MerkleNodeKey {
	fn encode(&self) -> Vec<u8> {
		let mut ret = Vec::with_capacity(2 + self.prefix.len());
		ret.extend(&self.partition[..]);
		ret.extend(&self.prefix[..]);
		ret
	}

	pub fn next_key(&self, h: Hash) -> Self {
		assert!(&h.as_slice()[0..self.prefix.len()] == &self.prefix[..]);
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
	ch.insert(ch.len(), (pos, v));
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
	assert!(v == vec![(12u8, [12u8; 32].into())]);
	
	intermediate_set_child(&mut v, 42u8, [42u8; 32].into());
	assert!(v == vec![(12u8, [12u8; 32].into()), (42u8, [42u8; 32].into())]);
	
	intermediate_set_child(&mut v, 4u8, [4u8; 32].into());
	assert!(v == vec![(4u8, [4u8; 32].into()), (12u8, [12u8; 32].into()), (42u8, [42u8; 32].into())]);
	
	intermediate_set_child(&mut v, 12u8, [8u8; 32].into());
	assert!(v == vec![(4u8, [4u8; 32].into()), (12u8, [8u8; 32].into()), (42u8, [42u8; 32].into())]);
	
	intermediate_set_child(&mut v, 6u8, [6u8; 32].into());
	assert!(v == vec![(4u8, [4u8; 32].into()), (6u8, [6u8; 32].into()), (12u8, [8u8; 32].into()), (42u8, [42u8; 32].into())]);

	intermediate_rm_child(&mut v, 42u8);
	assert!(v == vec![(4u8, [4u8; 32].into()), (6u8, [6u8; 32].into()), (12u8, [8u8; 32].into())]);

	intermediate_rm_child(&mut v, 11u8);
	assert!(v == vec![(4u8, [4u8; 32].into()), (6u8, [6u8; 32].into()), (12u8, [8u8; 32].into())]);

	intermediate_rm_child(&mut v, 6u8);
	assert!(v == vec![(4u8, [4u8; 32].into()), (12u8, [8u8; 32].into())]);
	
	intermediate_set_child(&mut v, 6u8, [7u8; 32].into());
	assert!(v == vec![(4u8, [4u8; 32].into()), (6u8, [7u8; 32].into()), (12u8, [8u8; 32].into())]);
}
