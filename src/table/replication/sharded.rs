use std::sync::Arc;
use std::time::Duration;

use garage_rpc::layout::*;
use garage_rpc::replication_mode::ConsistencyMode;
use garage_util::data::*;

use crate::replication::sharded::manager::LayoutManager;
use crate::replication::*;

/// Sharded replication schema:
/// - based on the ring of nodes, a certain set of neighbors
///   store entries, given as a function of the position of the
///   entry's hash in the ring
/// - reads are done on all of the nodes that replicate the data
/// - writes as well
#[derive(Clone)]
pub struct TableShardedReplication {
	/// The membership manager of this node
	pub layout_manager: Arc<LayoutManager>,
	pub consistency_mode: ConsistencyMode,
}

impl TableReplication for TableShardedReplication {
	// Do anti-entropy every 10 minutes
	const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(10 * 60);

	type WriteSets = WriteLock<Vec<Vec<Uuid>>>;

	fn storage_nodes(&self, hash: &Hash) -> Vec<Uuid> {
		let mut ret = vec![];
		for version in self.layout_manager.layout().versions().iter() {
			ret.extend(version.nodes_of(hash));
		}
		ret.sort();
		ret.dedup();
		ret
	}

	fn read_nodes(&self, hash: &Hash) -> Vec<Uuid> {
		self.layout_manager
			.layout()
			.read_version()
			.nodes_of(hash)
			.collect()
	}

	fn read_quorum(&self) -> usize {
		self.layout_manager
			.layout()
			.read_version()
			.read_quorum(self.consistency_mode)
	}

	fn write_sets(&self, hash: &Hash) -> Self::WriteSets {
		self.layout_manager.write_lock_with(|l| write_sets(l, hash))
	}

	fn write_quorum(&self) -> usize {
		self.layout_manager
			.layout()
			.current()
			.write_quorum(self.consistency_mode)
	}

	fn partition_of(&self, hash: &Hash) -> Partition {
		self.layout_manager.layout().current().partition_of(hash)
	}

	fn sync_partitions(&self) -> SyncPartitions {
		let layout = self.layout_manager.layout();
		let layout_version = layout.ack_map_min();

		let mut partitions = layout
			.current()
			.partitions()
			.map(|(partition, first_hash)| {
				SyncPartition {
					partition,
					first_hash,
					last_hash: [0u8; 32].into(), // filled in just after
					storage_sets: write_sets(&layout, &first_hash),
				}
			})
			.collect::<Vec<_>>();

		for i in 0..partitions.len() {
			partitions[i].last_hash = if i + 1 < partitions.len() {
				partitions[i + 1].first_hash
			} else {
				[0xFFu8; 32].into()
			};
		}

		SyncPartitions {
			layout_version,
			partitions,
		}
	}
}

fn write_sets(layout: &LayoutHelper, hash: &Hash) -> Vec<Vec<Uuid>> {
	layout
		.versions()
		.iter()
		.map(|x| x.nodes_of(hash).collect())
		.collect()
}
