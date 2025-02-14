use std::sync::Arc;

use garage_rpc::layout::*;
use garage_rpc::system::System;
use garage_util::data::*;

use crate::replication::*;

// TODO: find a way to track layout changes for this as well
// The hard thing is that this data is stored also on gateway nodes,
// whereas sharded data is stored only on non-Gateway nodes (storage nodes)
// Also we want to be more tolerant to failures of gateways so we don't
// want to do too much holding back of data when progress of gateway
// nodes is not reported in the layout history's ack/sync/sync_ack maps.

/// Full replication schema: all nodes store everything
/// Advantage: do all reads locally, extremely fast
/// Inconvenient: only suitable to reasonably small tables
/// Inconvenient: if some writes fail, nodes will read outdated data
#[derive(Clone)]
pub struct TableFullReplication {
	/// The membership manager of this node
	pub system: Arc<System>,
}

impl TableReplication for TableFullReplication {
	type WriteSets = Vec<Vec<Uuid>>;

	fn storage_nodes(&self, _hash: &Hash) -> Vec<Uuid> {
		let layout = self.system.cluster_layout();
		layout.current().all_nodes().to_vec()
	}

	fn read_nodes(&self, _hash: &Hash) -> Vec<Uuid> {
		vec![self.system.id]
	}
	fn read_quorum(&self) -> usize {
		1
	}

	fn write_sets(&self, hash: &Hash) -> Self::WriteSets {
		vec![self.storage_nodes(hash)]
	}
	fn write_quorum(&self) -> usize {
		let nmembers = self.system.cluster_layout().current().all_nodes().len();
		if nmembers < 3 {
			1
		} else {
			nmembers.div_euclid(2) + 1
		}
	}

	fn partition_of(&self, _hash: &Hash) -> Partition {
		0u16
	}

	fn sync_partitions(&self) -> SyncPartitions {
		let layout = self.system.cluster_layout();
		let layout_version = layout.current().version;
		SyncPartitions {
			layout_version,
			partitions: vec![SyncPartition {
				partition: 0u16,
				first_hash: [0u8; 32].into(),
				last_hash: [0xff; 32].into(),
				storage_sets: vec![layout.current().all_nodes().to_vec()],
			}],
		}
	}
}
