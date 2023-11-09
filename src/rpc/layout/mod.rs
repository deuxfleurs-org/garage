mod graph_algo;
mod history;
mod schema;
mod version;

pub mod manager;

// ---- re-exports ----

pub use history::*;
pub use schema::*;
pub use version::*;

// ---- defines: partitions ----

/// A partition id, which is stored on 16 bits
/// i.e. we have up to 2**16 partitions.
/// (in practice we have exactly 2**PARTITION_BITS partitions)
pub type Partition = u16;

// TODO: make this constant parametrizable in the config file
// For deployments with many nodes it might make sense to bump
// it up to 10.
// Maximum value : 16
/// How many bits from the hash are used to make partitions. Higher numbers means more fairness in
/// presence of numerous nodes, but exponentially bigger ring. Max 16
pub const PARTITION_BITS: usize = 8;

const NB_PARTITIONS: usize = 1usize << PARTITION_BITS;

// ---- defines: nodes ----

// Type to store compactly the id of a node in the system
// Change this to u16 the day we want to have more than 256 nodes in a cluster
pub type CompactNodeType = u8;
pub const MAX_NODE_NUMBER: usize = 256;
