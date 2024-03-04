use garage_util::config::Config;
use garage_util::crdt::AutoCrdt;
use garage_util::error::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReplicationFactor(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConsistencyMode {
	/// Read- and Write-quorum are 1
	Dangerous,
	/// Read-quorum is 1
	Degraded,
	/// Read- and Write-quorum are determined for read-after-write-consistency
	#[default]
	Consistent,
}

impl ConsistencyMode {
	pub fn parse(s: &str) -> Option<Self> {
		serde_json::from_value(serde_json::Value::String(s.to_string())).ok()
	}
}

impl AutoCrdt for ConsistencyMode {
	const WARN_IF_DIFFERENT: bool = true;
}

impl ReplicationFactor {
	pub fn new(replication_factor: usize) -> Option<Self> {
		if replication_factor < 1 {
			None
		} else {
			Some(Self(replication_factor))
		}
	}

	pub fn replication_factor(&self) -> usize {
		self.0
	}

	pub fn read_quorum(&self, consistency_mode: ConsistencyMode) -> usize {
		match consistency_mode {
			ConsistencyMode::Dangerous | ConsistencyMode::Degraded => 1,
			ConsistencyMode::Consistent => self.replication_factor().div_ceil(2),
		}
	}

	pub fn write_quorum(&self, consistency_mode: ConsistencyMode) -> usize {
		match consistency_mode {
			ConsistencyMode::Dangerous => 1,
			ConsistencyMode::Degraded | ConsistencyMode::Consistent => {
				(self.replication_factor() + 1) - self.read_quorum(ConsistencyMode::Consistent)
			}
		}
	}
}

impl std::convert::From<ReplicationFactor> for usize {
	fn from(replication_factor: ReplicationFactor) -> usize {
		replication_factor.0
	}
}

pub fn parse_replication_mode(
	config: &Config,
) -> Result<(ReplicationFactor, ConsistencyMode), Error> {
	match (&config.replication_mode, config.replication_factor, config.consistency_mode.as_str()) {
        (Some(replication_mode), None, "consistent") => {
            tracing::warn!("Legacy config option replication_mode in use. Please migrate to replication_factor and consistency_mode");
            let parsed_replication_mode = match replication_mode.as_str() {
                "1" | "none"  => Some((ReplicationFactor(1), ConsistencyMode::Consistent)),
                "2"           => Some((ReplicationFactor(2), ConsistencyMode::Consistent)),
                "2-dangerous" => Some((ReplicationFactor(2), ConsistencyMode::Dangerous)),
                "3"           => Some((ReplicationFactor(3), ConsistencyMode::Consistent)),
                "3-degraded"  => Some((ReplicationFactor(3), ConsistencyMode::Degraded)),
                "3-dangerous" => Some((ReplicationFactor(3), ConsistencyMode::Dangerous)),
                _ => None,
            };
            Some(parsed_replication_mode.ok_or_message("Invalid replication_mode in config file.")?)
        },
        (None, Some(replication_factor), consistency_mode) => {
            let replication_factor = ReplicationFactor::new(replication_factor)
                .ok_or_message("Invalid replication_factor in config file.")?;
            let consistency_mode = ConsistencyMode::parse(consistency_mode)
                .ok_or_message("Invalid consistency_mode in config file.")?;
            Some((replication_factor, consistency_mode))
        }
        _ => None,
    }.ok_or_message("Either the legacy replication_mode or replication_level and consistency_mode can be set, not both.")
}
