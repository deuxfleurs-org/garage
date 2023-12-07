#[derive(Clone, Copy)]
pub enum ReplicationMode {
	None,
	TwoWay,
	TwoWayDangerous,
	ThreeWay,
	ThreeWayDegraded,
	ThreeWayDangerous,
}

impl ReplicationMode {
	pub fn parse(v: &str) -> Option<Self> {
		match v {
			"none" | "1" => Some(Self::None),
			"2" => Some(Self::TwoWay),
			"2-dangerous" => Some(Self::TwoWayDangerous),
			"3" => Some(Self::ThreeWay),
			"3-degraded" => Some(Self::ThreeWayDegraded),
			"3-dangerous" => Some(Self::ThreeWayDangerous),
			_ => None,
		}
	}

	pub fn control_write_max_faults(&self) -> usize {
		match self {
			Self::None => 0,
			_ => 1,
		}
	}

	pub fn replication_factor(&self) -> usize {
		match self {
			Self::None => 1,
			Self::TwoWay | Self::TwoWayDangerous => 2,
			Self::ThreeWay | Self::ThreeWayDegraded | Self::ThreeWayDangerous => 3,
		}
	}

	pub fn read_quorum(&self) -> usize {
		match self {
			Self::None => 1,
			Self::TwoWay | Self::TwoWayDangerous => 1,
			Self::ThreeWay => 2,
			Self::ThreeWayDegraded | Self::ThreeWayDangerous => 1,
		}
	}

	pub fn write_quorum(&self) -> usize {
		match self {
			Self::None => 1,
			Self::TwoWay => 2,
			Self::TwoWayDangerous => 1,
			Self::ThreeWay | Self::ThreeWayDegraded => 2,
			Self::ThreeWayDangerous => 1,
		}
	}

	pub fn is_read_after_write_consistent(&self) -> bool {
		match self {
			Self::None | Self::TwoWay | Self::ThreeWay => true,
			_ => false,
		}
	}
}
