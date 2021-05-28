pub enum ReplicationMode {
	None,
	TwoWay,
	ThreeWay,
}

impl ReplicationMode {
	pub fn parse(v: &str) -> Option<Self> {
		match v {
			"none" | "1" => Some(Self::None),
			"2" => Some(Self::TwoWay),
			"3" => Some(Self::ThreeWay),
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
			Self::TwoWay => 2,
			Self::ThreeWay => 3,
		}
	}

	pub fn read_quorum(&self) -> usize {
		match self {
			Self::None => 1,
			Self::TwoWay => 1,
			Self::ThreeWay => 2,
		}
	}

	pub fn write_quorum(&self) -> usize {
		match self {
			Self::None => 1,
			Self::TwoWay => 2,
			Self::ThreeWay => 2,
		}
	}
}
