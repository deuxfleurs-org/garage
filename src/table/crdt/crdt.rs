use garage_util::data::*;

/// Definition of a CRDT - all CRDT Rust types implement this.
///
/// A CRDT is defined as a merge operator that respects a certain set of axioms.
///
/// In particular, the merge operator must be commutative, associative,
/// idempotent, and monotonic.
/// In other words, if `a`, `b` and `c` are CRDTs, and `⊔` denotes the merge operator,
/// the following axioms must apply:
///
/// ```text
/// a ⊔ b = b ⊔ a                   (commutativity)
/// (a ⊔ b) ⊔ c = a ⊔ (b ⊔ c)       (associativity)
/// (a ⊔ b) ⊔ b = a ⊔ b             (idempotence)
/// ```
///
/// Moreover, the relationship `≥` defined by `a ≥ b ⇔ ∃c. a = b ⊔ c` must be a partial order.
/// This implies a few properties such as: if `a ⊔ b ≠ a`, then there is no `c` such that `(a ⊔ b) ⊔ c = a`,
/// as this would imply a cycle in the partial order.
pub trait CRDT {
	/// Merge the two datastructures according to the CRDT rules.
	/// `self` is modified to contain the merged CRDT value. `other` is not modified.
	///
	/// # Arguments
	///
	/// * `other` - the other CRDT we wish to merge with
	fn merge(&mut self, other: &Self);
}

/// All types that implement `Ord` (a total order) can also implement a trivial CRDT
/// defined by the merge rule: `a ⊔ b = max(a, b)`. Implement this trait for your type
/// to enable this behavior.
pub trait AutoCRDT: Ord + Clone + std::fmt::Debug {
	/// WARN_IF_DIFFERENT: emit a warning when values differ. Set this to true if
	/// different values in your application should never happen. Set this to false
	/// if you are actually relying on the semantics of `a ⊔ b = max(a, b)`.
	const WARN_IF_DIFFERENT: bool;
}

impl<T> CRDT for T
where
	T: AutoCRDT,
{
	fn merge(&mut self, other: &Self) {
		if Self::WARN_IF_DIFFERENT && self != other {
			warn!(
				"Different CRDT values should be the same (logic error!): {:?} vs {:?}",
				self, other
			);
			if other > self {
				*self = other.clone();
			}
			warn!("Making an arbitrary choice: {:?}", self);
		} else if other > self {
			*self = other.clone();
		}
	}
}

impl AutoCRDT for String {
	const WARN_IF_DIFFERENT: bool = true;
}

impl AutoCRDT for bool {
	const WARN_IF_DIFFERENT: bool = true;
}

impl AutoCRDT for FixedBytes32 {
	const WARN_IF_DIFFERENT: bool = true;
}
