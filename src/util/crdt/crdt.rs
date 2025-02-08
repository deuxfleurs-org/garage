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
pub trait Crdt {
	/// Merge the two datastructures according to the CRDT rules.
	/// `self` is modified to contain the merged CRDT value. `other` is not modified.
	///
	/// # Arguments
	///
	/// * `other` - the other CRDT we wish to merge with
	fn merge(&mut self, other: &Self);
}

/// Option<T> implements Crdt for any type T, even if T doesn't implement CRDT itself: when
/// different values are detected, they are always merged to None.  This can be used for value
/// types which shoulnd't be merged, instead of trying to merge things when we know we don't want
/// to merge them (which is what the AutoCrdt trait is used for most of the time). This cases
/// arises very often, for example with a Lww or a LwwMap: the value type has to be a CRDT so that
/// we have a rule for what to do when timestamps aren't enough to disambiguate (in a distributed
/// system, anything can happen!), and with AutoCrdt the rule is to make an arbitrary (but
/// deterministic) choice between the two.  When using an Option<T> instead with this impl, ambiguity
/// cases are explicitly stored as None, which allows us to detect the ambiguity and handle it in
/// the way we want. (this can only work if we are happy with losing the value when an ambiguity
/// arises)
impl<T> Crdt for Option<T>
where
	T: Eq,
{
	fn merge(&mut self, other: &Self) {
		if self != other {
			*self = None;
		}
	}
}

/// All types that implement `Ord` (a total order) can also implement a trivial CRDT
/// defined by the merge rule: `a ⊔ b = max(a, b)`. Implement this trait for your type
/// to enable this behavior.
pub trait AutoCrdt: Ord + Clone + std::fmt::Debug {
	/// WARN_IF_DIFFERENT: emit a warning when values differ. Set this to true if
	/// different values in your application should never happen. Set this to false
	/// if you are actually relying on the semantics of `a ⊔ b = max(a, b)`.
	const WARN_IF_DIFFERENT: bool;
}

impl<T> Crdt for T
where
	T: AutoCrdt,
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

impl AutoCrdt for String {
	const WARN_IF_DIFFERENT: bool = true;
}

impl AutoCrdt for bool {
	const WARN_IF_DIFFERENT: bool = true;
}
