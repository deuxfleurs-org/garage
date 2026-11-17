use garage_table::crdt::Crdt;
use std::fmt::Debug;

pub fn check_crdt_laws<T>(a: T, b: T, c: T)
where
	T: Crdt + PartialEq + Clone + Debug,
{
	// Idempotency: merge(a, a) == a
	{
		let mut a2 = a.clone();
		a2.merge(&a);
		assert_eq!(
			a2, a,
			"merge is not idempotent:\n\na :: {a:?}\n\na2 :: {a2:?}\n"
		);
	}

	// Commutativity: merge(a, b) == merge(b, a)
	let ab = {
		let mut t = a.clone();
		t.merge(&b);
		t
	};
	let ba = {
		let mut t = b.clone();
		t.merge(&a);
		t
	};
	assert_eq!(
		ab, ba,
		"merge is not commutative:\n\nab :: {ab:?}\n\nba :: {ba:?}\n"
	);

	// LX's corrolary: merge(merge(a,b),b) = merge(a,b)
	let ab_b = {
		let mut t = ab.clone();
		t.merge(&b);
		t
	};
	assert_eq!(
		ab, ab_b,
		"merge(merge(a,b),b) != merge(a,b)):\n\nab :: {ab:?}\n\nb :: {b:?}\n\nab_b :: {ab_b:?}\n"
	);

	// Associativity: merge(merge(a, b), c) == merge(a, merge(b, c))
	let ab_c = {
		let mut t = ab;
		t.merge(&c);
		t
	};
	let bc = {
		let mut t = b;
		t.merge(&c);
		t
	};
	let a_bc = {
		let mut t = a;
		t.merge(&bc);
		t
	};
	assert_eq!(
		ab_c, a_bc,
		"merge is not associative:\n\nab_c :: {ab_c:?}\n\na_bc :: {a_bc:?}\n"
	);
}
