#![no_main]

use garage_model::s3::object_table::{Object, ObjectVersion};
use garage_table::crdt::Crdt;
use libfuzzer_sys::fuzz_target;

/// Build an Object from an arbitrary version list, using a fixed bucket/key
/// so that version slices can be compared across merge results.
/// Duplicate versions (same uuid+timestamp) are dropped before construction.
fn make_object(mut versions: Vec<ObjectVersion>) -> Object {
	versions.sort_by_key(|v| (v.timestamp, v.uuid));
	versions.dedup_by_key(|v| (v.timestamp, v.uuid));
	let mut res = Object::new([0u8; 32].into(), String::new(), versions);
	res.strip_obsolete();
	res
}

fuzz_target!(
	|inputs: (Vec<ObjectVersion>, Vec<ObjectVersion>, Vec<ObjectVersion>)| {
		let (v1, v2, v3) = inputs;
		let a = make_object(v1);
		let b = make_object(v2);
		let c = make_object(v3);

		// Idempotency: merge(a, a) == a
		{
			let mut a2 = a.clone();
			a2.merge(&a.clone());
			assert_eq!(
				a2.versions(),
				a.versions(),
				"merge is not idempotent: {a2:#?} != {a:#?}"
			);
		}

		// Commutativity: versions(merge(a, b)) == versions(merge(b, a))
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
			ab.versions(),
			ba.versions(),
			"merge is not commutative: {ab:#?} != {ba:#?}"
		);

		// Associativity: versions(merge(merge(a, b), c)) == versions(merge(a, merge(b, c)))
		let ab_c = {
			let mut t = ab.clone();
			t.merge(&c);
			t
		};
		let bc = {
			let mut t = b.clone();
			t.merge(&c);
			t
		};
		let a_bc = {
			let mut t = a.clone();
			t.merge(&bc);
			t
		};
		assert_eq!(
			ab_c.versions(),
			a_bc.versions(),
			"merge is not associative: {ab_c:#?} != {a_bc:#?}"
		);
	}
);
