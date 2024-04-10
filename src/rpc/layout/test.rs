use std::cmp::min;
use std::collections::HashMap;

use garage_util::crdt::Crdt;
use garage_util::error::*;

use crate::layout::*;
use crate::replication_mode::ReplicationFactor;

// This function checks that the partition size S computed is at least better than the
// one given by a very naive algorithm. To do so, we try to run the naive algorithm
// assuming a partion size of S+1. If we succed, it means that the optimal assignment
// was not optimal. The naive algorithm is the following :
// - we compute the max number of partitions associated to every node, capped at the
// partition number. It gives the number of tokens of every node.
// - every zone has a number of tokens equal to the sum of the tokens of its nodes.
// - we cycle over the partitions and associate zone tokens while respecting the
// zone redundancy constraint.
// NOTE: the naive algorithm is not optimal. Counter example:
// take nb_partition = 3  ; replication_factor = 5; redundancy = 4;
// number of tokens by zone : (A, 4), (B,1), (C,4), (D, 4), (E, 2)
// With these parameters, the naive algo fails, whereas there is a solution:
// (A,A,C,D,E) , (A,B,C,D,D) (A,C,C,D,E)
fn check_against_naive(cl: &LayoutVersion) -> Result<bool, Error> {
	let over_size = cl.partition_size + 1;
	let mut zone_token = HashMap::<String, usize>::new();

	let (zones, zone_to_id) = cl.generate_nongateway_zone_ids()?;

	if zones.is_empty() {
		return Ok(false);
	}

	for z in zones.iter() {
		zone_token.insert(z.clone(), 0);
	}
	for uuid in cl.nongateway_nodes() {
		let z = cl.expect_get_node_zone(&uuid);
		let c = cl.expect_get_node_capacity(&uuid);
		zone_token.insert(
			z.to_string(),
			zone_token[z] + min(NB_PARTITIONS, (c / over_size) as usize),
		);
	}

	// For every partition, we count the number of zone already associated and
	// the name of the last zone associated

	let mut id_zone_token = vec![0; zones.len()];
	for (z, t) in zone_token.iter() {
		id_zone_token[zone_to_id[z]] = *t;
	}

	let mut nb_token = vec![0; NB_PARTITIONS];
	let mut last_zone = vec![zones.len(); NB_PARTITIONS];

	let mut curr_zone = 0;

	let redundancy = cl.effective_zone_redundancy();

	for replic in 0..cl.replication_factor {
		for p in 0..NB_PARTITIONS {
			while id_zone_token[curr_zone] == 0
				|| (last_zone[p] == curr_zone
					&& redundancy - nb_token[p] <= cl.replication_factor - replic)
			{
				curr_zone += 1;
				if curr_zone >= zones.len() {
					return Ok(true);
				}
			}
			id_zone_token[curr_zone] -= 1;
			if last_zone[p] != curr_zone {
				nb_token[p] += 1;
				last_zone[p] = curr_zone;
			}
		}
	}

	return Ok(false);
}

fn show_msg(msg: &Message) {
	for s in msg.iter() {
		println!("{}", s);
	}
}

fn update_layout(
	cl: &mut LayoutHistory,
	node_capacity_vec: &[u64],
	node_zone_vec: &[&'static str],
	zone_redundancy: usize,
) {
	let staging = cl.staging.get_mut();

	for (i, (capacity, zone)) in node_capacity_vec
		.iter()
		.zip(node_zone_vec.iter())
		.enumerate()
	{
		let node_id = [i as u8; 32].into();

		let update = staging.roles.update_mutator(
			node_id,
			NodeRoleV(Some(NodeRole {
				zone: zone.to_string(),
				capacity: Some(*capacity),
				tags: (vec![]),
			})),
		);
		staging.roles.merge(&update);
	}
	staging.parameters.update(LayoutParameters {
		zone_redundancy: ZoneRedundancy::AtLeast(zone_redundancy),
	});
}

#[test]
fn test_assignment() {
	let mut node_capacity_vec = vec![4000, 1000, 2000];
	let mut node_zone_vec = vec!["A", "B", "C"];

	let mut cl = LayoutHistory::new(ReplicationFactor::new(3).unwrap());
	update_layout(&mut cl, &node_capacity_vec, &node_zone_vec, 3);
	let v = cl.current().version;
	let (mut cl, msg) = cl.apply_staged_changes(Some(v + 1)).unwrap();
	show_msg(&msg);
	assert_eq!(cl.check(), Ok(()));
	assert!(check_against_naive(cl.current()).unwrap());

	node_capacity_vec = vec![4000, 1000, 1000, 3000, 1000, 1000, 2000, 10000, 2000];
	node_zone_vec = vec!["A", "B", "C", "C", "C", "B", "G", "H", "I"];
	update_layout(&mut cl, &node_capacity_vec, &node_zone_vec, 2);
	let v = cl.current().version;
	let (mut cl, msg) = cl.apply_staged_changes(Some(v + 1)).unwrap();
	show_msg(&msg);
	assert_eq!(cl.check(), Ok(()));
	assert!(check_against_naive(cl.current()).unwrap());

	node_capacity_vec = vec![4000, 1000, 2000, 7000, 1000, 1000, 2000, 10000, 2000];
	update_layout(&mut cl, &node_capacity_vec, &node_zone_vec, 3);
	let v = cl.current().version;
	let (mut cl, msg) = cl.apply_staged_changes(Some(v + 1)).unwrap();
	show_msg(&msg);
	assert_eq!(cl.check(), Ok(()));
	assert!(check_against_naive(cl.current()).unwrap());

	node_capacity_vec = vec![
		4000000, 4000000, 2000000, 7000000, 1000000, 9000000, 2000000, 10000, 2000000,
	];
	update_layout(&mut cl, &node_capacity_vec, &node_zone_vec, 1);
	let v = cl.current().version;
	let (cl, msg) = cl.apply_staged_changes(Some(v + 1)).unwrap();
	show_msg(&msg);
	assert_eq!(cl.check(), Ok(()));
	assert!(check_against_naive(cl.current()).unwrap());
}
