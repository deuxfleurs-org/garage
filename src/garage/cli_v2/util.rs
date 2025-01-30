use bytesize::ByteSize;
use format_table::format_table;

use garage_util::error::Error;

use garage_api::admin::api::*;

pub fn capacity_string(v: Option<u64>) -> String {
	match v {
		Some(c) => ByteSize::b(c).to_string_as(false),
		None => "gateway".to_string(),
	}
}

pub fn get_staged_or_current_role(
	id: &str,
	layout: &GetClusterLayoutResponse,
) -> Option<NodeRoleResp> {
	for node in layout.staged_role_changes.iter() {
		if node.id == id {
			return match &node.action {
				NodeRoleChangeEnum::Remove { .. } => None,
				NodeRoleChangeEnum::Update {
					zone,
					capacity,
					tags,
				} => Some(NodeRoleResp {
					id: id.to_string(),
					zone: zone.to_string(),
					capacity: *capacity,
					tags: tags.clone(),
				}),
			};
		}
	}

	for node in layout.roles.iter() {
		if node.id == id {
			return Some(node.clone());
		}
	}

	None
}

pub fn find_matching_node<'a>(
	cand: impl std::iter::Iterator<Item = &'a str>,
	pattern: &'a str,
) -> Result<String, Error> {
	let mut candidates = vec![];
	for c in cand {
		if c.starts_with(pattern) && !candidates.contains(&c) {
			candidates.push(c);
		}
	}
	if candidates.len() != 1 {
		Err(Error::Message(format!(
			"{} nodes match '{}'",
			candidates.len(),
			pattern,
		)))
	} else {
		Ok(candidates[0].to_string())
	}
}

pub fn print_staging_role_changes(layout: &GetClusterLayoutResponse) -> bool {
	let has_role_changes = !layout.staged_role_changes.is_empty();

	// TODO!! Layout parameters
	let has_layout_changes = false;

	if has_role_changes || has_layout_changes {
		println!();
		println!("==== STAGED ROLE CHANGES ====");
		if has_role_changes {
			let mut table = vec!["ID\tTags\tZone\tCapacity".to_string()];
			for change in layout.staged_role_changes.iter() {
				match &change.action {
					NodeRoleChangeEnum::Update {
						tags,
						zone,
						capacity,
					} => {
						let tags = tags.join(",");
						table.push(format!(
							"{:.16}\t{}\t{}\t{}",
							change.id,
							tags,
							zone,
							capacity_string(*capacity),
						));
					}
					NodeRoleChangeEnum::Remove { .. } => {
						table.push(format!("{:.16}\tREMOVED", change.id));
					}
				}
			}
			format_table(table);
			println!();
		}
		//TODO
		/*
		if has_layout_changes {
			println!(
				"Zone redundancy: {}",
				staging.parameters.get().zone_redundancy
			);
		}
		*/
		true
	} else {
		false
	}
}
