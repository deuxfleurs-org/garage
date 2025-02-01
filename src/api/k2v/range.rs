//! Utility module for retrieving ranges of items in Garage tables
//! Implements parameters (prefix, start, end, limit) as specified
//! for endpoints ReadIndex, ReadBatch and DeleteBatch

use std::sync::Arc;

use garage_table::replication::TableShardedReplication;
use garage_table::*;

use garage_api_common::helpers::key_after_prefix;

use crate::error::*;

/// Read range in a Garage table.
/// Returns (entries, more?, nextStart)
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_range<F>(
	table: &Arc<Table<F, TableShardedReplication>>,
	partition_key: &F::P,
	prefix: &Option<String>,
	start: &Option<String>,
	end: &Option<String>,
	limit: Option<u64>,
	filter: Option<F::Filter>,
	enumeration_order: EnumerationOrder,
) -> Result<(Vec<F::E>, bool, Option<String>), Error>
where
	F: TableSchema<S = String> + 'static,
{
	let (mut start, mut start_ignore) = match (prefix, start) {
		(None, None) => (None, false),
		(None, Some(s)) => (Some(s.clone()), false),
		(Some(p), Some(s)) => {
			if !s.starts_with(p) {
				return Err(Error::bad_request(format!(
					"Start key '{}' does not start with prefix '{}'",
					s, p
				)));
			}
			(Some(s.clone()), false)
		}
		(Some(p), None) if enumeration_order == EnumerationOrder::Reverse => {
			let start = key_after_prefix(p)
				.ok_or_internal_error("Sorry, can't list this prefix in reverse order")?;
			(Some(start), true)
		}
		(Some(p), None) => (Some(p.clone()), false),
	};

	let mut entries = vec![];
	loop {
		let n_get = std::cmp::min(
			1000,
			limit.map(|x| x as usize).unwrap_or(usize::MAX - 10) - entries.len() + 2,
		);
		let get_ret = table
			.get_range(
				partition_key,
				start.clone(),
				filter.clone(),
				n_get,
				enumeration_order,
			)
			.await?;

		let get_ret_len = get_ret.len();

		for entry in get_ret {
			if start_ignore && Some(entry.sort_key()) == start.as_ref() {
				continue;
			}
			if let Some(p) = prefix {
				if !entry.sort_key().starts_with(p) {
					return Ok((entries, false, None));
				}
			}
			if let Some(e) = end {
				let is_finished = match enumeration_order {
					EnumerationOrder::Forward => entry.sort_key() >= e,
					EnumerationOrder::Reverse => entry.sort_key() <= e,
				};
				if is_finished {
					return Ok((entries, false, None));
				}
			}
			if let Some(l) = limit {
				if entries.len() >= l as usize {
					return Ok((entries, true, Some(entry.sort_key().clone())));
				}
			}
			entries.push(entry);
		}

		if get_ret_len < n_get {
			return Ok((entries, false, None));
		}

		start = Some(entries.last().unwrap().sort_key().clone());
		start_ignore = true;
	}
}
