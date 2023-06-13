//! Implements a RangeSeenMarker, a data type used in the PollRange API
//! to indicate which items in the range have already been seen
//! and which have not been seen yet.
//!
//! It consists of a vector clock that indicates that for each node,
//! all items produced by that node with timestamps <= the value in the
//! vector clock has been seen, as well as a set of causal contexts for
//! individual items.

use std::collections::BTreeMap;

use base64::prelude::*;
use serde::{Deserialize, Serialize};

use garage_util::data::Uuid;
use garage_util::encode::{nonversioned_decode, nonversioned_encode};
use garage_util::error::Error;

use crate::helper::error::{Error as HelperError, OkOrBadRequest};
use crate::k2v::causality::*;
use crate::k2v::item_table::*;
use crate::k2v::sub::*;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RangeSeenMarker {
	vector_clock: VectorClock,
	items: BTreeMap<String, VectorClock>,
}

impl RangeSeenMarker {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn restrict(&mut self, range: &PollRange) {
		if let Some(start) = &range.start {
			self.items = self.items.split_off(start);
		}
		if let Some(end) = &range.end {
			self.items.split_off(end);
		}
		if let Some(pfx) = &range.prefix {
			self.items.retain(|k, _v| k.starts_with(pfx));
		}
	}

	pub fn mark_seen_node_items<'a, I: IntoIterator<Item = &'a K2VItem>>(
		&mut self,
		node: Uuid,
		items: I,
	) {
		let node = make_node_id(node);
		for item in items.into_iter() {
			let cc = item.causal_context();

			if let Some(ts) = cc.vector_clock.get(&node) {
				let ent = self.vector_clock.entry(node).or_insert(0);
				*ent = std::cmp::max(*ent, *ts);
			}

			if vclock_gt(&cc.vector_clock, &self.vector_clock) {
				match self.items.get_mut(&item.sort_key) {
					None => {
						self.items.insert(item.sort_key.clone(), cc.vector_clock);
					}
					Some(ent) => *ent = vclock_max(ent, &cc.vector_clock),
				}
			}
		}
	}

	pub fn canonicalize(&mut self) {
		let self_vc = &self.vector_clock;
		self.items.retain(|_sk, vc| vclock_gt(vc, self_vc))
	}

	pub fn encode(&mut self) -> Result<String, Error> {
		self.canonicalize();

		let bytes = nonversioned_encode(&self)?;
		let bytes = zstd::stream::encode_all(&mut &bytes[..], zstd::DEFAULT_COMPRESSION_LEVEL)?;
		Ok(BASE64_STANDARD.encode(bytes))
	}

	/// Decode from msgpack+zstd+b64 representation, returns None on error.
	pub fn decode(s: &str) -> Option<Self> {
		let bytes = BASE64_STANDARD.decode(s).ok()?;
		let bytes = zstd::stream::decode_all(&mut &bytes[..]).ok()?;
		nonversioned_decode(&bytes).ok()
	}

	pub fn decode_helper(s: &str) -> Result<Self, HelperError> {
		Self::decode(s).ok_or_bad_request("Invalid causality token")
	}

	pub fn is_new_item(&self, item: &K2VItem) -> bool {
		let cc = item.causal_context();
		vclock_gt(&cc.vector_clock, &self.vector_clock)
			&& self
				.items
				.get(&item.sort_key)
				.map(|vc| vclock_gt(&cc.vector_clock, vc))
				.unwrap_or(true)
	}
}
