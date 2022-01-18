use std::collections::{BTreeMap, BTreeSet};
use std::iter::{Iterator, Peekable};
use std::sync::Arc;

use hyper::{Body, Response};

use garage_util::data::*;
use garage_util::error::Error as GarageError;
use garage_util::time::*;

use garage_model::garage::Garage;
use garage_model::object_table::*;

use crate::encoding::*;
use crate::error::*;
use crate::s3_put;
use crate::s3_xml;

#[derive(Debug)]
pub struct ListQueryCommon {
	pub bucket_name: String,
	pub bucket_id: Uuid,
	pub delimiter: Option<String>,
	pub page_size: usize,
	pub prefix: String,
	pub urlencode_resp: bool,
}

#[derive(Debug)]
pub struct ListObjectsQuery {
	pub is_v2: bool,
	pub marker: Option<String>,
	pub continuation_token: Option<String>,
	pub start_after: Option<String>,
	pub common: ListQueryCommon,
}

#[derive(Debug)]
pub struct ListMultipartUploadsQuery {
	pub key_marker: Option<String>,
	pub upload_id_marker: Option<String>,
	pub common: ListQueryCommon,
}

pub async fn handle_list(
	garage: Arc<Garage>,
	query: &ListObjectsQuery,
) -> Result<Response<Body>, Error> {
	let io = |bucket, key, count| {
		let t = &garage.object_table;
		async move {
			t.get_range(&bucket, key, Some(ObjectFilter::IsData), count)
				.await
		}
	};

	let mut acc = query.build_accumulator();
	let pagination = fetch_list_entries(&query.common, query.begin()?, &mut acc, &io).await?;

	let result = s3_xml::ListBucketResult {
		xmlns: (),
		// Sending back request information
		name: s3_xml::Value(query.common.bucket_name.to_string()),
		prefix: uriencode_maybe(&query.common.prefix, query.common.urlencode_resp),
		max_keys: s3_xml::IntValue(query.common.page_size as i64),
		delimiter: query
			.common
			.delimiter
			.as_ref()
			.map(|x| uriencode_maybe(x, query.common.urlencode_resp)),
		encoding_type: match query.common.urlencode_resp {
			true => Some(s3_xml::Value("url".to_string())),
			false => None,
		},
		marker: match (!query.is_v2, &query.marker) {
			(true, Some(k)) => Some(uriencode_maybe(k, query.common.urlencode_resp)),
			_ => None,
		},
		start_after: match (query.is_v2, &query.start_after) {
			(true, Some(sa)) => Some(uriencode_maybe(sa, query.common.urlencode_resp)),
			_ => None,
		},
		continuation_token: match (query.is_v2, &query.continuation_token) {
			(true, Some(ct)) => Some(s3_xml::Value(ct.to_string())),
			_ => None,
		},

		// Pagination
		is_truncated: s3_xml::Value(format!("{}", pagination.is_some())),
		key_count: Some(s3_xml::IntValue(
			acc.keys.len() as i64 + acc.common_prefixes.len() as i64,
		)),
		next_marker: match (!query.is_v2, &pagination) {
			(true, Some(RangeBegin::AfterKey { key: k }))
			| (
				true,
				Some(RangeBegin::IncludingKey {
					fallback_key: Some(k),
					..
				}),
			) => Some(uriencode_maybe(k, query.common.urlencode_resp)),
			_ => None,
		},
		next_continuation_token: match (query.is_v2, &pagination) {
			(true, Some(RangeBegin::AfterKey { key })) => Some(s3_xml::Value(format!(
				"]{}",
				base64::encode(key.as_bytes())
			))),
			(true, Some(RangeBegin::IncludingKey { key, .. })) => Some(s3_xml::Value(format!(
				"[{}",
				base64::encode(key.as_bytes())
			))),
			_ => None,
		},

		// Body
		contents: acc
			.keys
			.iter()
			.map(|(key, info)| s3_xml::ListBucketItem {
				key: uriencode_maybe(key, query.common.urlencode_resp),
				last_modified: s3_xml::Value(msec_to_rfc3339(info.last_modified)),
				size: s3_xml::IntValue(info.size as i64),
				etag: s3_xml::Value(format!("\"{}\"", info.etag)),
				storage_class: s3_xml::Value("STANDARD".to_string()),
			})
			.collect(),
		common_prefixes: acc
			.common_prefixes
			.iter()
			.map(|pfx| s3_xml::CommonPrefix {
				prefix: uriencode_maybe(pfx, query.common.urlencode_resp),
			})
			.collect(),
	};

	let xml = s3_xml::to_xml_with_header(&result)?;
	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(xml.into_bytes()))?)
}

pub async fn handle_list_multipart_upload(
	garage: Arc<Garage>,
	query: &ListMultipartUploadsQuery,
) -> Result<Response<Body>, Error> {
	let io = |bucket, key, count| {
		let t = &garage.object_table;
		async move {
			t.get_range(&bucket, key, Some(ObjectFilter::IsUploading), count)
				.await
		}
	};

	let mut acc = query.build_accumulator();
	let pagination = fetch_list_entries(&query.common, query.begin()?, &mut acc, &io).await?;

	let result = s3_xml::ListMultipartUploadsResult {
		xmlns: (),

		// Sending back some information about the request
		bucket: s3_xml::Value(query.common.bucket_name.to_string()),
		prefix: uriencode_maybe(&query.common.prefix, query.common.urlencode_resp),
		delimiter: query
			.common
			.delimiter
			.as_ref()
			.map(|d| uriencode_maybe(d, query.common.urlencode_resp)),
		max_uploads: s3_xml::IntValue(query.common.page_size as i64),
		key_marker: query
			.key_marker
			.as_ref()
			.map(|m| uriencode_maybe(m, query.common.urlencode_resp)),
		upload_id_marker: query
			.upload_id_marker
			.as_ref()
			.map(|m| s3_xml::Value(m.to_string())),
		encoding_type: match query.common.urlencode_resp {
			true => Some(s3_xml::Value("url".to_string())),
			false => None,
		},

		// Handling pagination
		is_truncated: s3_xml::Value(format!("{}", pagination.is_some())),
		next_key_marker: match &pagination {
			None => None,
			Some(RangeBegin::AfterKey { key })
			| Some(RangeBegin::AfterUpload { key, .. })
			| Some(RangeBegin::IncludingKey { key, .. }) => {
				Some(uriencode_maybe(key, query.common.urlencode_resp))
			}
		},
		next_upload_id_marker: match pagination {
			Some(RangeBegin::AfterUpload { upload, .. }) => {
				Some(s3_xml::Value(hex::encode(upload)))
			}
			Some(RangeBegin::IncludingKey { .. }) => Some(s3_xml::Value("include".to_string())),
			_ => None,
		},

		// Result body
		upload: acc
			.keys
			.iter()
			.map(|(uuid, info)| s3_xml::ListMultipartItem {
				initiated: s3_xml::Value(msec_to_rfc3339(info.timestamp)),
				key: uriencode_maybe(&info.key, query.common.urlencode_resp),
				upload_id: s3_xml::Value(hex::encode(uuid)),
				storage_class: s3_xml::Value("STANDARD".to_string()),
				initiator: s3_xml::Initiator {
					display_name: s3_xml::Value("Dummy Key".to_string()),
					id: s3_xml::Value("GKDummyKey".to_string()),
				},
				owner: s3_xml::Owner {
					display_name: s3_xml::Value("Dummy Key".to_string()),
					id: s3_xml::Value("GKDummyKey".to_string()),
				},
			})
			.collect(),
		common_prefixes: acc
			.common_prefixes
			.iter()
			.map(|c| s3_xml::CommonPrefix {
				prefix: s3_xml::Value(c.to_string()),
			})
			.collect(),
	};

	let xml = s3_xml::to_xml_with_header(&result)?;

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(xml.into_bytes()))?)
}

/*
 * Private enums and structs
 */

#[derive(Debug)]
struct ObjectInfo {
	last_modified: u64,
	size: u64,
	etag: String,
}

#[derive(Debug, PartialEq)]
struct UploadInfo {
	key: String,
	timestamp: u64,
}

enum ExtractionResult {
	NoMore,
	Filled,
	FilledAtUpload {
		key: String,
		upload: Uuid,
	},
	Extracted {
		key: String,
	},
	// Fallback key is used for legacy APIs that only support
	// exlusive pagination (and not inclusive one).
	SkipTo {
		key: String,
		fallback_key: Option<String>,
	},
}

#[derive(PartialEq, Clone, Debug)]
enum RangeBegin {
	// Fallback key is used for legacy APIs that only support
	// exlusive pagination (and not inclusive one).
	IncludingKey {
		key: String,
		fallback_key: Option<String>,
	},
	AfterKey {
		key: String,
	},
	AfterUpload {
		key: String,
		upload: Uuid,
	},
}
type Pagination = Option<RangeBegin>;

/*
 * Fetch list entries
 */

async fn fetch_list_entries<R, F>(
	query: &ListQueryCommon,
	begin: RangeBegin,
	acc: &mut impl ExtractAccumulator,
	mut io: F,
) -> Result<Pagination, Error>
where
	R: futures::Future<Output = Result<Vec<Object>, GarageError>>,
	F: FnMut(Uuid, Option<String>, usize) -> R,
{
	let mut cursor = begin;
	// +1 is needed as we may need to skip the 1st key
	// (range is inclusive while most S3 requests are exclusive)
	let count = query.page_size + 1;

	loop {
		let start_key = match cursor {
			RangeBegin::AfterKey { ref key }
			| RangeBegin::AfterUpload { ref key, .. }
			| RangeBegin::IncludingKey { ref key, .. } => Some(key.clone()),
		};

		// Fetch objects
		let objects = io(query.bucket_id, start_key.clone(), count).await?;

		debug!(
			"List: get range {:?} (max {}), results: {}",
			start_key,
			count,
			objects.len()
		);
		let server_more = objects.len() >= count;

		let prev_req_cursor = cursor.clone();
		let mut iter = objects.iter().peekable();

		// Drop the first key if needed
		// Only AfterKey requires it according to the S3 spec and our implem.
		match (&cursor, iter.peek()) {
			(RangeBegin::AfterKey { key }, Some(object)) if &object.key == key => iter.next(),
			(_, _) => None,
		};

		while let Some(object) = iter.peek() {
			if !object.key.starts_with(&query.prefix) {
				// If the key is not in the requested prefix, we're done
				return Ok(None);
			}

			cursor = match acc.extract(query, &cursor, &mut iter) {
				ExtractionResult::Extracted { key } => RangeBegin::AfterKey { key },
				ExtractionResult::SkipTo { key, fallback_key } => {
					RangeBegin::IncludingKey { key, fallback_key }
				}
				ExtractionResult::FilledAtUpload { key, upload } => {
					return Ok(Some(RangeBegin::AfterUpload { key, upload }))
				}
				ExtractionResult::Filled => return Ok(Some(cursor)),
				ExtractionResult::NoMore => return Ok(None),
			};
		}

		if !server_more {
			// We did not fully fill the accumulator despite exhausting all the data we have,
			// we're done
			return Ok(None);
		}

		if prev_req_cursor == cursor {
			unreachable!("No progress has been done in the loop. This is a bug, please report it.");
		}
	}
}

/*
 * ListQuery logic
 */

/// Determine the key from where we want to start fetch objects from the database
///
/// We choose whether the object at this key must
/// be included or excluded from the response.
/// This key can be the prefix in the base case, or intermediate
/// points in the dataset if we are continuing a previous listing.
impl ListObjectsQuery {
	fn build_accumulator(&self) -> Accumulator<String, ObjectInfo> {
		Accumulator::<String, ObjectInfo>::new(self.common.page_size)
	}

	fn begin(&self) -> Result<RangeBegin, Error> {
		if self.is_v2 {
			match (&self.continuation_token, &self.start_after) {
				// In V2 mode, the continuation token is defined as an opaque
				// string in the spec, so we can do whatever we want with it.
				// In our case, it is defined as either [ or ] (for include
				// representing the key to start with.
				(Some(token), _) => match &token[..1] {
					"[" => Ok(RangeBegin::IncludingKey {
						key: String::from_utf8(base64::decode(token[1..].as_bytes())?)?,
						fallback_key: None,
					}),
					"]" => Ok(RangeBegin::AfterKey {
						key: String::from_utf8(base64::decode(token[1..].as_bytes())?)?,
					}),
					_ => Err(Error::BadRequest("Invalid continuation token".to_string())),
				},

				// StartAfter has defined semantics in the spec:
				// start listing at the first key immediately after.
				(_, Some(key)) => Ok(RangeBegin::AfterKey {
					key: key.to_string(),
				}),

				// In the case where neither is specified, we start
				// listing at the specified prefix. If an object has this
				// exact same key, we include it. (@TODO is this correct?)
				_ => Ok(RangeBegin::IncludingKey {
					key: self.common.prefix.to_string(),
					fallback_key: None,
				}),
			}
		} else {
			match &self.marker {
				// In V1 mode, the spec defines the Marker value to mean
				// the same thing as the StartAfter value in V2 mode.
				Some(key) => Ok(RangeBegin::AfterKey {
					key: key.to_string(),
				}),
				_ => Ok(RangeBegin::IncludingKey {
					key: self.common.prefix.to_string(),
					fallback_key: None,
				}),
			}
		}
	}
}

impl ListMultipartUploadsQuery {
	fn build_accumulator(&self) -> Accumulator<Uuid, UploadInfo> {
		Accumulator::<Uuid, UploadInfo>::new(self.common.page_size)
	}

	fn begin(&self) -> Result<RangeBegin, Error> {
		match (&self.upload_id_marker, &self.key_marker) {
			// If both the upload id marker and the key marker are sets,
			// the spec specifies that we must start listing uploads INCLUDING the given key,
			// AFTER the specified upload id (sorted in a lexicographic order).
			// To enable some optimisations, we emulate "IncludingKey" by extending the upload id
			// semantic. We base our reasoning on the hypothesis that S3's upload ids are opaques
			// while Garage's ones are 32 bytes hex encoded which enables us to extend this query
			// with a specific "include" upload id.
			(Some(up_marker), Some(key_marker)) => match &up_marker[..] {
				"include" => Ok(RangeBegin::IncludingKey {
					key: key_marker.to_string(),
					fallback_key: None,
				}),
				uuid => Ok(RangeBegin::AfterUpload {
					key: key_marker.to_string(),
					upload: s3_put::decode_upload_id(uuid)?,
				}),
			},

			// If only the key marker is specified, the spec says that we must start listing
			// uploads AFTER the specified key.
			(None, Some(key_marker)) => Ok(RangeBegin::AfterKey {
				key: key_marker.to_string(),
			}),
			_ => Ok(RangeBegin::IncludingKey {
				key: self.common.prefix.to_string(),
				fallback_key: None,
			}),
		}
	}
}

/*
 * Accumulator logic
 */

trait ExtractAccumulator {
	fn extract<'a>(
		&mut self,
		query: &ListQueryCommon,
		cursor: &RangeBegin,
		iter: &mut Peekable<impl Iterator<Item = &'a Object>>,
	) -> ExtractionResult;
}

struct Accumulator<K, V> {
	common_prefixes: BTreeSet<String>,
	keys: BTreeMap<K, V>,
	max_capacity: usize,
}

type ObjectAccumulator = Accumulator<String, ObjectInfo>;
type UploadAccumulator = Accumulator<Uuid, UploadInfo>;

impl<K: std::cmp::Ord, V> Accumulator<K, V> {
	fn new(page_size: usize) -> Accumulator<K, V> {
		Accumulator {
			common_prefixes: BTreeSet::<String>::new(),
			keys: BTreeMap::<K, V>::new(),
			max_capacity: page_size,
		}
	}

	/// Observe the Object iterator and try to extract a single common prefix
	///
	/// This function can consume an arbitrary number of items as long as they share the same
	/// common prefix.
	fn extract_common_prefix<'a>(
		&mut self,
		objects: &mut Peekable<impl Iterator<Item = &'a Object>>,
		query: &ListQueryCommon,
	) -> Option<ExtractionResult> {
		// Get the next object from the iterator
		let object = objects.peek().expect("This iterator can not be empty as it is checked earlier in the code. This is a logic bug, please report it.");

		// Check if this is a common prefix (requires a passed delimiter and its value in the key)
		let pfx = match common_prefix(object, query) {
			Some(p) => p,
			None => return None,
		};

		// Try to register this prefix
		// If not possible, we can return early
		if !self.try_insert_common_prefix(pfx.to_string()) {
			return Some(ExtractionResult::Filled);
		}

		// We consume the whole common prefix from the iterator
		let mut last_pfx_key = &object.key;
		loop {
			last_pfx_key = match objects.peek() {
				Some(o) if o.key.starts_with(pfx) => &o.key,
				Some(_) => {
					return Some(ExtractionResult::Extracted {
						key: last_pfx_key.to_owned(),
					})
				}
				None => {
					return match key_after_prefix(pfx) {
						Some(next) => Some(ExtractionResult::SkipTo {
							key: next,
							fallback_key: Some(last_pfx_key.to_owned()),
						}),
						None => Some(ExtractionResult::NoMore),
					}
				}
			};

			objects.next();
		}
	}

	fn is_full(&mut self) -> bool {
		self.keys.len() + self.common_prefixes.len() >= self.max_capacity
	}

	fn try_insert_common_prefix(&mut self, key: String) -> bool {
		// If we already have an entry, we can continue
		if self.common_prefixes.contains(&key) {
			return true;
		}

		// Otherwise, we need to check if we can add it
		match self.is_full() {
			true => false,
			false => {
				self.common_prefixes.insert(key);
				true
			}
		}
	}

	fn try_insert_entry(&mut self, key: K, value: V) -> bool {
		// It is impossible to add twice a key, this is an error
		assert!(!self.keys.contains_key(&key));

		match self.is_full() {
			true => false,
			false => {
				self.keys.insert(key, value);
				true
			}
		}
	}
}

impl ExtractAccumulator for ObjectAccumulator {
	fn extract<'a>(
		&mut self,
		query: &ListQueryCommon,
		_cursor: &RangeBegin,
		objects: &mut Peekable<impl Iterator<Item = &'a Object>>,
	) -> ExtractionResult {
		if let Some(e) = self.extract_common_prefix(objects, query) {
			return e;
		}

		let object = objects.next().expect("This iterator can not be empty as it is checked earlier in the code. This is a logic bug, please report it.");

		let version = match object.versions().iter().find(|x| x.is_data()) {
			Some(v) => v,
			None => unreachable!(
				"Expect to have objects having data due to earlier filtering. This is a logic bug."
			),
		};

		let meta = match &version.state {
			ObjectVersionState::Complete(ObjectVersionData::Inline(meta, _)) => meta,
			ObjectVersionState::Complete(ObjectVersionData::FirstBlock(meta, _)) => meta,
			_ => unreachable!(),
		};
		let info = ObjectInfo {
			last_modified: version.timestamp,
			size: meta.size,
			etag: meta.etag.to_string(),
		};

		match self.try_insert_entry(object.key.clone(), info) {
			true => ExtractionResult::Extracted {
				key: object.key.clone(),
			},
			false => ExtractionResult::Filled,
		}
	}
}

impl ExtractAccumulator for UploadAccumulator {
	/// Observe the iterator, process a single key, and try to extract one or more upload entries
	///
	/// This function processes a single object from the iterator that can contain an arbitrary
	/// number of versions, and thus "uploads".
	fn extract<'a>(
		&mut self,
		query: &ListQueryCommon,
		cursor: &RangeBegin,
		objects: &mut Peekable<impl Iterator<Item = &'a Object>>,
	) -> ExtractionResult {
		if let Some(e) = self.extract_common_prefix(objects, query) {
			return e;
		}

		// Get the next object from the iterator
		let object = objects.next().expect("This iterator can not be empty as it is checked earlier in the code. This is a logic bug, please report it.");

		let mut uploads_for_key = object
			.versions()
			.iter()
			.filter(|x| x.is_uploading())
			.collect::<Vec<&ObjectVersion>>();

		// S3 logic requires lexicographically sorted upload ids.
		uploads_for_key.sort_unstable_by_key(|e| e.uuid);

		// Skip results if an upload marker is provided
		if let RangeBegin::AfterUpload { upload, .. } = cursor {
			// Because our data are sorted, we can use a binary search to find the UUID
			// or to find where it should have been added. Once this position is found,
			// we use it to discard the first part of the array.
			let idx = match uploads_for_key.binary_search_by(|e| e.uuid.cmp(upload)) {
				// we start after the found uuid so we need to discard the pointed value.
				// In the worst case, the UUID is the last element, which lead us to an empty array
				// but we are never out of bound.
				Ok(i) => i + 1,
				// if the UUID is not found, the upload may have been discarded between the 2 request,
				// this function returns where it could have been inserted,
				// the pointed value is thus greater than our marker and we need to keep it.
				Err(i) => i,
			};
			uploads_for_key = uploads_for_key[idx..].to_vec();
		}

		let mut iter = uploads_for_key.iter();

		// The first entry is a specific case
		// as it changes our result enum type
		let first_upload = match iter.next() {
			Some(u) => u,
			None => {
				return ExtractionResult::Extracted {
					key: object.key.clone(),
				}
			}
		};
		let first_up_info = UploadInfo {
			key: object.key.to_string(),
			timestamp: first_upload.timestamp,
		};
		if !self.try_insert_entry(first_upload.uuid, first_up_info) {
			return ExtractionResult::Filled;
		}

		// We can then collect the remaining uploads in a loop
		let mut prev_uuid = first_upload.uuid;
		for upload in iter {
			let up_info = UploadInfo {
				key: object.key.to_string(),
				timestamp: upload.timestamp,
			};

			// Insert data in our accumulator
			// If it is full, return information to paginate.
			if !self.try_insert_entry(upload.uuid, up_info) {
				return ExtractionResult::FilledAtUpload {
					key: object.key.clone(),
					upload: prev_uuid,
				};
			}
			// Update our last added UUID
			prev_uuid = upload.uuid;
		}

		// We successfully collected all the uploads
		ExtractionResult::Extracted {
			key: object.key.clone(),
		}
	}
}

/*
 * Utility functions
 */

/// Returns the common prefix of the object given the query prefix and delimiter
fn common_prefix<'a>(object: &'a Object, query: &ListQueryCommon) -> Option<&'a str> {
	match &query.delimiter {
		Some(delimiter) => object.key[query.prefix.len()..]
			.find(delimiter)
			.map(|i| &object.key[..query.prefix.len() + i + delimiter.len()]),
		None => None,
	}
}

/// URIencode a value if needed
fn uriencode_maybe(s: &str, yes: bool) -> s3_xml::Value {
	if yes {
		s3_xml::Value(uri_encode(s, true))
	} else {
		s3_xml::Value(s.to_string())
	}
}

const UTF8_BEFORE_LAST_CHAR: char = '\u{10FFFE}';

/// Compute the key after the prefix
fn key_after_prefix(pfx: &str) -> Option<String> {
	let mut next = pfx.to_string();
	while !next.is_empty() {
		let tail = next.pop().unwrap();
		if tail >= char::MAX {
			continue;
		}

		// Circumvent a limitation of RangeFrom that overflow earlier than needed
		// See: https://doc.rust-lang.org/core/ops/struct.RangeFrom.html
		let new_tail = if tail == UTF8_BEFORE_LAST_CHAR {
			char::MAX
		} else {
			(tail..).nth(1).unwrap()
		};

		next.push(new_tail);
		return Some(next);
	}

	None
}

/*
 * Unit tests of this module
 */
#[cfg(test)]
mod tests {
	use super::*;
	use std::iter::FromIterator;

	const TS: u64 = 1641394898314;

	fn bucket() -> Uuid {
		Uuid::from([0x42; 32])
	}

	fn query() -> ListMultipartUploadsQuery {
		ListMultipartUploadsQuery {
			common: ListQueryCommon {
				prefix: "".to_string(),
				delimiter: Some("/".to_string()),
				page_size: 1000,
				urlencode_resp: false,
				bucket_name: "a".to_string(),
				bucket_id: Uuid::from([0x00; 32]),
			},
			key_marker: None,
			upload_id_marker: None,
		}
	}

	fn objs() -> Vec<Object> {
		vec![
			Object::new(
				bucket(),
				"a/b/c".to_string(),
				vec![objup_version([0x01; 32])],
			),
			Object::new(bucket(), "d".to_string(), vec![objup_version([0x01; 32])]),
		]
	}

	fn objup_version(uuid: [u8; 32]) -> ObjectVersion {
		ObjectVersion {
			uuid: Uuid::from(uuid),
			timestamp: TS,
			state: ObjectVersionState::Uploading(ObjectVersionHeaders {
				content_type: "text/plain".to_string(),
				other: BTreeMap::<String, String>::new(),
			}),
		}
	}

	#[test]
	fn test_key_after_prefix() {
		assert_eq!(UTF8_BEFORE_LAST_CHAR as u32, (char::MAX as u32) - 1);
		assert_eq!(key_after_prefix("a/b/").unwrap().as_str(), "a/b0");
		assert_eq!(key_after_prefix("€").unwrap().as_str(), "₭");
		assert_eq!(
			key_after_prefix("􏿽").unwrap().as_str(),
			String::from(char::from_u32(0x10FFFE).unwrap())
		);

		// When the last character is the biggest UTF8 char
		let a = String::from_iter(['a', char::MAX].iter());
		assert_eq!(key_after_prefix(a.as_str()).unwrap().as_str(), "b");

		// When all characters are the biggest UTF8 char
		let b = String::from_iter([char::MAX; 3].iter());
		assert!(key_after_prefix(b.as_str()).is_none());

		// Check utf8 surrogates
		let c = String::from('\u{D7FF}');
		assert_eq!(
			key_after_prefix(c.as_str()).unwrap().as_str(),
			String::from('\u{E000}')
		);

		// Check the character before the biggest one
		let d = String::from('\u{10FFFE}');
		assert_eq!(
			key_after_prefix(d.as_str()).unwrap().as_str(),
			String::from(char::MAX)
		);
	}

	#[test]
	fn test_common_prefixes() {
		let mut query = query();
		let objs = objs();

		query.common.prefix = "a/".to_string();
		assert_eq!(
			common_prefix(&objs.get(0).unwrap(), &query.common),
			Some("a/b/")
		);

		query.common.prefix = "a/b/".to_string();
		assert_eq!(common_prefix(&objs.get(0).unwrap(), &query.common), None);
	}

	#[test]
	fn test_extract_common_prefix() {
		let mut query = query();
		query.common.prefix = "a/".to_string();
		let objs = objs();
		let mut acc = UploadAccumulator::new(query.common.page_size);

		let mut iter = objs.iter().peekable();
		match acc.extract_common_prefix(&mut iter, &query.common) {
			Some(ExtractionResult::Extracted { key }) => assert_eq!(key, "a/b/c".to_string()),
			_ => panic!("wrong result"),
		}
		assert_eq!(acc.common_prefixes.len(), 1);
		assert_eq!(acc.common_prefixes.iter().next().unwrap(), "a/b/");
	}

	#[test]
	fn test_extract_upload() {
		let objs = vec![
			Object::new(
				bucket(),
				"b".to_string(),
				vec![
					objup_version([0x01; 32]),
					objup_version([0x80; 32]),
					objup_version([0x8f; 32]),
					objup_version([0xdd; 32]),
				],
			),
			Object::new(bucket(), "c".to_string(), vec![]),
		];

		let mut acc = UploadAccumulator::new(2);
		let mut start = RangeBegin::AfterUpload {
			key: "b".to_string(),
			upload: Uuid::from([0x01; 32]),
		};

		let mut iter = objs.iter().peekable();

		// Check the case where we skip some uploads
		match acc.extract(&(query().common), &start, &mut iter) {
			ExtractionResult::FilledAtUpload { key, upload } => {
				assert_eq!(key, "b");
				assert_eq!(upload, Uuid::from([0x8f; 32]));
			}
			_ => panic!("wrong result"),
		};

		assert_eq!(acc.keys.len(), 2);
		assert_eq!(
			acc.keys.get(&Uuid::from([0x80; 32])).unwrap(),
			&UploadInfo {
				timestamp: TS,
				key: "b".to_string()
			}
		);
		assert_eq!(
			acc.keys.get(&Uuid::from([0x8f; 32])).unwrap(),
			&UploadInfo {
				timestamp: TS,
				key: "b".to_string()
			}
		);

		acc = UploadAccumulator::new(2);
		start = RangeBegin::AfterUpload {
			key: "b".to_string(),
			upload: Uuid::from([0xff; 32]),
		};
		iter = objs.iter().peekable();

		// Check the case where we skip all the uploads
		match acc.extract(&(query().common), &start, &mut iter) {
			ExtractionResult::Extracted { key } if key.as_str() == "b" => (),
			_ => panic!("wrong result"),
		};
	}

	#[tokio::test]
	async fn test_fetch_uploads_no_result() -> Result<(), Error> {
		let query = query();
		let mut acc = query.build_accumulator();
		let page = fetch_list_entries(
			&query.common,
			query.begin()?,
			&mut acc,
			|_, _, _| async move { Ok(vec![]) },
		)
		.await?;
		assert_eq!(page, None);
		assert_eq!(acc.common_prefixes.len(), 0);
		assert_eq!(acc.keys.len(), 0);

		Ok(())
	}

	#[tokio::test]
	async fn test_fetch_uploads_basic() -> Result<(), Error> {
		let query = query();
		let mut acc = query.build_accumulator();
		let mut fake_io = |_, _, _| async move { Ok(objs()) };
		let page =
			fetch_list_entries(&query.common, query.begin()?, &mut acc, &mut fake_io).await?;
		assert_eq!(page, None);
		assert_eq!(acc.common_prefixes.len(), 1);
		assert_eq!(acc.keys.len(), 1);
		assert!(acc.common_prefixes.contains("a/"));

		Ok(())
	}

	#[tokio::test]
	async fn test_fetch_uploads_advanced() -> Result<(), Error> {
		let mut query = query();
		query.common.page_size = 2;

		let mut fake_io = |_, k: Option<String>, _| async move {
			Ok(match k.as_deref() {
				Some("") => vec![
					Object::new(bucket(), "b/a".to_string(), vec![objup_version([0x01; 32])]),
					Object::new(bucket(), "b/b".to_string(), vec![objup_version([0x01; 32])]),
					Object::new(bucket(), "b/c".to_string(), vec![objup_version([0x01; 32])]),
				],
				Some("b0") => vec![
					Object::new(bucket(), "c/a".to_string(), vec![objup_version([0x01; 32])]),
					Object::new(bucket(), "c/b".to_string(), vec![objup_version([0x01; 32])]),
					Object::new(bucket(), "c/c".to_string(), vec![objup_version([0x02; 32])]),
				],
				Some("c0") => vec![Object::new(
					bucket(),
					"d".to_string(),
					vec![objup_version([0x01; 32])],
				)],
				_ => panic!("wrong value {:?}", k),
			})
		};

		let mut acc = query.build_accumulator();
		let page =
			fetch_list_entries(&query.common, query.begin()?, &mut acc, &mut fake_io).await?;
		assert_eq!(
			page,
			Some(RangeBegin::IncludingKey {
				key: "c0".to_string(),
				fallback_key: Some("c/c".to_string())
			})
		);
		assert_eq!(acc.common_prefixes.len(), 2);
		assert_eq!(acc.keys.len(), 0);
		assert!(acc.common_prefixes.contains("b/"));
		assert!(acc.common_prefixes.contains("c/"));

		Ok(())
	}
}
