use chrono::{DateTime, Utc};
use serde::Serialize;

use garage_util::data::Uuid;

/// Integration event emitted when an object version becomes the latest
/// complete data-bearing version for a given key.
#[derive(Debug, Clone, Serialize)]
pub struct ObjectCreatedEvent {
	/// Logical type of the event, useful for routing and consumers.
	pub event_type: String,
	/// Unique identifier of this event instance.
	pub event_id: Uuid,
	/// Timestamp when the event was created.
	pub occurred_at: DateTime<Utc>,

	/// Identifier of the bucket that owns the object.
	pub bucket_id: Uuid,
	/// Object key within the bucket.
	pub key: String,
	/// Identifier of the object version that was created.
	pub version_id: Uuid,

	/// Size of the object payload in bytes.
	pub size: u64,
	/// Optional content type of the object, if known.
	pub content_type: Option<String>,
}

