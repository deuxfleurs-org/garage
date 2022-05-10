use crate::error::{Error, OkOrBadRequest};

use std::borrow::Cow;

use hyper::header::HeaderValue;
use hyper::{HeaderMap, Method, Request};

use crate::helpers::Authorization;
use crate::router_macros::{generateQueryParameters, router_match};

router_match! {@func

/// List of all S3 API endpoints.
///
/// For each endpoint, it contains the parameters this endpoint receive by url (bucket, key and
/// query parameters). Parameters it may receive by header are left out, however headers are
/// considered when required to determine between one endpoint or another (for CopyObject and
/// UploadObject, for instance).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
	AbortMultipartUpload {
		key: String,
		upload_id: String,
	},
	CompleteMultipartUpload {
		key: String,
		upload_id: String,
	},
	CopyObject {
		key: String,
	},
	CreateBucket {
	},
	CreateMultipartUpload {
		key: String,
	},
	DeleteBucket {
	},
	DeleteBucketAnalyticsConfiguration {
		id: String,
	},
	DeleteBucketCors {
	},
	DeleteBucketEncryption {
	},
	DeleteBucketIntelligentTieringConfiguration {
		id: String,
	},
	DeleteBucketInventoryConfiguration {
		id: String,
	},
	DeleteBucketLifecycle {
	},
	DeleteBucketMetricsConfiguration {
		id: String,
	},
	DeleteBucketOwnershipControls {
	},
	DeleteBucketPolicy {
	},
	DeleteBucketReplication {
	},
	DeleteBucketTagging {
	},
	DeleteBucketWebsite {
	},
	DeleteObject {
		key: String,
		version_id: Option<String>,
	},
	DeleteObjects {
	},
	DeleteObjectTagging {
		key: String,
		version_id: Option<String>,
	},
	DeletePublicAccessBlock {
	},
	GetBucketAccelerateConfiguration {
	},
	GetBucketAcl {
	},
	GetBucketAnalyticsConfiguration {
		id: String,
	},
	GetBucketCors {
	},
	GetBucketEncryption {
	},
	GetBucketIntelligentTieringConfiguration {
		id: String,
	},
	GetBucketInventoryConfiguration {
		id: String,
	},
	GetBucketLifecycleConfiguration {
	},
	GetBucketLocation {
	},
	GetBucketLogging {
	},
	GetBucketMetricsConfiguration {
		id: String,
	},
	GetBucketNotificationConfiguration {
	},
	GetBucketOwnershipControls {
	},
	GetBucketPolicy {
	},
	GetBucketPolicyStatus {
	},
	GetBucketReplication {
	},
	GetBucketRequestPayment {
	},
	GetBucketTagging {
	},
	GetBucketVersioning {
	},
	GetBucketWebsite {
	},
	/// There are actually many more query parameters, used to add headers to the answer. They were
	/// not added here as they are best handled in a dedicated route.
	GetObject {
		key: String,
		part_number: Option<u64>,
		version_id: Option<String>,
	},
	GetObjectAcl {
		key: String,
		version_id: Option<String>,
	},
	GetObjectLegalHold {
		key: String,
		version_id: Option<String>,
	},
	GetObjectLockConfiguration {
	},
	GetObjectRetention {
		key: String,
		version_id: Option<String>,
	},
	GetObjectTagging {
		key: String,
		version_id: Option<String>,
	},
	GetObjectTorrent {
		key: String,
	},
	GetPublicAccessBlock {
	},
	HeadBucket {
	},
	HeadObject {
		key: String,
		part_number: Option<u64>,
		version_id: Option<String>,
	},
	ListBucketAnalyticsConfigurations {
		continuation_token: Option<String>,
	},
	ListBucketIntelligentTieringConfigurations {
		continuation_token: Option<String>,
	},
	ListBucketInventoryConfigurations {
		continuation_token: Option<String>,
	},
	ListBucketMetricsConfigurations {
		continuation_token: Option<String>,
	},
	ListBuckets,
	ListMultipartUploads {
		delimiter: Option<char>,
		encoding_type: Option<String>,
		key_marker: Option<String>,
		max_uploads: Option<usize>,
		prefix: Option<String>,
		upload_id_marker: Option<String>,
	},
	ListObjects {
		delimiter: Option<char>,
		encoding_type: Option<String>,
		marker: Option<String>,
		max_keys: Option<usize>,
		prefix: Option<String>,
	},
	ListObjectsV2 {
		// This value should always be 2. It is not checked when constructing the struct
		list_type: String,
		continuation_token: Option<String>,
		delimiter: Option<char>,
		encoding_type: Option<String>,
		fetch_owner: Option<bool>,
		max_keys: Option<usize>,
		prefix: Option<String>,
		start_after: Option<String>,
	},
	ListObjectVersions {
		delimiter: Option<char>,
		encoding_type: Option<String>,
		key_marker: Option<String>,
		max_keys: Option<u64>,
		prefix: Option<String>,
		version_id_marker: Option<String>,
	},
	ListParts {
		key: String,
		max_parts: Option<u64>,
		part_number_marker: Option<u64>,
		upload_id: String,
	},
	Options,
	PutBucketAccelerateConfiguration {
	},
	PutBucketAcl {
	},
	PutBucketAnalyticsConfiguration {
		id: String,
	},
	PutBucketCors {
	},
	PutBucketEncryption {
	},
	PutBucketIntelligentTieringConfiguration {
		id: String,
	},
	PutBucketInventoryConfiguration {
		id: String,
	},
	PutBucketLifecycleConfiguration {
	},
	PutBucketLogging {
	},
	PutBucketMetricsConfiguration {
		id: String,
	},
	PutBucketNotificationConfiguration {
	},
	PutBucketOwnershipControls {
	},
	PutBucketPolicy {
	},
	PutBucketReplication {
	},
	PutBucketRequestPayment {
	},
	PutBucketTagging {
	},
	PutBucketVersioning {
	},
	PutBucketWebsite {
	},
	PutObject {
		key: String,
	},
	PutObjectAcl {
		key: String,
		version_id: Option<String>,
	},
	PutObjectLegalHold {
		key: String,
		version_id: Option<String>,
	},
	PutObjectLockConfiguration {
	},
	PutObjectRetention {
		key: String,
		version_id: Option<String>,
	},
	PutObjectTagging {
		key: String,
		version_id: Option<String>,
	},
	PutPublicAccessBlock {
	},
	RestoreObject {
		key: String,
		version_id: Option<String>,
	},
	SelectObjectContent {
		key: String,
		// This value should always be 2. It is not checked when constructing the struct
		select_type: String,
	},
	UploadPart {
		key: String,
		part_number: u64,
		upload_id: String,
	},
	UploadPartCopy {
		key: String,
		part_number: u64,
		upload_id: String,
	},
	// This endpoint is not documented with others because it has special use case :
	// It's intended to be used with HTML forms, using a multipart/form-data body.
	// It works a lot like presigned requests, but everything is in the form instead
	// of being query parameters of the URL, so authenticating it is a bit different.
	PostObject,
}}

impl Endpoint {
	/// Determine which S3 endpoint a request is for using the request, and a bucket which was
	/// possibly extracted from the Host header.
	/// Returns Self plus bucket name, if endpoint is not Endpoint::ListBuckets
	pub fn from_request<T>(
		req: &Request<T>,
		bucket: Option<String>,
	) -> Result<(Self, Option<String>), Error> {
		let uri = req.uri();
		let path = uri.path().trim_start_matches('/');
		let query = uri.query();
		if bucket.is_none() && path.is_empty() {
			if *req.method() == Method::OPTIONS {
				return Ok((Self::Options, None));
			} else {
				return Ok((Self::ListBuckets, None));
			}
		}

		let (bucket, key) = if let Some(bucket) = bucket {
			(bucket, path)
		} else {
			path.split_once('/')
				.map(|(b, p)| (b.to_owned(), p.trim_start_matches('/')))
				.unwrap_or((path.to_owned(), ""))
		};

		if *req.method() == Method::OPTIONS {
			return Ok((Self::Options, Some(bucket)));
		}

		let key = percent_encoding::percent_decode_str(key)
			.decode_utf8()?
			.into_owned();

		let mut query = QueryParameters::from_query(query.unwrap_or_default())?;

		let res = match *req.method() {
			Method::GET => Self::from_get(key, &mut query)?,
			Method::HEAD => Self::from_head(key, &mut query)?,
			Method::POST => Self::from_post(key, &mut query)?,
			Method::PUT => Self::from_put(key, &mut query, req.headers())?,
			Method::DELETE => Self::from_delete(key, &mut query)?,
			_ => return Err(Error::BadRequest("Unknown method".to_owned())),
		};

		if let Some(message) = query.nonempty_message() {
			debug!("Unused query parameter: {}", message)
		}
		Ok((res, Some(bucket)))
	}

	/// Determine which endpoint a request is for, knowing it is a GET.
	fn from_get(key: String, query: &mut QueryParameters<'_>) -> Result<Self, Error> {
		router_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), key, query, None),
			key: [
				EMPTY if upload_id => ListParts (query::upload_id, opt_parse::max_parts, opt_parse::part_number_marker),
				EMPTY => GetObject (query_opt::version_id, opt_parse::part_number),
				ACL => GetObjectAcl (query_opt::version_id),
				LEGAL_HOLD => GetObjectLegalHold (query_opt::version_id),
				RETENTION => GetObjectRetention (query_opt::version_id),
				TAGGING => GetObjectTagging (query_opt::version_id),
				TORRENT => GetObjectTorrent,
			],
			no_key: [
				EMPTY if list_type => ListObjectsV2 (query::list_type, query_opt::continuation_token,
													 opt_parse::delimiter, query_opt::encoding_type,
													 opt_parse::fetch_owner, opt_parse::max_keys,
													 query_opt::prefix, query_opt::start_after),
				EMPTY => ListObjects (opt_parse::delimiter, query_opt::encoding_type, query_opt::marker,
									  opt_parse::max_keys, opt_parse::prefix),
				ACCELERATE => GetBucketAccelerateConfiguration,
				ACL => GetBucketAcl,
				ANALYTICS if id => GetBucketAnalyticsConfiguration (query::id),
				ANALYTICS => ListBucketAnalyticsConfigurations (query_opt::continuation_token),
				CORS => GetBucketCors,
				ENCRYPTION => GetBucketEncryption,
				INTELLIGENT_TIERING if id => GetBucketIntelligentTieringConfiguration (query::id),
				INTELLIGENT_TIERING => ListBucketIntelligentTieringConfigurations (query_opt::continuation_token),
				INVENTORY if id => GetBucketInventoryConfiguration (query::id),
				INVENTORY => ListBucketInventoryConfigurations (query_opt::continuation_token),
				LIFECYCLE => GetBucketLifecycleConfiguration,
				LOCATION => GetBucketLocation,
				LOGGING => GetBucketLogging,
				METRICS if id => GetBucketMetricsConfiguration (query::id),
				METRICS => ListBucketMetricsConfigurations (query_opt::continuation_token),
				NOTIFICATION => GetBucketNotificationConfiguration,
				OBJECT_LOCK => GetObjectLockConfiguration,
				OWNERSHIP_CONTROLS => GetBucketOwnershipControls,
				POLICY => GetBucketPolicy,
				POLICY_STATUS => GetBucketPolicyStatus,
				PUBLIC_ACCESS_BLOCK => GetPublicAccessBlock,
				REPLICATION => GetBucketReplication,
				REQUEST_PAYMENT => GetBucketRequestPayment,
				TAGGING => GetBucketTagging,
				UPLOADS => ListMultipartUploads (opt_parse::delimiter, query_opt::encoding_type,
												 query_opt::key_marker, opt_parse::max_uploads,
												 query_opt::prefix, query_opt::upload_id_marker),
				VERSIONING => GetBucketVersioning,
				VERSIONS => ListObjectVersions (opt_parse::delimiter, query_opt::encoding_type,
												query_opt::key_marker, opt_parse::max_keys,
												query_opt::prefix, query_opt::version_id_marker),
				WEBSITE => GetBucketWebsite,
			]
		}
	}

	/// Determine which endpoint a request is for, knowing it is a HEAD.
	fn from_head(key: String, query: &mut QueryParameters<'_>) -> Result<Self, Error> {
		router_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), key, query, None),
			key: [
				EMPTY => HeadObject(opt_parse::part_number, query_opt::version_id),
			],
			no_key: [
				EMPTY => HeadBucket,
			]
		}
	}

	/// Determine which endpoint a request is for, knowing it is a POST.
	fn from_post(key: String, query: &mut QueryParameters<'_>) -> Result<Self, Error> {
		router_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), key, query, None),
			key: [
				EMPTY if upload_id  => CompleteMultipartUpload (query::upload_id),
				RESTORE => RestoreObject (query_opt::version_id),
				SELECT => SelectObjectContent (query::select_type),
				UPLOADS => CreateMultipartUpload,
			],
			no_key: [
				EMPTY => PostObject,
				DELETE => DeleteObjects,
			]
		}
	}

	/// Determine which endpoint a request is for, knowing it is a PUT.
	fn from_put(
		key: String,
		query: &mut QueryParameters<'_>,
		headers: &HeaderMap<HeaderValue>,
	) -> Result<Self, Error> {
		router_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), key, query, headers),
			key: [
				EMPTY if part_number header "x-amz-copy-source" => UploadPartCopy (parse::part_number, query::upload_id),
				EMPTY header "x-amz-copy-source" => CopyObject,
				EMPTY if part_number => UploadPart (parse::part_number, query::upload_id),
				EMPTY => PutObject,
				ACL => PutObjectAcl (query_opt::version_id),
				LEGAL_HOLD => PutObjectLegalHold (query_opt::version_id),
				RETENTION => PutObjectRetention (query_opt::version_id),
				TAGGING => PutObjectTagging (query_opt::version_id),

			],
			no_key: [
				EMPTY => CreateBucket,
				ACCELERATE => PutBucketAccelerateConfiguration,
				ACL => PutBucketAcl,
				ANALYTICS => PutBucketAnalyticsConfiguration (query::id),
				CORS => PutBucketCors,
				ENCRYPTION => PutBucketEncryption,
				INTELLIGENT_TIERING => PutBucketIntelligentTieringConfiguration(query::id),
				INVENTORY => PutBucketInventoryConfiguration(query::id),
				LIFECYCLE => PutBucketLifecycleConfiguration,
				LOGGING => PutBucketLogging,
				METRICS => PutBucketMetricsConfiguration(query::id),
				NOTIFICATION => PutBucketNotificationConfiguration,
				OBJECT_LOCK => PutObjectLockConfiguration,
				OWNERSHIP_CONTROLS => PutBucketOwnershipControls,
				POLICY => PutBucketPolicy,
				PUBLIC_ACCESS_BLOCK => PutPublicAccessBlock,
				REPLICATION => PutBucketReplication,
				REQUEST_PAYMENT => PutBucketRequestPayment,
				TAGGING => PutBucketTagging,
				VERSIONING => PutBucketVersioning,
				WEBSITE => PutBucketWebsite,
			]
		}
	}

	/// Determine which endpoint a request is for, knowing it is a DELETE.
	fn from_delete(key: String, query: &mut QueryParameters<'_>) -> Result<Self, Error> {
		router_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), key, query, None),
			key: [
				EMPTY if upload_id => AbortMultipartUpload (query::upload_id),
				EMPTY => DeleteObject (query_opt::version_id),
				TAGGING => DeleteObjectTagging (query_opt::version_id),
			],
			no_key: [
				EMPTY => DeleteBucket,
				ANALYTICS => DeleteBucketAnalyticsConfiguration (query::id),
				CORS => DeleteBucketCors,
				ENCRYPTION => DeleteBucketEncryption,
				INTELLIGENT_TIERING => DeleteBucketIntelligentTieringConfiguration (query::id),
				INVENTORY => DeleteBucketInventoryConfiguration (query::id),
				LIFECYCLE => DeleteBucketLifecycle,
				METRICS => DeleteBucketMetricsConfiguration (query::id),
				OWNERSHIP_CONTROLS => DeleteBucketOwnershipControls,
				POLICY => DeleteBucketPolicy,
				PUBLIC_ACCESS_BLOCK => DeletePublicAccessBlock,
				REPLICATION => DeleteBucketReplication,
				TAGGING => DeleteBucketTagging,
				WEBSITE => DeleteBucketWebsite,
			]
		}
	}

	/// Get the key the request target. Returns None for requests which don't use a key.
	#[allow(dead_code)]
	pub fn get_key(&self) -> Option<&str> {
		router_match! {
			@extract
			self,
			key,
			[
				AbortMultipartUpload,
				CompleteMultipartUpload,
				CopyObject,
				CreateMultipartUpload,
				DeleteObject,
				DeleteObjectTagging,
				GetObject,
				GetObjectAcl,
				GetObjectLegalHold,
				GetObjectRetention,
				GetObjectTagging,
				GetObjectTorrent,
				HeadObject,
				ListParts,
				PutObject,
				PutObjectAcl,
				PutObjectLegalHold,
				PutObjectRetention,
				PutObjectTagging,
				RestoreObject,
				SelectObjectContent,
				UploadPart,
				UploadPartCopy,
			]
		}
	}

	/// Get the kind of authorization which is required to perform the operation.
	pub fn authorization_type(&self) -> Authorization {
		if let Endpoint::ListBuckets = self {
			return Authorization::None;
		};
		let readonly = router_match! {
			@match
			self,
			[
				GetBucketAccelerateConfiguration,
				GetBucketAcl,
				GetBucketAnalyticsConfiguration,
				GetBucketEncryption,
				GetBucketIntelligentTieringConfiguration,
				GetBucketInventoryConfiguration,
				GetBucketLifecycleConfiguration,
				GetBucketLocation,
				GetBucketLogging,
				GetBucketMetricsConfiguration,
				GetBucketNotificationConfiguration,
				GetBucketOwnershipControls,
				GetBucketPolicy,
				GetBucketPolicyStatus,
				GetBucketReplication,
				GetBucketRequestPayment,
				GetBucketTagging,
				GetBucketVersioning,
				GetObject,
				GetObjectAcl,
				GetObjectLegalHold,
				GetObjectLockConfiguration,
				GetObjectRetention,
				GetObjectTagging,
				GetObjectTorrent,
				GetPublicAccessBlock,
				HeadBucket,
				HeadObject,
				ListBucketAnalyticsConfigurations,
				ListBucketIntelligentTieringConfigurations,
				ListBucketInventoryConfigurations,
				ListBucketMetricsConfigurations,
				ListMultipartUploads,
				ListObjects,
				ListObjectsV2,
				ListObjectVersions,
				ListParts,
				SelectObjectContent,
			]
		};
		let owner = router_match! {
			@match
			self,
			[
				DeleteBucket,
				GetBucketWebsite,
				PutBucketWebsite,
				DeleteBucketWebsite,
				GetBucketCors,
				PutBucketCors,
				DeleteBucketCors,
			]
		};
		if readonly {
			Authorization::Read
		} else if owner {
			Authorization::Owner
		} else {
			Authorization::Write
		}
	}
}

// parameter name => struct field
generateQueryParameters! {
	"continuation-token" => continuation_token,
	"delimiter" => delimiter,
	"encoding-type" => encoding_type,
	"fetch-owner" => fetch_owner,
	"id" => id,
	"key-marker" => key_marker,
	"list-type" => list_type,
	"marker" => marker,
	"max-keys" => max_keys,
	"max-parts" => max_parts,
	"max-uploads" => max_uploads,
	"partNumber" => part_number,
	"part-number-marker" => part_number_marker,
	"prefix" => prefix,
	"select-type" => select_type,
	"start-after" => start_after,
	"uploadId" => upload_id,
	"upload-id-marker" => upload_id_marker,
	"versionId" => version_id,
	"version-id-marker" => version_id_marker
}

mod keywords {
	//! This module contain all query parameters with no associated value S3 uses to differentiate
	//! endpoints.
	pub const EMPTY: &str = "";

	pub const ACCELERATE: &str = "accelerate";
	pub const ACL: &str = "acl";
	pub const ANALYTICS: &str = "analytics";
	pub const CORS: &str = "cors";
	pub const DELETE: &str = "delete";
	pub const ENCRYPTION: &str = "encryption";
	pub const INTELLIGENT_TIERING: &str = "intelligent-tiering";
	pub const INVENTORY: &str = "inventory";
	pub const LEGAL_HOLD: &str = "legal-hold";
	pub const LIFECYCLE: &str = "lifecycle";
	pub const LOCATION: &str = "location";
	pub const LOGGING: &str = "logging";
	pub const METRICS: &str = "metrics";
	pub const NOTIFICATION: &str = "notification";
	pub const OBJECT_LOCK: &str = "object-lock";
	pub const OWNERSHIP_CONTROLS: &str = "ownershipControls";
	pub const POLICY: &str = "policy";
	pub const POLICY_STATUS: &str = "policyStatus";
	pub const PUBLIC_ACCESS_BLOCK: &str = "publicAccessBlock";
	pub const REPLICATION: &str = "replication";
	pub const REQUEST_PAYMENT: &str = "requestPayment";
	pub const RESTORE: &str = "restore";
	pub const RETENTION: &str = "retention";
	pub const SELECT: &str = "select";
	pub const TAGGING: &str = "tagging";
	pub const TORRENT: &str = "torrent";
	pub const UPLOADS: &str = "uploads";
	pub const VERSIONING: &str = "versioning";
	pub const VERSIONS: &str = "versions";
	pub const WEBSITE: &str = "website";
}

#[cfg(test)]
mod tests {
	use super::*;

	fn parse(
		method: &str,
		uri: &str,
		bucket: Option<String>,
		header: Option<(&str, &str)>,
	) -> (Endpoint, Option<String>) {
		let mut req = Request::builder().method(method).uri(uri);
		if let Some((k, v)) = header {
			req = req.header(k, v)
		}
		let req = req.body(()).unwrap();

		Endpoint::from_request(&req, bucket).unwrap()
	}

	macro_rules! test_cases {
        ($($method:ident $uri:expr => $variant:ident )*) => {{
            $(
            assert!(
                matches!(
                    parse(test_cases!{@actual_method $method}, $uri, Some("my_bucket".to_owned()), None).0,
                    Endpoint::$variant { .. }
                )
            );
            assert!(
                matches!(
                    parse(test_cases!{@actual_method $method}, concat!("/my_bucket", $uri), None, None).0,
                    Endpoint::$variant { .. }
                )
            );

            test_cases!{@auth $method $uri}
            )*
        }};

        (@actual_method HEAD) => {{ "HEAD" }};
        (@actual_method GET) => {{ "GET" }};
        (@actual_method OWNER_GET) => {{ "GET" }};
        (@actual_method PUT) => {{ "PUT" }};
        (@actual_method OWNER_PUT) => {{ "PUT" }};
        (@actual_method POST) => {{ "POST" }};
        (@actual_method DELETE) => {{ "DELETE" }};
        (@actual_method OWNER_DELETE) => {{ "DELETE" }};

        (@auth HEAD $uri:expr) => {{
            assert_eq!(parse("HEAD", concat!("/my_bucket", $uri), None, None).0.authorization_type(),
                Authorization::Read)
        }};
        (@auth GET $uri:expr) => {{
            assert_eq!(parse("GET", concat!("/my_bucket", $uri), None, None).0.authorization_type(),
                Authorization::Read)
        }};
        (@auth OWNER_GET $uri:expr) => {{
            assert_eq!(parse("GET", concat!("/my_bucket", $uri), None, None).0.authorization_type(),
                Authorization::Owner)
        }};
        (@auth PUT $uri:expr) => {{
            assert_eq!(parse("PUT", concat!("/my_bucket", $uri), None, None).0.authorization_type(),
                Authorization::Write)
        }};
        (@auth OWNER_PUT $uri:expr) => {{
            assert_eq!(parse("PUT", concat!("/my_bucket", $uri), None, None).0.authorization_type(),
                Authorization::Owner)
        }};
        (@auth POST $uri:expr) => {{
            assert_eq!(parse("POST", concat!("/my_bucket", $uri), None, None).0.authorization_type(),
                Authorization::Write)
        }};
        (@auth DELETE $uri:expr) => {{
            assert_eq!(parse("DELETE", concat!("/my_bucket", $uri), None, None).0.authorization_type(),
                Authorization::Write)
        }};
        (@auth OWNER_DELETE $uri:expr) => {{
            assert_eq!(parse("DELETE", concat!("/my_bucket", $uri), None, None).0.authorization_type(),
                Authorization::Owner)
        }};
    }

	#[test]
	fn test_bucket_extraction() {
		assert_eq!(
			parse("GET", "/my/key", Some("my_bucket".to_owned()), None).1,
			parse("GET", "/my_bucket/my/key", None, None).1
		);
		assert_eq!(
			parse("GET", "/my_bucket/my/key", None, None).1.unwrap(),
			"my_bucket"
		);
		assert!(parse("GET", "/", None, None).1.is_none());
	}

	#[test]
	fn test_key() {
		assert_eq!(
			parse("GET", "/my/key", Some("my_bucket".to_owned()), None)
				.0
				.get_key(),
			parse("GET", "/my_bucket/my/key", None, None).0.get_key()
		);
		assert_eq!(
			parse("GET", "/my_bucket/my/key", None, None)
				.0
				.get_key()
				.unwrap(),
			"my/key"
		);
		assert_eq!(
			parse("GET", "/my_bucket/my/key?acl", None, None)
				.0
				.get_key()
				.unwrap(),
			"my/key"
		);
		assert!(parse("GET", "/my_bucket/?list-type=2", None, None)
			.0
			.get_key()
			.is_none());

		assert_eq!(
			parse("GET", "/my_bucket/%26%2B%3F%25%C3%A9/something", None, None)
				.0
				.get_key()
				.unwrap(),
			"&+?%é/something"
		);

		/*
		 * this case is failing. We should verify how clients encode space in url
		assert_eq!(
			parse("GET", "/my_bucket/+", None, None).get_key().unwrap(),
			" ");
		 */
	}

	#[test]
	fn invalid_endpoint() {
		let req = Request::builder()
			.method("GET")
			.uri("/bucket/key?website")
			.body(())
			.unwrap();

		assert!(Endpoint::from_request(&req, None).is_err())
	}

	#[test]
	fn test_aws_doc_examples() {
		test_cases!(
			DELETE "/example-object?uploadId=VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZ" => AbortMultipartUpload
			DELETE "/Key+?uploadId=UploadId" => AbortMultipartUpload
			POST "/example-object?uploadId=AAAsb2FkIElEIGZvciBlbHZpbmcncyWeeS1tb3ZpZS5tMnRzIRRwbG9hZA" => CompleteMultipartUpload
			POST "/Key+?uploadId=UploadId" => CompleteMultipartUpload
			PUT "/" => CreateBucket
			POST "/example-object?uploads" => CreateMultipartUpload
			POST "/{Key+}?uploads" => CreateMultipartUpload
			OWNER_DELETE "/" => DeleteBucket
			DELETE "/?analytics&id=list1" => DeleteBucketAnalyticsConfiguration
			DELETE "/?analytics&id=Id" => DeleteBucketAnalyticsConfiguration
			OWNER_DELETE "/?cors" => DeleteBucketCors
			DELETE "/?encryption" => DeleteBucketEncryption
			DELETE "/?intelligent-tiering&id=Id" => DeleteBucketIntelligentTieringConfiguration
			DELETE "/?inventory&id=list1" => DeleteBucketInventoryConfiguration
			DELETE "/?inventory&id=Id" => DeleteBucketInventoryConfiguration
			DELETE "/?lifecycle" => DeleteBucketLifecycle
			DELETE "/?metrics&id=ExampleMetrics" => DeleteBucketMetricsConfiguration
			DELETE "/?metrics&id=Id" => DeleteBucketMetricsConfiguration
			DELETE "/?ownershipControls" => DeleteBucketOwnershipControls
			DELETE "/?policy" => DeleteBucketPolicy
			DELETE "/?replication" => DeleteBucketReplication
			DELETE "/?tagging" => DeleteBucketTagging
			OWNER_DELETE "/?website" => DeleteBucketWebsite
			DELETE "/my-second-image.jpg" => DeleteObject
			DELETE "/my-third-image.jpg?versionId=UIORUnfndfiufdisojhr398493jfdkjFJjkndnqUifhnw89493jJFJ" => DeleteObject
			DELETE "/Key+?versionId=VersionId" => DeleteObject
			POST "/?delete" => DeleteObjects
			DELETE "/exampleobject?tagging" => DeleteObjectTagging
			DELETE "/{Key+}?tagging&versionId=VersionId" => DeleteObjectTagging
			DELETE "/?publicAccessBlock" => DeletePublicAccessBlock
			GET "/?accelerate" => GetBucketAccelerateConfiguration
			GET "/?acl" => GetBucketAcl
			GET "/?analytics&id=Id" => GetBucketAnalyticsConfiguration
			OWNER_GET "/?cors" => GetBucketCors
			GET "/?encryption" => GetBucketEncryption
			GET "/?intelligent-tiering&id=Id" => GetBucketIntelligentTieringConfiguration
			GET "/?inventory&id=list1" => GetBucketInventoryConfiguration
			GET "/?inventory&id=Id" => GetBucketInventoryConfiguration
			GET "/?lifecycle" => GetBucketLifecycleConfiguration
			GET "/?location" => GetBucketLocation
			GET "/?logging" => GetBucketLogging
			GET "/?metrics&id=Documents" => GetBucketMetricsConfiguration
			GET "/?metrics&id=Id" => GetBucketMetricsConfiguration
			GET "/?notification" => GetBucketNotificationConfiguration
			GET "/?ownershipControls" => GetBucketOwnershipControls
			GET "/?policy" => GetBucketPolicy
			GET "/?policyStatus" => GetBucketPolicyStatus
			GET "/?replication" => GetBucketReplication
			GET "/?requestPayment" => GetBucketRequestPayment
			GET "/?tagging" => GetBucketTagging
			GET "/?versioning" => GetBucketVersioning
			OWNER_GET "/?website" => GetBucketWebsite
			GET "/my-image.jpg" => GetObject
			GET "/myObject?versionId=3/L4kqtJlcpXroDTDmpUMLUo" => GetObject
			GET "/Junk3.txt?response-cache-control=No-cache&response-content-disposition=attachment%3B%20filename%3Dtesting.txt&response-content-encoding=x-gzip&response-content-language=mi%2C%20en&response-expires=Thu%2C%2001%20Dec%201994%2016:00:00%20GMT" => GetObject
			GET "/Key+?partNumber=1&response-cache-control=ResponseCacheControl&response-content-disposition=ResponseContentDisposition&response-content-encoding=ResponseContentEncoding&response-content-language=ResponseContentLanguage&response-content-type=ResponseContentType&response-expires=ResponseExpires&versionId=VersionId" => GetObject
			GET "/my-image.jpg?acl" => GetObjectAcl
			GET "/my-image.jpg?versionId=3/L4kqtJlcpXroDVBH40Nr8X8gdRQBpUMLUo&acl" => GetObjectAcl
			GET "/{Key+}?acl&versionId=VersionId" => GetObjectAcl
			GET "/{Key+}?legal-hold&versionId=VersionId" => GetObjectLegalHold
			GET "/?object-lock" => GetObjectLockConfiguration
			GET "/{Key+}?retention&versionId=VersionId" => GetObjectRetention
			GET "/example-object?tagging" => GetObjectTagging
			GET "/{Key+}?tagging&versionId=VersionId" => GetObjectTagging
			GET "/quotes/Nelson?torrent" => GetObjectTorrent
			GET "/{Key+}?torrent" => GetObjectTorrent
			GET "/?publicAccessBlock" => GetPublicAccessBlock
			HEAD "/" => HeadBucket
			HEAD "/my-image.jpg" => HeadObject
			HEAD "/my-image.jpg?versionId=3HL4kqCxf3vjVBH40Nrjfkd" => HeadObject
			HEAD "/Key+?partNumber=3&versionId=VersionId" => HeadObject
			GET "/?analytics" => ListBucketAnalyticsConfigurations
			GET "/?analytics&continuation-token=ContinuationToken" => ListBucketAnalyticsConfigurations
			GET "/?intelligent-tiering" => ListBucketIntelligentTieringConfigurations
			GET "/?intelligent-tiering&continuation-token=ContinuationToken" => ListBucketIntelligentTieringConfigurations
			GET "/?inventory" => ListBucketInventoryConfigurations
			GET "/?inventory&continuation-token=ContinuationToken" => ListBucketInventoryConfigurations
			GET "/?metrics" => ListBucketMetricsConfigurations
			GET "/?metrics&continuation-token=ContinuationToken" => ListBucketMetricsConfigurations
			GET "/?uploads&max-uploads=3" => ListMultipartUploads
			GET "/?uploads&delimiter=/" => ListMultipartUploads
			GET "/?uploads&delimiter=/&prefix=photos/2006/" => ListMultipartUploads
			GET "/?uploads&delimiter=D&encoding-type=EncodingType&key-marker=KeyMarker&max-uploads=1&prefix=Prefix&upload-id-marker=UploadIdMarker" => ListMultipartUploads
			GET "/" => ListObjects
			GET "/?prefix=N&marker=Ned&max-keys=40" => ListObjects
			GET "/?delimiter=/" => ListObjects
			GET "/?prefix=photos/2006/&delimiter=/" => ListObjects

			GET "/?delimiter=D&encoding-type=EncodingType&marker=Marker&max-keys=1&prefix=Prefix" => ListObjects
			GET "/?list-type=2" => ListObjectsV2
			GET "/?list-type=2&max-keys=3&prefix=E&start-after=ExampleGuide.pdf" => ListObjectsV2
			GET "/?list-type=2&delimiter=/" => ListObjectsV2
			GET "/?list-type=2&prefix=photos/2006/&delimiter=/" => ListObjectsV2
			GET "/?list-type=2" => ListObjectsV2
			GET "/?list-type=2&continuation-token=1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=" => ListObjectsV2
			GET "/?list-type=2&continuation-token=ContinuationToken&delimiter=D&encoding-type=EncodingType&fetch-owner=true&max-keys=1&prefix=Prefix&start-after=StartAfter" => ListObjectsV2
			GET "/?versions" => ListObjectVersions
			GET "/?versions&key-marker=key2" => ListObjectVersions
			GET "/?versions&key-marker=key3&version-id-marker=t46ZenlYTZBnj" => ListObjectVersions
			GET "/?versions&key-marker=key3&version-id-marker=t46Z0menlYTZBnj&max-keys=3" => ListObjectVersions
			GET "/?versions&delimiter=/" => ListObjectVersions
			GET "/?versions&prefix=photos/2006/&delimiter=/" => ListObjectVersions
			GET "/?versions&delimiter=D&encoding-type=EncodingType&key-marker=KeyMarker&max-keys=2&prefix=Prefix&version-id-marker=VersionIdMarker" => ListObjectVersions
			GET "/example-object?uploadId=XXBsb2FkIElEIGZvciBlbHZpbmcncyVcdS1tb3ZpZS5tMnRzEEEwbG9hZA&max-parts=2&part-number-marker=1" => ListParts
			GET "/Key+?max-parts=2&part-number-marker=2&uploadId=UploadId" => ListParts
			PUT "/?accelerate" => PutBucketAccelerateConfiguration
			PUT "/?acl" => PutBucketAcl
			PUT "/?analytics&id=report1" => PutBucketAnalyticsConfiguration
			PUT "/?analytics&id=Id" => PutBucketAnalyticsConfiguration
			OWNER_PUT "/?cors" => PutBucketCors
			PUT "/?encryption" => PutBucketEncryption
			PUT "/?intelligent-tiering&id=Id" => PutBucketIntelligentTieringConfiguration
			PUT "/?inventory&id=report1" => PutBucketInventoryConfiguration
			PUT "/?inventory&id=Id" => PutBucketInventoryConfiguration
			PUT "/?lifecycle" => PutBucketLifecycleConfiguration
			PUT "/?logging" => PutBucketLogging
			PUT "/?metrics&id=EntireBucket" => PutBucketMetricsConfiguration
			PUT "/?metrics&id=Id" => PutBucketMetricsConfiguration
			PUT "/?notification" => PutBucketNotificationConfiguration
			PUT "/?ownershipControls" => PutBucketOwnershipControls
			PUT "/?policy" => PutBucketPolicy
			PUT "/?replication" => PutBucketReplication
			PUT "/?requestPayment" => PutBucketRequestPayment
			PUT "/?tagging" => PutBucketTagging
			PUT "/?versioning" => PutBucketVersioning
			OWNER_PUT "/?website" => PutBucketWebsite
			PUT "/my-image.jpg" => PutObject
			PUT "/Key+" => PutObject
			PUT "/my-image.jpg?acl" => PutObjectAcl
			PUT "/my-image.jpg?acl&versionId=3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nrjfkd" => PutObjectAcl
			PUT "/{Key+}?acl&versionId=VersionId" => PutObjectAcl
			PUT "/{Key+}?legal-hold&versionId=VersionId" => PutObjectLegalHold
			PUT "/?object-lock" => PutObjectLockConfiguration
			PUT "/{Key+}?retention&versionId=VersionId" => PutObjectRetention
			PUT "/object-key?tagging" => PutObjectTagging
			PUT "/{Key+}?tagging&versionId=VersionId" => PutObjectTagging
			PUT "/?publicAccessBlock" => PutPublicAccessBlock
			POST "/object-one.csv?restore" => RestoreObject
			POST "/{Key+}?restore&versionId=VersionId" => RestoreObject
			PUT "/my-movie.m2ts?partNumber=1&uploadId=VCVsb2FkIElEIGZvciBlbZZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZR" => UploadPart
			PUT "/Key+?partNumber=2&uploadId=UploadId" => UploadPart
			POST "/" => PostObject
		);
		// no bucket, won't work with the rest of the test suite
		assert!(matches!(
			parse("GET", "/", None, None).0,
			Endpoint::ListBuckets { .. }
		));
		assert!(matches!(
			parse("GET", "/", None, None).0.authorization_type(),
			Authorization::None
		));

		// require a header
		assert!(matches!(
			parse(
				"PUT",
				"/Key+",
				Some("my_bucket".to_owned()),
				Some(("x-amz-copy-source", "some/key"))
			)
			.0,
			Endpoint::CopyObject { .. }
		));
		assert!(matches!(
			parse(
				"PUT",
				"/my_bucket/Key+",
				None,
				Some(("x-amz-copy-source", "some/key"))
			)
			.0,
			Endpoint::CopyObject { .. }
		));
		assert!(matches!(
			parse(
				"PUT",
				"/my_bucket/Key+",
				None,
				Some(("x-amz-copy-source", "some/key"))
			)
			.0
			.authorization_type(),
			Authorization::Write
		));

		// require a header
		assert!(matches!(
			parse(
				"PUT",
				"/Key+?partNumber=2&uploadId=UploadId",
				Some("my_bucket".to_owned()),
				Some(("x-amz-copy-source", "some/key"))
			)
			.0,
			Endpoint::UploadPartCopy { .. }
		));
		assert!(matches!(
			parse(
				"PUT",
				"/my_bucket/Key+?partNumber=2&uploadId=UploadId",
				None,
				Some(("x-amz-copy-source", "some/key"))
			)
			.0,
			Endpoint::UploadPartCopy { .. }
		));
		assert!(matches!(
			parse(
				"PUT",
				"/my_bucket/Key+?partNumber=2&uploadId=UploadId",
				None,
				Some(("x-amz-copy-source", "some/key"))
			)
			.0
			.authorization_type(),
			Authorization::Write
		));

		// POST request, but with GET semantic for permissions purpose
		assert!(matches!(
			parse(
				"POST",
				"/{Key+}?select&select-type=2",
				Some("my_bucket".to_owned()),
				None
			)
			.0,
			Endpoint::SelectObjectContent { .. }
		));
		assert!(matches!(
			parse("POST", "/my_bucket/{Key+}?select&select-type=2", None, None).0,
			Endpoint::SelectObjectContent { .. }
		));
		assert!(matches!(
			parse("POST", "/my_bucket/{Key+}?select&select-type=2", None, None)
				.0
				.authorization_type(),
			Authorization::Read
		));
	}
}
