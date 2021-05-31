use quick_xml::se::to_string;
use serde::{Serialize, Serializer};

use crate::Error as ApiError;

pub fn to_xml_with_header<T: Serialize>(x: &T) -> Result<String, ApiError> {
	let mut xml = r#"<?xml version="1.0" encoding="UTF-8"?>"#.to_string();
	xml.push_str(&to_string(x)?);
	Ok(xml)
}

fn xmlns_tag<S: Serializer>(_v: &(), s: S) -> Result<S::Ok, S::Error> {
	s.serialize_str("http://s3.amazonaws.com/doc/2006-03-01/")
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Value(#[serde(rename = "$value")] pub String);

#[derive(Debug, Serialize, PartialEq)]
pub struct IntValue(#[serde(rename = "$value")] pub i64);

#[derive(Debug, Serialize, PartialEq)]
pub struct Bucket {
	#[serde(rename = "CreationDate")]
	pub creation_date: Value,
	#[serde(rename = "Name")]
	pub name: Value,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct DisplayName(#[serde(rename = "$value")] pub String);

#[derive(Debug, Serialize, PartialEq)]
pub struct Id(#[serde(rename = "$value")] pub String);

#[derive(Debug, Serialize, PartialEq)]
pub struct Owner {
	#[serde(rename = "DisplayName")]
	pub display_name: Value,
	#[serde(rename = "ID")]
	pub id: Value,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct BucketList {
	#[serde(rename = "Bucket")]
	pub entries: Vec<Bucket>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct ListAllMyBucketsResult {
	#[serde(rename = "Buckets")]
	pub buckets: BucketList,
	#[serde(rename = "Owner")]
	pub owner: Owner,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct LocationConstraint {
	#[serde(serialize_with = "xmlns_tag")]
	pub xmlns: (),
	#[serde(rename = "$value")]
	pub region: String,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Deleted {
	#[serde(rename = "Key")]
	pub key: Value,
	#[serde(rename = "VersionId")]
	pub version_id: Value,
	#[serde(rename = "DeleteMarkerVersionId")]
	pub delete_marker_version_id: Value,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Error {
	#[serde(rename = "Code")]
	pub code: Value,
	#[serde(rename = "Message")]
	pub message: Value,
	#[serde(rename = "Resource")]
	pub resource: Option<Value>,
	#[serde(rename = "Region")]
	pub region: Option<Value>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct DeleteError {
	#[serde(rename = "Code")]
	pub code: Value,
	#[serde(rename = "Key")]
	pub key: Option<Value>,
	#[serde(rename = "Message")]
	pub message: Value,
	#[serde(rename = "VersionId")]
	pub version_id: Option<Value>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct DeleteResult {
	#[serde(serialize_with = "xmlns_tag")]
	pub xmlns: (),
	#[serde(rename = "Deleted")]
	pub deleted: Vec<Deleted>,
	#[serde(rename = "Error")]
	pub errors: Vec<DeleteError>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct CopyObjectResult {
	#[serde(rename = "LastModified")]
	pub last_modified: Value,
	#[serde(rename = "ETag")]
	pub etag: Value,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct InitiateMultipartUploadResult {
	#[serde(serialize_with = "xmlns_tag")]
	pub xmlns: (),
	#[serde(rename = "Bucket")]
	pub bucket: Value,
	#[serde(rename = "Key")]
	pub key: Value,
	#[serde(rename = "UploadId")]
	pub upload_id: Value,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct CompleteMultipartUploadResult {
	#[serde(serialize_with = "xmlns_tag")]
	pub xmlns: (),
	#[serde(rename = "Location")]
	pub location: Option<Value>,
	#[serde(rename = "Bucket")]
	pub bucket: Value,
	#[serde(rename = "Key")]
	pub key: Value,
	#[serde(rename = "ETag")]
	pub etag: Value,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct ListBucketItem {
	#[serde(rename = "Key")]
	pub key: Value,
	#[serde(rename = "LastModified")]
	pub last_modified: Value,
	#[serde(rename = "ETag")]
	pub etag: Value,
	#[serde(rename = "Size")]
	pub size: IntValue,
	#[serde(rename = "StorageClass")]
	pub storage_class: Value,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct CommonPrefix {
	#[serde(rename = "Prefix")]
	pub prefix: Value,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct ListBucketResult {
	#[serde(serialize_with = "xmlns_tag")]
	pub xmlns: (),
	#[serde(rename = "Name")]
	pub name: Value,
	#[serde(rename = "Prefix")]
	pub prefix: Value,
	#[serde(rename = "Marker")]
	pub marker: Option<Value>,
	#[serde(rename = "NextMarker")]
	pub next_marker: Option<Value>,
	#[serde(rename = "StartAfter")]
	pub start_after: Option<Value>,
	#[serde(rename = "ContinuationToken")]
	pub continuation_token: Option<Value>,
	#[serde(rename = "NextContinuationToken")]
	pub next_continuation_token: Option<Value>,
	#[serde(rename = "KeyCount")]
	pub key_count: Option<IntValue>,
	#[serde(rename = "MaxKeys")]
	pub max_keys: IntValue,
	#[serde(rename = "Delimiter")]
	pub delimiter: Option<Value>,
	#[serde(rename = "EncodingType")]
	pub encoding_type: Option<Value>,
	#[serde(rename = "IsTruncated")]
	pub is_truncated: Value,
	#[serde(rename = "Contents")]
	pub contents: Vec<ListBucketItem>,
	#[serde(rename = "CommonPrefixes")]
	pub common_prefixes: Vec<CommonPrefix>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct VersioningConfiguration {
	#[serde(serialize_with = "xmlns_tag")]
	pub xmlns: (),
	#[serde(rename = "Status")]
	pub status: Option<Value>,
}

#[cfg(test)]
mod tests {
	use super::*;

	use garage_util::time::*;

	#[test]
	fn error_message() -> Result<(), ApiError> {
		let error = Error {
			code: Value("TestError".to_string()),
			message: Value("A dummy error message".to_string()),
			resource: Some(Value("/bucket/a/plop".to_string())),
			region: Some(Value("garage".to_string())),
		};
		assert_eq!(
			to_xml_with_header(&error)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<Error>\
	<Code>TestError</Code>\
	<Message>A dummy error message</Message>\
	<Resource>/bucket/a/plop</Resource>\
	<Region>garage</Region>\
</Error>"
		);
		Ok(())
	}

	#[test]
	fn list_all_my_buckets_result() -> Result<(), ApiError> {
		let list_buckets = ListAllMyBucketsResult {
			owner: Owner {
				display_name: Value("owner_name".to_string()),
				id: Value("qsdfjklm".to_string()),
			},
			buckets: BucketList {
				entries: vec![
					Bucket {
						creation_date: Value(msec_to_rfc3339(0)),
						name: Value("bucket_A".to_string()),
					},
					Bucket {
						creation_date: Value(msec_to_rfc3339(3600 * 24 * 1000)),
						name: Value("bucket_B".to_string()),
					},
				],
			},
		};
		assert_eq!(
			to_xml_with_header(&list_buckets)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<ListAllMyBucketsResult>\
   <Buckets>\
      <Bucket>\
         <CreationDate>1970-01-01T00:00:00.000Z</CreationDate>\
         <Name>bucket_A</Name>\
      </Bucket>\
      <Bucket>\
         <CreationDate>1970-01-02T00:00:00.000Z</CreationDate>\
         <Name>bucket_B</Name>\
      </Bucket>\
   </Buckets>\
   <Owner>\
      <DisplayName>owner_name</DisplayName>\
      <ID>qsdfjklm</ID>\
   </Owner>\
</ListAllMyBucketsResult>"
		);
		Ok(())
	}

	#[test]
	fn get_bucket_location_result() -> Result<(), ApiError> {
		let get_bucket_location = LocationConstraint {
			xmlns: (),
			region: "garage".to_string(),
		};
		assert_eq!(
			to_xml_with_header(&get_bucket_location)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">garage</LocationConstraint>"
		);
		Ok(())
	}

	#[test]
	fn get_bucket_versioning_result() -> Result<(), ApiError> {
		let get_bucket_versioning = VersioningConfiguration {
			xmlns: (),
			status: None,
		};
		assert_eq!(
			to_xml_with_header(&get_bucket_versioning)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>"
		);
		let get_bucket_versioning2 = VersioningConfiguration {
			xmlns: (),
			status: Some(Value("Suspended".to_string())),
		};
		assert_eq!(
			to_xml_with_header(&get_bucket_versioning2)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Suspended</Status></VersioningConfiguration>"
		);

		Ok(())
	}

	#[test]
	fn delete_result() -> Result<(), ApiError> {
		let delete_result = DeleteResult {
			xmlns: (),
			deleted: vec![
				Deleted {
					key: Value("a/plop".to_string()),
					version_id: Value("qsdfjklm".to_string()),
					delete_marker_version_id: Value("wxcvbn".to_string()),
				},
				Deleted {
					key: Value("b/plip".to_string()),
					version_id: Value("1234".to_string()),
					delete_marker_version_id: Value("4321".to_string()),
				},
			],
			errors: vec![
				DeleteError {
					code: Value("NotFound".to_string()),
					key: Some(Value("c/plap".to_string())),
					message: Value("Object c/plap not found".to_string()),
					version_id: None,
				},
				DeleteError {
					code: Value("Forbidden".to_string()),
					key: Some(Value("d/plep".to_string())),
					message: Value("Not authorized".to_string()),
					version_id: Some(Value("789".to_string())),
				},
			],
		};
		assert_eq!(
			to_xml_with_header(&delete_result)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
    <Deleted>\
        <Key>a/plop</Key>\
        <VersionId>qsdfjklm</VersionId>\
        <DeleteMarkerVersionId>wxcvbn</DeleteMarkerVersionId>\
    </Deleted>\
    <Deleted>\
        <Key>b/plip</Key>\
        <VersionId>1234</VersionId>\
        <DeleteMarkerVersionId>4321</DeleteMarkerVersionId>\
    </Deleted>\
    <Error>\
        <Code>NotFound</Code>\
        <Key>c/plap</Key>\
        <Message>Object c/plap not found</Message>\
    </Error>\
    <Error>\
        <Code>Forbidden</Code>\
        <Key>d/plep</Key>\
        <Message>Not authorized</Message>\
        <VersionId>789</VersionId>\
    </Error>\
</DeleteResult>"
		);
		Ok(())
	}

	#[test]
	fn copy_object_result() -> Result<(), ApiError> {
		let copy_result = CopyObjectResult {
			last_modified: Value(msec_to_rfc3339(0)),
			etag: Value("9b2cf535f27731c974343645a3985328".to_string()),
		};
		assert_eq!(
			to_xml_with_header(&copy_result)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<CopyObjectResult>\
    <LastModified>1970-01-01T00:00:00.000Z</LastModified>\
    <ETag>9b2cf535f27731c974343645a3985328</ETag>\
</CopyObjectResult>\
			"
		);
		Ok(())
	}

	#[test]
	fn initiate_multipart_upload_result() -> Result<(), ApiError> {
		let result = InitiateMultipartUploadResult {
			xmlns: (),
			bucket: Value("mybucket".to_string()),
			key: Value("a/plop".to_string()),
			upload_id: Value("azerty".to_string()),
		};
		assert_eq!(
			to_xml_with_header(&result)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
	<Bucket>mybucket</Bucket>\
	<Key>a/plop</Key>\
	<UploadId>azerty</UploadId>\
</InitiateMultipartUploadResult>"
		);
		Ok(())
	}

	#[test]
	fn complete_multipart_upload_result() -> Result<(), ApiError> {
		let result = CompleteMultipartUploadResult {
			xmlns: (),
			location: Some(Value("https://garage.tld/mybucket/a/plop".to_string())),
			bucket: Value("mybucket".to_string()),
			key: Value("a/plop".to_string()),
			etag: Value("3858f62230ac3c915f300c664312c11f-9".to_string()),
		};
		assert_eq!(
			to_xml_with_header(&result)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
	<Location>https://garage.tld/mybucket/a/plop</Location>\
	<Bucket>mybucket</Bucket>\
	<Key>a/plop</Key>\
	<ETag>3858f62230ac3c915f300c664312c11f-9</ETag>\
</CompleteMultipartUploadResult>"
		);
		Ok(())
	}

	#[test]
	fn list_objects_v1_1() -> Result<(), ApiError> {
		let result = ListBucketResult {
			xmlns: (),
			name: Value("example-bucket".to_string()),
			prefix: Value("".to_string()),
			marker: Some(Value("".to_string())),
			next_marker: None,
			start_after: None,
			continuation_token: None,
			next_continuation_token: None,
			key_count: None,
			max_keys: IntValue(1000),
			encoding_type: None,
			delimiter: Some(Value("/".to_string())),
			is_truncated: Value("false".to_string()),
			contents: vec![ListBucketItem {
				key: Value("sample.jpg".to_string()),
				last_modified: Value(msec_to_rfc3339(0)),
				etag: Value("bf1d737a4d46a19f3bced6905cc8b902".to_string()),
				size: IntValue(142863),
				storage_class: Value("STANDARD".to_string()),
			}],
			common_prefixes: vec![CommonPrefix {
				prefix: Value("photos/".to_string()),
			}],
		};
		assert_eq!(
			to_xml_with_header(&result)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
  <Name>example-bucket</Name>\
  <Prefix></Prefix>\
  <Marker></Marker>\
  <MaxKeys>1000</MaxKeys>\
  <Delimiter>/</Delimiter>\
  <IsTruncated>false</IsTruncated>\
  <Contents>\
    <Key>sample.jpg</Key>\
    <LastModified>1970-01-01T00:00:00.000Z</LastModified>\
    <ETag>bf1d737a4d46a19f3bced6905cc8b902</ETag>\
    <Size>142863</Size>\
    <StorageClass>STANDARD</StorageClass>\
  </Contents>\
  <CommonPrefixes>\
    <Prefix>photos/</Prefix>\
  </CommonPrefixes>\
</ListBucketResult>"
		);
		Ok(())
	}

	#[test]
	fn list_objects_v1_2() -> Result<(), ApiError> {
		let result = ListBucketResult {
			xmlns: (),
			name: Value("example-bucket".to_string()),
			prefix: Value("photos/2006/".to_string()),
			marker: Some(Value("".to_string())),
			next_marker: None,
			start_after: None,
			continuation_token: None,
			next_continuation_token: None,
			key_count: None,
			max_keys: IntValue(1000),
			delimiter: Some(Value("/".to_string())),
			encoding_type: None,
			is_truncated: Value("false".to_string()),
			contents: vec![],
			common_prefixes: vec![
				CommonPrefix {
					prefix: Value("photos/2006/February/".to_string()),
				},
				CommonPrefix {
					prefix: Value("photos/2006/January/".to_string()),
				},
			],
		};
		assert_eq!(
			to_xml_with_header(&result)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
  <Name>example-bucket</Name>\
  <Prefix>photos/2006/</Prefix>\
  <Marker></Marker>\
  <MaxKeys>1000</MaxKeys>\
  <Delimiter>/</Delimiter>\
  <IsTruncated>false</IsTruncated>\
  <CommonPrefixes>\
    <Prefix>photos/2006/February/</Prefix>\
  </CommonPrefixes>\
  <CommonPrefixes>\
    <Prefix>photos/2006/January/</Prefix>\
  </CommonPrefixes>\
</ListBucketResult>"
		);
		Ok(())
	}

	#[test]
	fn list_objects_v2_1() -> Result<(), ApiError> {
		let result = ListBucketResult {
			xmlns: (),
			name: Value("quotes".to_string()),
			prefix: Value("E".to_string()),
			marker: None,
			next_marker: None,
			start_after: Some(Value("ExampleGuide.pdf".to_string())),
			continuation_token: None,
			next_continuation_token: None,
			key_count: None,
			max_keys: IntValue(3),
			delimiter: None,
			encoding_type: None,
			is_truncated: Value("false".to_string()),
			contents: vec![ListBucketItem {
				key: Value("ExampleObject.txt".to_string()),
				last_modified: Value(msec_to_rfc3339(0)),
				etag: Value("599bab3ed2c697f1d26842727561fd94".to_string()),
				size: IntValue(857),
				storage_class: Value("REDUCED_REDUNDANCY".to_string()),
			}],
			common_prefixes: vec![],
		};
		assert_eq!(
			to_xml_with_header(&result)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
  <Name>quotes</Name>\
  <Prefix>E</Prefix>\
  <StartAfter>ExampleGuide.pdf</StartAfter>\
  <MaxKeys>3</MaxKeys>\
  <IsTruncated>false</IsTruncated>\
  <Contents>\
    <Key>ExampleObject.txt</Key>\
    <LastModified>1970-01-01T00:00:00.000Z</LastModified>\
    <ETag>599bab3ed2c697f1d26842727561fd94</ETag>\
    <Size>857</Size>\
    <StorageClass>REDUCED_REDUNDANCY</StorageClass>\
  </Contents>\
</ListBucketResult>"
		);
		Ok(())
	}

	#[test]
	fn list_objects_v2_2() -> Result<(), ApiError> {
		let result = ListBucketResult {
			xmlns: (),
			name: Value("bucket".to_string()),
			prefix: Value("".to_string()),
			marker: None,
			next_marker: None,
			start_after: None,
			continuation_token: Some(Value(
				"1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=".to_string(),
			)),
			next_continuation_token: Some(Value("qsdfjklm".to_string())),
			key_count: Some(IntValue(112)),
			max_keys: IntValue(1000),
			delimiter: None,
			encoding_type: None,
			is_truncated: Value("false".to_string()),
			contents: vec![ListBucketItem {
				key: Value("happyfacex.jpg".to_string()),
				last_modified: Value(msec_to_rfc3339(0)),
				etag: Value("70ee1738b6b21e2c8a43f3a5ab0eee71".to_string()),
				size: IntValue(1111),
				storage_class: Value("STANDARD".to_string()),
			}],
			common_prefixes: vec![],
		};
		assert_eq!(
			to_xml_with_header(&result)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
  <Name>bucket</Name>\
  <Prefix></Prefix>\
  <ContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</ContinuationToken>\
  <NextContinuationToken>qsdfjklm</NextContinuationToken>\
  <KeyCount>112</KeyCount>\
  <MaxKeys>1000</MaxKeys>\
  <IsTruncated>false</IsTruncated>\
  <Contents>\
    <Key>happyfacex.jpg</Key>\
    <LastModified>1970-01-01T00:00:00.000Z</LastModified>\
    <ETag>70ee1738b6b21e2c8a43f3a5ab0eee71</ETag>\
    <Size>1111</Size>\
    <StorageClass>STANDARD</StorageClass>\
  </Contents>\
</ListBucketResult>"
		);
		Ok(())
	}
}