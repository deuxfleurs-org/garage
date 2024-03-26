use crate::common;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart};
use base64::prelude::*;

const SZ_5MB: usize = 5 * 1024 * 1024;
const SZ_10MB: usize = 10 * 1024 * 1024;

#[tokio::test]
async fn test_multipart_upload() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("testmpu");

	let u1 = vec![0x11; SZ_5MB];
	let u2 = vec![0x22; SZ_5MB];
	let u3 = vec![0x33; SZ_5MB];
	let u4 = vec![0x44; SZ_5MB];
	let u5 = vec![0x55; SZ_5MB];

	let up = ctx
		.client
		.create_multipart_upload()
		.bucket(&bucket)
		.key("a")
		.send()
		.await
		.unwrap();
	assert!(up.upload_id.is_some());

	let uid = up.upload_id.as_ref().unwrap();

	let p3 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.part_number(3)
		.body(ByteStream::from(u3.clone()))
		.send()
		.await
		.unwrap();

	let _p1 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.part_number(1)
		.body(ByteStream::from(u1))
		.send()
		.await
		.unwrap();

	let _p4 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.part_number(4)
		.body(ByteStream::from(u4))
		.send()
		.await
		.unwrap();

	let p1bis = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.part_number(1)
		.body(ByteStream::from(u2.clone()))
		.send()
		.await
		.unwrap();

	let p6 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.part_number(6)
		.body(ByteStream::from(u5.clone()))
		.send()
		.await
		.unwrap();

	{
		let r = ctx
			.client
			.list_parts()
			.bucket(&bucket)
			.key("a")
			.upload_id(uid)
			.send()
			.await
			.unwrap();
		assert_eq!(r.parts.unwrap().len(), 4);
	}

	let cmp = CompletedMultipartUpload::builder()
		.parts(
			CompletedPart::builder()
				.part_number(1)
				.e_tag(p1bis.e_tag.unwrap())
				.build(),
		)
		.parts(
			CompletedPart::builder()
				.part_number(3)
				.e_tag(p3.e_tag.unwrap())
				.build(),
		)
		.parts(
			CompletedPart::builder()
				.part_number(6)
				.e_tag(p6.e_tag.unwrap())
				.build(),
		)
		.build();

	ctx.client
		.complete_multipart_upload()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.multipart_upload(cmp)
		.send()
		.await
		.unwrap();

	// The multipart upload must not appear anymore
	assert!(ctx
		.client
		.list_parts()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.send()
		.await
		.is_err());

	{
		// The object must appear as a regular object
		let r = ctx
			.client
			.head_object()
			.bucket(&bucket)
			.key("a")
			.send()
			.await
			.unwrap();

		assert_eq!(r.content_length.unwrap(), (SZ_5MB * 3) as i64);
	}

	{
		let o = ctx
			.client
			.get_object()
			.bucket(&bucket)
			.key("a")
			.send()
			.await
			.unwrap();

		assert_bytes_eq!(o.body, &[&u2[..], &u3[..], &u5[..]].concat());
	}

	{
		for (part_number, data) in [(1, &u2), (2, &u3), (3, &u5)] {
			let o = ctx
				.client
				.get_object()
				.bucket(&bucket)
				.key("a")
				.part_number(part_number)
				.send()
				.await
				.unwrap();

			eprintln!("get_object with part_number = {}", part_number);
			assert_eq!(o.content_length.unwrap(), SZ_5MB as i64);
			assert_bytes_eq!(o.body, data);
		}
	}
}

#[tokio::test]
async fn test_multipart_with_checksum() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("testmpu-cksum");

	let u1 = vec![0x11; SZ_5MB];
	let u2 = vec![0x22; SZ_5MB];
	let u3 = vec![0x33; SZ_5MB];

	let ck1 = calculate_sha1(&u1);
	let ck2 = calculate_sha1(&u2);
	let ck3 = calculate_sha1(&u3);

	let up = ctx
		.client
		.create_multipart_upload()
		.bucket(&bucket)
		.checksum_algorithm(ChecksumAlgorithm::Sha1)
		.key("a")
		.send()
		.await
		.unwrap();
	assert!(up.upload_id.is_some());

	let uid = up.upload_id.as_ref().unwrap();

	let p1 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.part_number(1)
		.checksum_sha1(&ck1)
		.body(ByteStream::from(u1.clone()))
		.send()
		.await
		.unwrap();

	// wrong checksum value should return an error
	let err1 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.part_number(2)
		.checksum_sha1(&ck1)
		.body(ByteStream::from(u2.clone()))
		.send()
		.await;
	assert!(err1.is_err());

	let p2 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.part_number(2)
		.checksum_sha1(&ck2)
		.body(ByteStream::from(u2))
		.send()
		.await
		.unwrap();

	let p3 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.part_number(3)
		.checksum_sha1(&ck3)
		.body(ByteStream::from(u3.clone()))
		.send()
		.await
		.unwrap();

	{
		let r = ctx
			.client
			.list_parts()
			.bucket(&bucket)
			.key("a")
			.upload_id(uid)
			.send()
			.await
			.unwrap();
		let parts = r.parts.unwrap();
		assert_eq!(parts.len(), 3);
		assert!(parts[0].checksum_crc32.is_none());
		assert!(parts[0].checksum_crc32_c.is_none());
		assert!(parts[0].checksum_sha256.is_none());
		assert_eq!(parts[0].checksum_sha1.as_deref().unwrap(), ck1);
		assert_eq!(parts[1].checksum_sha1.as_deref().unwrap(), ck2);
		assert_eq!(parts[2].checksum_sha1.as_deref().unwrap(), ck3);
	}

	let cmp = CompletedMultipartUpload::builder()
		.parts(
			CompletedPart::builder()
				.part_number(1)
				.checksum_sha1(&ck1)
				.e_tag(p1.e_tag.unwrap())
				.build(),
		)
		.parts(
			CompletedPart::builder()
				.part_number(2)
				.checksum_sha1(&ck2)
				.e_tag(p2.e_tag.unwrap())
				.build(),
		)
		.parts(
			CompletedPart::builder()
				.part_number(3)
				.checksum_sha1(&ck3)
				.e_tag(p3.e_tag.unwrap())
				.build(),
		)
		.build();

	let expected_checksum = calculate_sha1(
		&vec![
			BASE64_STANDARD.decode(&ck1).unwrap(),
			BASE64_STANDARD.decode(&ck2).unwrap(),
			BASE64_STANDARD.decode(&ck3).unwrap(),
		]
		.concat(),
	);

	let res = ctx
		.client
		.complete_multipart_upload()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.checksum_sha1(expected_checksum.clone())
		.multipart_upload(cmp)
		.send()
		.await
		.unwrap();

	assert_eq!(res.checksum_sha1, Some(expected_checksum));
}

#[tokio::test]
async fn test_uploadlistpart() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("uploadpart");

	let u1 = vec![0xee; SZ_5MB];
	let u2 = vec![0x11; SZ_5MB];

	let up = ctx
		.client
		.create_multipart_upload()
		.bucket(&bucket)
		.key("a")
		.send()
		.await
		.unwrap();
	let uid = up.upload_id.as_ref().unwrap();

	assert!(up.upload_id.is_some());

	{
		let r = ctx
			.client
			.list_parts()
			.bucket(&bucket)
			.key("a")
			.upload_id(uid)
			.send()
			.await
			.unwrap();

		assert!(r.parts.is_none());
	}

	let p1 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.part_number(2)
		.body(ByteStream::from(u1))
		.send()
		.await
		.unwrap();

	{
		// ListPart on 1st element
		let r = ctx
			.client
			.list_parts()
			.bucket(&bucket)
			.key("a")
			.upload_id(uid)
			.send()
			.await
			.unwrap();

		let ps = r.parts.unwrap();
		assert_eq!(ps.len(), 1);
		assert_eq!(ps[0].part_number.unwrap(), 2);
		let fp = &ps[0];
		assert!(fp.last_modified.is_some());
		assert_eq!(
			fp.e_tag.as_ref().unwrap(),
			"\"3366bb9dcf710d6801b5926467d02e19\""
		);
		assert_eq!(fp.size.unwrap(), SZ_5MB as i64);
	}

	let p2 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.part_number(1)
		.body(ByteStream::from(u2))
		.send()
		.await
		.unwrap();

	{
		// ListPart on the 2 elements
		let r = ctx
			.client
			.list_parts()
			.bucket(&bucket)
			.key("a")
			.upload_id(uid)
			.send()
			.await
			.unwrap();

		let ps = r.parts.unwrap();
		assert_eq!(ps.len(), 2);

		assert_eq!(ps[0].part_number.unwrap(), 1);
		let fp = &ps[0];
		assert!(fp.last_modified.is_some());
		assert_eq!(
			fp.e_tag.as_ref().unwrap(),
			"\"3c484266f9315485694556e6c693bfa2\""
		);
		assert_eq!(fp.size.unwrap(), SZ_5MB as i64);

		assert_eq!(ps[1].part_number.unwrap(), 2);
		let sp = &ps[1];
		assert!(sp.last_modified.is_some());
		assert_eq!(
			sp.e_tag.as_ref().unwrap(),
			"\"3366bb9dcf710d6801b5926467d02e19\""
		);
		assert_eq!(sp.size.unwrap(), SZ_5MB as i64);
	}

	{
		// Call pagination
		let r = ctx
			.client
			.list_parts()
			.bucket(&bucket)
			.key("a")
			.upload_id(uid)
			.max_parts(1)
			.send()
			.await
			.unwrap();

		assert!(r.part_number_marker.is_none());
		assert_eq!(r.next_part_number_marker.as_deref(), Some("1"));
		assert_eq!(r.max_parts.unwrap(), 1_i32);
		assert!(r.is_truncated.unwrap());
		assert_eq!(r.key.unwrap(), "a");
		assert_eq!(r.upload_id.unwrap().as_str(), uid.as_str());
		let parts = r.parts.unwrap();
		assert_eq!(parts.len(), 1);
		let fp = &parts[0];
		assert_eq!(fp.part_number.unwrap(), 1);
		assert_eq!(
			fp.e_tag.as_ref().unwrap(),
			"\"3c484266f9315485694556e6c693bfa2\""
		);

		let r2 = ctx
			.client
			.list_parts()
			.bucket(&bucket)
			.key("a")
			.upload_id(uid)
			.max_parts(1)
			.part_number_marker(r.next_part_number_marker.as_ref().unwrap())
			.send()
			.await
			.unwrap();

		assert_eq!(
			r2.part_number_marker.as_ref().unwrap(),
			r.next_part_number_marker.as_ref().unwrap()
		);
		assert_eq!(r2.max_parts.unwrap(), 1_i32);
		assert_eq!(r2.key.unwrap(), "a");
		assert_eq!(r2.upload_id.unwrap().as_str(), uid.as_str());
		let parts = r2.parts.unwrap();
		assert_eq!(parts.len(), 1);
		let fp = &parts[0];
		assert_eq!(fp.part_number.unwrap(), 2);
		assert_eq!(
			fp.e_tag.as_ref().unwrap(),
			"\"3366bb9dcf710d6801b5926467d02e19\""
		);
		//assert!(r2.is_truncated);   // WHY? (this was the test before)
		assert!(!r2.is_truncated.unwrap());
	}

	let cmp = CompletedMultipartUpload::builder()
		.parts(
			CompletedPart::builder()
				.part_number(1)
				.e_tag(p2.e_tag.unwrap())
				.build(),
		)
		.parts(
			CompletedPart::builder()
				.part_number(2)
				.e_tag(p1.e_tag.unwrap())
				.build(),
		)
		.build();

	ctx.client
		.complete_multipart_upload()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.multipart_upload(cmp)
		.send()
		.await
		.unwrap();

	// The multipart upload must not appear anymore
	assert!(ctx
		.client
		.list_parts()
		.bucket(&bucket)
		.key("a")
		.upload_id(uid)
		.send()
		.await
		.is_err());

	{
		// The object must appear as a regular object
		let r = ctx
			.client
			.head_object()
			.bucket(&bucket)
			.key("a")
			.send()
			.await
			.unwrap();

		assert_eq!(r.content_length.unwrap(), (SZ_5MB * 2) as i64);
	}
}

#[tokio::test]
async fn test_uploadpartcopy() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("uploadpartcopy");

	let u1 = vec![0x11; SZ_10MB];
	let u2 = vec![0x22; SZ_5MB];
	let u3 = vec![0x33; SZ_5MB];
	let u4 = vec![0x44; SZ_5MB];
	let u5 = vec![0x55; SZ_5MB];

	let overflow = 5500000 - SZ_5MB;
	let mut exp_obj = u3.clone();
	exp_obj.extend(&u4[500..]);
	exp_obj.extend(&u5[..overflow + 1]);
	exp_obj.extend(&u2);
	exp_obj.extend(&u1[500..5500000 + 1]);

	// (setup) Upload a single part object
	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("source1")
		.body(ByteStream::from(u1))
		.send()
		.await
		.unwrap();

	// (setup) Upload a multipart object with 2 parts
	{
		let up = ctx
			.client
			.create_multipart_upload()
			.bucket(&bucket)
			.key("source2")
			.send()
			.await
			.unwrap();
		let uid = up.upload_id.as_ref().unwrap();

		let p1 = ctx
			.client
			.upload_part()
			.bucket(&bucket)
			.key("source2")
			.upload_id(uid)
			.part_number(1)
			.body(ByteStream::from(u4))
			.send()
			.await
			.unwrap();

		let p2 = ctx
			.client
			.upload_part()
			.bucket(&bucket)
			.key("source2")
			.upload_id(uid)
			.part_number(2)
			.body(ByteStream::from(u5))
			.send()
			.await
			.unwrap();

		let cmp = CompletedMultipartUpload::builder()
			.parts(
				CompletedPart::builder()
					.part_number(1)
					.e_tag(p1.e_tag.unwrap())
					.build(),
			)
			.parts(
				CompletedPart::builder()
					.part_number(2)
					.e_tag(p2.e_tag.unwrap())
					.build(),
			)
			.build();

		ctx.client
			.complete_multipart_upload()
			.bucket(&bucket)
			.key("source2")
			.upload_id(uid)
			.multipart_upload(cmp)
			.send()
			.await
			.unwrap();
	}

	// Our multipart object that does copy
	let up = ctx
		.client
		.create_multipart_upload()
		.bucket(&bucket)
		.key("target")
		.send()
		.await
		.unwrap();
	let uid = up.upload_id.as_ref().unwrap();

	let p3 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("target")
		.upload_id(uid)
		.part_number(3)
		.body(ByteStream::from(u2))
		.send()
		.await
		.unwrap();

	let p1 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key("target")
		.upload_id(uid)
		.part_number(1)
		.body(ByteStream::from(u3))
		.send()
		.await
		.unwrap();

	let p2 = ctx
		.client
		.upload_part_copy()
		.bucket(&bucket)
		.key("target")
		.upload_id(uid)
		.part_number(2)
		.copy_source("uploadpartcopy/source2")
		.copy_source_range("bytes=500-5500000")
		.send()
		.await
		.unwrap();

	let p4 = ctx
		.client
		.upload_part_copy()
		.bucket(&bucket)
		.key("target")
		.upload_id(uid)
		.part_number(4)
		.copy_source("uploadpartcopy/source1")
		.copy_source_range("bytes=500-5500000")
		.send()
		.await
		.unwrap();

	let cmp = CompletedMultipartUpload::builder()
		.parts(
			CompletedPart::builder()
				.part_number(1)
				.e_tag(p1.e_tag.unwrap())
				.build(),
		)
		.parts(
			CompletedPart::builder()
				.part_number(2)
				.e_tag(p2.copy_part_result.unwrap().e_tag.unwrap())
				.build(),
		)
		.parts(
			CompletedPart::builder()
				.part_number(3)
				.e_tag(p3.e_tag.unwrap())
				.build(),
		)
		.parts(
			CompletedPart::builder()
				.part_number(4)
				.e_tag(p4.copy_part_result.unwrap().e_tag.unwrap())
				.build(),
		)
		.build();

	ctx.client
		.complete_multipart_upload()
		.bucket(&bucket)
		.key("target")
		.upload_id(uid)
		.multipart_upload(cmp)
		.send()
		.await
		.unwrap();

	// (check) Get object

	let obj = ctx
		.client
		.get_object()
		.bucket(&bucket)
		.key("target")
		.send()
		.await
		.unwrap();

	let real_obj = obj
		.body
		.collect()
		.await
		.expect("Error reading data")
		.into_bytes();

	assert_eq!(real_obj.len(), exp_obj.len());
	assert_eq!(real_obj, exp_obj);
}

fn calculate_sha1(bytes: &[u8]) -> String {
	use sha1::{Digest, Sha1};

	let mut hasher = Sha1::new();
	hasher.update(bytes);
	BASE64_STANDARD.encode(&hasher.finalize()[..])
}
