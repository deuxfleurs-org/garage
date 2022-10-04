use crate::common;
use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::types::ByteStream;

const SZ_5MB: usize = 5 * 1024 * 1024;
const SZ_10MB: usize = 10 * 1024 * 1024;

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
		let fp = ps.iter().find(|x| x.part_number == 2).unwrap();
		assert!(fp.last_modified.is_some());
		assert_eq!(
			fp.e_tag.as_ref().unwrap(),
			"\"3366bb9dcf710d6801b5926467d02e19\""
		);
		assert_eq!(fp.size, SZ_5MB as i64);
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
		let fp = ps.iter().find(|x| x.part_number == 1).unwrap();
		assert!(fp.last_modified.is_some());
		assert_eq!(
			fp.e_tag.as_ref().unwrap(),
			"\"3c484266f9315485694556e6c693bfa2\""
		);
		assert_eq!(fp.size, SZ_5MB as i64);
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
		assert!(r.next_part_number_marker.is_some());
		assert_eq!(r.max_parts, 1_i32);
		assert!(r.is_truncated);
		assert_eq!(r.key.unwrap(), "a");
		assert_eq!(r.upload_id.unwrap().as_str(), uid.as_str());
		assert_eq!(r.parts.unwrap().len(), 1);

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
		assert_eq!(r2.max_parts, 1_i32);
		assert!(r2.is_truncated);
		assert_eq!(r2.key.unwrap(), "a");
		assert_eq!(r2.upload_id.unwrap().as_str(), uid.as_str());
		assert_eq!(r2.parts.unwrap().len(), 1);
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

		assert_eq!(r.content_length, (SZ_5MB * 2) as i64);
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
