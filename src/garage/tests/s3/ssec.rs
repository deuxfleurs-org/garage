use crate::common::{self, Context};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

const SSEC_KEY: &str = "u8zCfnEyt5Imo/krN+sxA1DQXxLWtPJavU6T6gOVj1Y=";
const SSEC_KEY_MD5: &str = "jMGbs3GyZkYjJUP6q5jA7g==";
const SSEC_KEY2: &str = "XkYVk4Z3vVDO2yJaUqCAEZX6lL10voMxtV06d8my/eU=";
const SSEC_KEY2_MD5: &str = "kedo2ab8J1MCjHwJuLTJHw==";

const SZ_2MB: usize = 2 * 1024 * 1024;

#[tokio::test]
async fn test_ssec_object() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("sse-c");

	let bytes1 = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".to_vec();
	let bytes2 = (0..400000)
		.map(|x| ((x * 3792) % 256) as u8)
		.collect::<Vec<u8>>();

	for data in vec![bytes1, bytes2] {
		let stream = ByteStream::new(data.clone().into());

		// Write encrypted object
		let r = ctx
			.client
			.put_object()
			.bucket(&bucket)
			.key("testobj")
			.sse_customer_algorithm("AES256")
			.sse_customer_key(SSEC_KEY)
			.sse_customer_key_md5(SSEC_KEY_MD5)
			.body(stream)
			.send()
			.await
			.unwrap();
		assert_eq!(r.sse_customer_algorithm, Some("AES256".into()));
		assert_eq!(r.sse_customer_key_md5, Some(SSEC_KEY_MD5.into()));

		test_read_encrypted(
			&ctx,
			&bucket,
			"testobj",
			&data,
			SSEC_KEY,
			SSEC_KEY_MD5,
			SSEC_KEY2,
			SSEC_KEY2_MD5,
		)
		.await;

		// Test copy from encrypted to non-encrypted
		let r = ctx
			.client
			.copy_object()
			.bucket(&bucket)
			.key("test-copy-enc-dec")
			.copy_source(format!("{}/{}", bucket, "testobj"))
			.copy_source_sse_customer_algorithm("AES256")
			.copy_source_sse_customer_key(SSEC_KEY)
			.copy_source_sse_customer_key_md5(SSEC_KEY_MD5)
			.send()
			.await
			.unwrap();
		assert_eq!(r.sse_customer_algorithm, None);
		assert_eq!(r.sse_customer_key_md5, None);

		// Test read decrypted file
		let r = ctx
			.client
			.get_object()
			.bucket(&bucket)
			.key("test-copy-enc-dec")
			.send()
			.await
			.unwrap();
		assert_bytes_eq!(r.body, &data);
		assert_eq!(r.sse_customer_algorithm, None);
		assert_eq!(r.sse_customer_key_md5, None);

		// Test copy from non-encrypted to encrypted
		let r = ctx
			.client
			.copy_object()
			.bucket(&bucket)
			.key("test-copy-enc-dec-enc")
			.copy_source(format!("{}/test-copy-enc-dec", bucket))
			.sse_customer_algorithm("AES256")
			.sse_customer_key(SSEC_KEY2)
			.sse_customer_key_md5(SSEC_KEY2_MD5)
			.send()
			.await
			.unwrap();
		assert_eq!(r.sse_customer_algorithm, Some("AES256".into()));
		assert_eq!(r.sse_customer_key_md5, Some(SSEC_KEY2_MD5.into()));

		test_read_encrypted(
			&ctx,
			&bucket,
			"test-copy-enc-dec-enc",
			&data,
			SSEC_KEY2,
			SSEC_KEY2_MD5,
			SSEC_KEY,
			SSEC_KEY_MD5,
		)
		.await;

		// Test copy from encrypted to encrypted with different keys
		let r = ctx
			.client
			.copy_object()
			.bucket(&bucket)
			.key("test-copy-enc-enc")
			.copy_source(format!("{}/{}", bucket, "testobj"))
			.copy_source_sse_customer_algorithm("AES256")
			.copy_source_sse_customer_key(SSEC_KEY)
			.copy_source_sse_customer_key_md5(SSEC_KEY_MD5)
			.sse_customer_algorithm("AES256")
			.sse_customer_key(SSEC_KEY2)
			.sse_customer_key_md5(SSEC_KEY2_MD5)
			.send()
			.await
			.unwrap();
		assert_eq!(r.sse_customer_algorithm, Some("AES256".into()));
		assert_eq!(r.sse_customer_key_md5, Some(SSEC_KEY2_MD5.into()));
		test_read_encrypted(
			&ctx,
			&bucket,
			"test-copy-enc-enc",
			&data,
			SSEC_KEY2,
			SSEC_KEY2_MD5,
			SSEC_KEY,
			SSEC_KEY_MD5,
		)
		.await;

		// Test copy from encrypted to encrypted with the same key
		let r = ctx
			.client
			.copy_object()
			.bucket(&bucket)
			.key("test-copy-enc-enc-same")
			.copy_source(format!("{}/{}", bucket, "testobj"))
			.copy_source_sse_customer_algorithm("AES256")
			.copy_source_sse_customer_key(SSEC_KEY)
			.copy_source_sse_customer_key_md5(SSEC_KEY_MD5)
			.sse_customer_algorithm("AES256")
			.sse_customer_key(SSEC_KEY)
			.sse_customer_key_md5(SSEC_KEY_MD5)
			.send()
			.await
			.unwrap();
		assert_eq!(r.sse_customer_algorithm, Some("AES256".into()));
		assert_eq!(r.sse_customer_key_md5, Some(SSEC_KEY_MD5.into()));
		test_read_encrypted(
			&ctx,
			&bucket,
			"test-copy-enc-enc-same",
			&data,
			SSEC_KEY,
			SSEC_KEY_MD5,
			SSEC_KEY2,
			SSEC_KEY2_MD5,
		)
		.await;
	}
}

#[tokio::test]
async fn test_multipart_upload() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-ssec-mpu");

	let u1 = vec![0x11; SZ_2MB];
	let u2 = vec![0x22; SZ_2MB];
	let u3 = vec![0x33; SZ_2MB];
	let all = [&u1[..], &u2[..], &u3[..]].concat();

	// Test simple encrypted mpu
	{
		let up = ctx
			.client
			.create_multipart_upload()
			.bucket(&bucket)
			.key("a")
			.sse_customer_algorithm("AES256")
			.sse_customer_key(SSEC_KEY)
			.sse_customer_key_md5(SSEC_KEY_MD5)
			.send()
			.await
			.unwrap();
		assert!(up.upload_id.is_some());
		assert_eq!(up.sse_customer_algorithm, Some("AES256".into()));
		assert_eq!(up.sse_customer_key_md5, Some(SSEC_KEY_MD5.into()));

		let uid = up.upload_id.as_ref().unwrap();

		let mut etags = vec![];
		for (i, part) in vec![&u1, &u2, &u3].into_iter().enumerate() {
			let pu = ctx
				.client
				.upload_part()
				.bucket(&bucket)
				.key("a")
				.upload_id(uid)
				.part_number((i + 1) as i32)
				.sse_customer_algorithm("AES256")
				.sse_customer_key(SSEC_KEY)
				.sse_customer_key_md5(SSEC_KEY_MD5)
				.body(ByteStream::from(part.to_vec()))
				.send()
				.await
				.unwrap();
			etags.push(pu.e_tag.unwrap());
		}

		let mut cmp = CompletedMultipartUpload::builder();
		for (i, etag) in etags.into_iter().enumerate() {
			cmp = cmp.parts(
				CompletedPart::builder()
					.part_number((i + 1) as i32)
					.e_tag(etag)
					.build(),
			);
		}

		ctx.client
			.complete_multipart_upload()
			.bucket(&bucket)
			.key("a")
			.upload_id(uid)
			.multipart_upload(cmp.build())
			.send()
			.await
			.unwrap();

		test_read_encrypted(
			&ctx,
			&bucket,
			"a",
			&all,
			SSEC_KEY,
			SSEC_KEY_MD5,
			SSEC_KEY2,
			SSEC_KEY2_MD5,
		)
		.await;
	}

	// Test upload part copy from first object
	{
		// (setup) Upload a single part object
		ctx.client
			.put_object()
			.bucket(&bucket)
			.key("b")
			.body(ByteStream::from(u1.clone()))
			.sse_customer_algorithm("AES256")
			.sse_customer_key(SSEC_KEY2)
			.sse_customer_key_md5(SSEC_KEY2_MD5)
			.send()
			.await
			.unwrap();

		let up = ctx
			.client
			.create_multipart_upload()
			.bucket(&bucket)
			.key("target")
			.sse_customer_algorithm("AES256")
			.sse_customer_key(SSEC_KEY2)
			.sse_customer_key_md5(SSEC_KEY2_MD5)
			.send()
			.await
			.unwrap();
		let uid = up.upload_id.as_ref().unwrap();

		let p1 = ctx
			.client
			.upload_part()
			.bucket(&bucket)
			.key("target")
			.upload_id(uid)
			.part_number(1)
			.sse_customer_algorithm("AES256")
			.sse_customer_key(SSEC_KEY2)
			.sse_customer_key_md5(SSEC_KEY2_MD5)
			.body(ByteStream::from(u3.clone()))
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
			.copy_source(format!("{}/a", bucket))
			.copy_source_range("bytes=500-550000")
			.copy_source_sse_customer_algorithm("AES256")
			.copy_source_sse_customer_key(SSEC_KEY)
			.copy_source_sse_customer_key_md5(SSEC_KEY_MD5)
			.sse_customer_algorithm("AES256")
			.sse_customer_key(SSEC_KEY2)
			.sse_customer_key_md5(SSEC_KEY2_MD5)
			.send()
			.await
			.unwrap();

		let p3 = ctx
			.client
			.upload_part()
			.bucket(&bucket)
			.key("target")
			.upload_id(uid)
			.part_number(3)
			.sse_customer_algorithm("AES256")
			.sse_customer_key(SSEC_KEY2)
			.sse_customer_key_md5(SSEC_KEY2_MD5)
			.body(ByteStream::from(u2.clone()))
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
			.copy_source(format!("{}/b", bucket))
			.copy_source_range("bytes=1500-20500")
			.copy_source_sse_customer_algorithm("AES256")
			.copy_source_sse_customer_key(SSEC_KEY2)
			.copy_source_sse_customer_key_md5(SSEC_KEY2_MD5)
			.sse_customer_algorithm("AES256")
			.sse_customer_key(SSEC_KEY2)
			.sse_customer_key_md5(SSEC_KEY2_MD5)
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
		let expected = [&u3[..], &all[500..550001], &u2[..], &u1[1500..20501]].concat();
		test_read_encrypted(
			&ctx,
			&bucket,
			"target",
			&expected,
			SSEC_KEY2,
			SSEC_KEY2_MD5,
			SSEC_KEY,
			SSEC_KEY_MD5,
		)
		.await;
	}
}

async fn test_read_encrypted(
	ctx: &Context,
	bucket: &str,
	obj_key: &str,
	expected_data: &[u8],
	enc_key: &str,
	enc_key_md5: &str,
	wrong_enc_key: &str,
	wrong_enc_key_md5: &str,
) {
	// Test read encrypted without key
	let o = ctx
		.client
		.get_object()
		.bucket(bucket)
		.key(obj_key)
		.send()
		.await;
	assert!(
		o.is_err(),
		"encrypted file could be read without encryption key"
	);

	// Test read encrypted with wrong key
	let o = ctx
		.client
		.get_object()
		.bucket(bucket)
		.key(obj_key)
		.sse_customer_key(wrong_enc_key)
		.sse_customer_key_md5(wrong_enc_key_md5)
		.send()
		.await;
	assert!(
		o.is_err(),
		"encrypted file could be read with incorrect encryption key"
	);

	// Test read encrypted with correct key
	let o = ctx
		.client
		.get_object()
		.bucket(bucket)
		.key(obj_key)
		.sse_customer_algorithm("AES256")
		.sse_customer_key(enc_key)
		.sse_customer_key_md5(enc_key_md5)
		.send()
		.await
		.unwrap();
	assert_bytes_eq!(o.body, expected_data);
	assert_eq!(o.sse_customer_algorithm, Some("AES256".into()));
	assert_eq!(o.sse_customer_key_md5, Some(enc_key_md5.to_string()));
}
