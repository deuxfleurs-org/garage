use crate::common;

const KEYS: [&str; 8] = ["a", "a/a", "a/b", "a/c", "a/d/a", "a/é", "b", "c"];

#[tokio::test]
async fn test_listobjectsv2() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("listobjectsv2");

	for k in KEYS {
		ctx.client
			.put_object()
			.bucket(&bucket)
			.key(k)
			.send()
			.await
			.unwrap();
	}

	{
		// Scoping the variable to avoid reusing it
		// in a following assert due to copy paste
		let r = ctx
			.client
			.list_objects_v2()
			.bucket(&bucket)
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.unwrap().len(), 8);
		assert!(r.common_prefixes.is_none());
	}

	//@FIXME aws-sdk-s3 automatically checks max-key values.
	// If we set it to zero, it drops it, and it is probably
	// the same behavior on values bigger than 1000.
	// Boto and awscli do not perform these tests, we should write
	// our own minimal library to bypass AWS SDK's tests and be
	// sure that we behave correctly.

	{
		// With 2 elements
		let r = ctx
			.client
			.list_objects_v2()
			.bucket(&bucket)
			.max_keys(2)
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.unwrap().len(), 2);
		assert!(r.common_prefixes.is_none());
		assert!(r.next_continuation_token.is_some());
	}

	{
		// With pagination
		let mut cnt = 0;
		let mut next = None;
		let last_idx = KEYS.len() - 1;

		for i in 0..KEYS.len() {
			let r = ctx
				.client
				.list_objects_v2()
				.bucket(&bucket)
				.set_continuation_token(next)
				.max_keys(1)
				.send()
				.await
				.unwrap();

			cnt += 1;
			next = r.next_continuation_token;

			assert_eq!(r.contents.unwrap().len(), 1);
			assert!(r.common_prefixes.is_none());
			if i != last_idx {
				assert!(next.is_some());
			}
		}
		assert_eq!(cnt, KEYS.len());
	}

	{
		// With a delimiter
		let r = ctx
			.client
			.list_objects_v2()
			.bucket(&bucket)
			.delimiter("/")
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.unwrap().len(), 3);
		assert_eq!(r.common_prefixes.unwrap().len(), 1);
	}

	{
		// With a delimiter and pagination
		let mut cnt_pfx = 0;
		let mut cnt_key = 0;
		let mut next = None;

		for _i in 0..KEYS.len() {
			let r = ctx
				.client
				.list_objects_v2()
				.bucket(&bucket)
				.set_continuation_token(next)
				.delimiter("/")
				.max_keys(1)
				.send()
				.await
				.unwrap();

			next = r.next_continuation_token;
			match (r.contents, r.common_prefixes) {
				(Some(k), None) if k.len() == 1 => cnt_key += 1,
				(None, Some(pfx)) if pfx.len() == 1 => cnt_pfx += 1,
				_ => unreachable!("logic error"),
			};
			if next.is_none() {
				break;
			}
		}
		assert_eq!(cnt_key, 3);
		assert_eq!(cnt_pfx, 1);
	}

	{
		// With a prefix
		let r = ctx
			.client
			.list_objects_v2()
			.bucket(&bucket)
			.prefix("a/")
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.unwrap().len(), 5);
		assert!(r.common_prefixes.is_none());
	}

	{
		// With a prefix and a delimiter
		let r = ctx
			.client
			.list_objects_v2()
			.bucket(&bucket)
			.prefix("a/")
			.delimiter("/")
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.unwrap().len(), 4);
		assert_eq!(r.common_prefixes.unwrap().len(), 1);
	}

	{
		// With a prefix, a delimiter and max_key
		let r = ctx
			.client
			.list_objects_v2()
			.bucket(&bucket)
			.prefix("a/")
			.delimiter("/")
			.max_keys(1)
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.as_ref().unwrap().len(), 1);
		assert_eq!(
			r.contents
				.unwrap()
				.first()
				.unwrap()
				.key
				.as_ref()
				.unwrap()
				.as_str(),
			"a/a"
		);
		assert!(r.common_prefixes.is_none());
	}
	{
		// With start_after before all keys
		let r = ctx
			.client
			.list_objects_v2()
			.bucket(&bucket)
			.start_after("Z")
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.unwrap().len(), 8);
		assert!(r.common_prefixes.is_none());
	}
	{
		// With start_after after all keys
		let r = ctx
			.client
			.list_objects_v2()
			.bucket(&bucket)
			.start_after("c")
			.send()
			.await
			.unwrap();

		assert!(r.contents.is_none());
		assert!(r.common_prefixes.is_none());
	}
}

#[tokio::test]
async fn test_listobjectsv1() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("listobjects");

	for k in KEYS {
		ctx.client
			.put_object()
			.bucket(&bucket)
			.key(k)
			.send()
			.await
			.unwrap();
	}

	{
		let r = ctx
			.client
			.list_objects()
			.bucket(&bucket)
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.unwrap().len(), 8);
		assert!(r.common_prefixes.is_none());
	}

	{
		// With 2 elements
		let r = ctx
			.client
			.list_objects()
			.bucket(&bucket)
			.max_keys(2)
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.unwrap().len(), 2);
		assert!(r.common_prefixes.is_none());
		assert!(r.next_marker.is_some());
	}

	{
		// With pagination
		let mut cnt = 0;
		let mut next = None;
		let last_idx = KEYS.len() - 1;

		for i in 0..KEYS.len() {
			let r = ctx
				.client
				.list_objects()
				.bucket(&bucket)
				.set_marker(next)
				.max_keys(1)
				.send()
				.await
				.unwrap();

			cnt += 1;
			next = r.next_marker;

			assert_eq!(r.contents.unwrap().len(), 1);
			assert!(r.common_prefixes.is_none());
			if i != last_idx {
				assert!(next.is_some());
			}
		}
		assert_eq!(cnt, KEYS.len());
	}

	{
		// With a delimiter
		let r = ctx
			.client
			.list_objects()
			.bucket(&bucket)
			.delimiter("/")
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.unwrap().len(), 3);
		assert_eq!(r.common_prefixes.unwrap().len(), 1);
	}

	{
		// With a delimiter and pagination
		let mut cnt_pfx = 0;
		let mut cnt_key = 0;
		let mut next = None;

		for _i in 0..KEYS.len() {
			let r = ctx
				.client
				.list_objects()
				.bucket(&bucket)
				.delimiter("/")
				.set_marker(next)
				.max_keys(1)
				.send()
				.await
				.unwrap();

			next = r.next_marker;
			match (r.contents, r.common_prefixes) {
				(Some(k), None) if k.len() == 1 => cnt_key += 1,
				(None, Some(pfx)) if pfx.len() == 1 => cnt_pfx += 1,
				_ => unreachable!("logic error"),
			};
			if next.is_none() {
				break;
			}
		}
		assert_eq!(cnt_key, 3);
		// We have no optimization to skip the whole prefix
		// on listobjectsv1 so we return the same one 5 times,
		// for each element. It is up to the client to merge its result.
		// This is compliant with AWS spec.
		assert_eq!(cnt_pfx, 5);
	}

	{
		// With a prefix
		let r = ctx
			.client
			.list_objects()
			.bucket(&bucket)
			.prefix("a/")
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.unwrap().len(), 5);
		assert!(r.common_prefixes.is_none());
	}

	{
		// With a prefix and a delimiter
		let r = ctx
			.client
			.list_objects()
			.bucket(&bucket)
			.prefix("a/")
			.delimiter("/")
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.unwrap().len(), 4);
		assert_eq!(r.common_prefixes.unwrap().len(), 1);
	}

	{
		// With a prefix, a delimiter and max_key
		let r = ctx
			.client
			.list_objects()
			.bucket(&bucket)
			.prefix("a/")
			.delimiter("/")
			.max_keys(1)
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.as_ref().unwrap().len(), 1);
		assert_eq!(
			r.contents
				.unwrap()
				.first()
				.unwrap()
				.key
				.as_ref()
				.unwrap()
				.as_str(),
			"a/a"
		);
		assert!(r.common_prefixes.is_none());
	}
	{
		// With marker before all keys
		let r = ctx
			.client
			.list_objects()
			.bucket(&bucket)
			.marker("Z")
			.send()
			.await
			.unwrap();

		assert_eq!(r.contents.unwrap().len(), 8);
		assert!(r.common_prefixes.is_none());
	}
	{
		// With start_after after all keys
		let r = ctx
			.client
			.list_objects()
			.bucket(&bucket)
			.marker("c")
			.send()
			.await
			.unwrap();

		assert!(r.contents.is_none());
		assert!(r.common_prefixes.is_none());
	}
}
