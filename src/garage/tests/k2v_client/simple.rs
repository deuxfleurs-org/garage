use std::time::Duration;

use k2v_client::*;

use crate::common;

#[tokio::test]
async fn test_simple() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-client-simple");
	let k2v_client = ctx.k2v_client(&bucket);

	k2v_client
		.insert_item("root", "test1", b"Hello, world!".to_vec(), None)
		.await
		.unwrap();

	let res = k2v_client.read_item("root", "test1").await.unwrap();

	assert_eq!(res.value.len(), 1);
	assert_eq!(res.value[0], K2vValue::Value(b"Hello, world!".to_vec()));
}

#[tokio::test]
async fn test_special_chars() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-client-simple-special-chars");
	let k2v_client = ctx.k2v_client(&bucket);

	let (pk, sk) = ("root@plépp", "≤≤««");
	k2v_client
		.insert_item(pk, sk, b"Hello, world!".to_vec(), None)
		.await
		.unwrap();

	let res = k2v_client.read_item(pk, sk).await.unwrap();
	assert_eq!(res.value.len(), 1);
	assert_eq!(res.value[0], K2vValue::Value(b"Hello, world!".to_vec()));

	// sleep a bit before read_index
	tokio::time::sleep(Duration::from_secs(1)).await;
	let res = k2v_client.read_index(Default::default()).await.unwrap();
	assert_eq!(res.items.len(), 1);
	assert_eq!(res.items.keys().next().unwrap(), pk);

	let res = k2v_client
		.read_batch(&[BatchReadOp {
			partition_key: pk,
			filter: Default::default(),
			single_item: false,
			conflicts_only: false,
			tombstones: false,
		}])
		.await
		.unwrap();
	assert_eq!(res.len(), 1);
	let res = &res[0];
	assert_eq!(res.items.len(), 1);
	assert_eq!(res.items.keys().next().unwrap(), sk);
}
