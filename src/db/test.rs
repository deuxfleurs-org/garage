use crate::*;

fn test_suite(db: Db) {
	let tree = db.open_tree("tree").unwrap();

	let ka: &[u8] = &b"test"[..];
	let kb: &[u8] = &b"zwello"[..];
	let kint: &[u8] = &b"tz"[..];
	let va: &[u8] = &b"plop"[..];
	let vb: &[u8] = &b"plip"[..];
	let vc: &[u8] = &b"plup"[..];

	// ---- test simple insert/delete ----

	assert!(tree.insert(ka, va).is_ok());
	assert_eq!(tree.get(ka).unwrap().unwrap(), va);
	assert_eq!(tree.len().unwrap(), 1);

	// ---- test transaction logic ----

	let res = db.transaction::<_, (), _>(|tx| {
		assert_eq!(tx.get(&tree, ka).unwrap().unwrap(), va);

		assert_eq!(tx.insert(&tree, ka, vb).unwrap(), ());

		assert_eq!(tx.get(&tree, ka).unwrap().unwrap(), vb);

		Ok(12)
	});
	assert!(matches!(res, Ok(12)));
	assert_eq!(tree.get(ka).unwrap().unwrap(), vb);

	let res = db.transaction::<(), _, _>(|tx| {
		assert_eq!(tx.get(&tree, ka).unwrap().unwrap(), vb);

		assert_eq!(tx.insert(&tree, ka, vc).unwrap(), ());

		assert_eq!(tx.get(&tree, ka).unwrap().unwrap(), vc);

		Err(TxError::Abort(42))
	});
	assert!(matches!(res, Err(TxError::Abort(42))));
	assert_eq!(tree.get(ka).unwrap().unwrap(), vb);

	// ---- test iteration outside of transactions ----

	let mut iter = tree.iter().unwrap();
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (ka, vb));
	assert!(iter.next().is_none());
	drop(iter);

	assert!(tree.insert(kb, vc).is_ok());
	assert_eq!(tree.get(kb).unwrap().unwrap(), vc);

	let mut iter = tree.iter().unwrap();
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (ka, vb));
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (kb, vc));
	assert!(iter.next().is_none());
	drop(iter);

	let mut iter = tree.range(kint..).unwrap();
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (kb, vc));
	assert!(iter.next().is_none());
	drop(iter);

	let mut iter = tree.range_rev(..kint).unwrap();
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (ka, vb));
	assert!(iter.next().is_none());
	drop(iter);

	let mut iter = tree.iter_rev().unwrap();
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (kb, vc));
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (ka, vb));
	assert!(iter.next().is_none());
	drop(iter);

	// ---- test iteration within transactions ----

	db.transaction::<_, (), _>(|tx| {
		let mut iter = tx.iter(&tree).unwrap();
		let next = iter.next().unwrap().unwrap();
		assert_eq!((next.0.as_ref(), next.1.as_ref()), (ka, vb));
		let next = iter.next().unwrap().unwrap();
		assert_eq!((next.0.as_ref(), next.1.as_ref()), (kb, vc));
		assert!(iter.next().is_none());
		Ok(())
	})
	.unwrap();

	db.transaction::<_, (), _>(|tx| {
		let mut iter = tx.range(&tree, kint..).unwrap();
		let next = iter.next().unwrap().unwrap();
		assert_eq!((next.0.as_ref(), next.1.as_ref()), (kb, vc));
		assert!(iter.next().is_none());
		Ok(())
	})
	.unwrap();

	db.transaction::<_, (), _>(|tx| {
		let mut iter = tx.range_rev(&tree, ..kint).unwrap();
		let next = iter.next().unwrap().unwrap();
		assert_eq!((next.0.as_ref(), next.1.as_ref()), (ka, vb));
		assert!(iter.next().is_none());
		Ok(())
	})
	.unwrap();

	db.transaction::<_, (), _>(|tx| {
		let mut iter = tx.iter_rev(&tree).unwrap();
		let next = iter.next().unwrap().unwrap();
		assert_eq!((next.0.as_ref(), next.1.as_ref()), (kb, vc));
		let next = iter.next().unwrap().unwrap();
		assert_eq!((next.0.as_ref(), next.1.as_ref()), (ka, vb));
		assert!(iter.next().is_none());
		Ok(())
	})
	.unwrap();
}

#[test]
#[cfg(feature = "lmdb")]
fn test_lmdb_db() {
	use crate::lmdb_adapter::LmdbDb;

	let path = mktemp::Temp::new_dir().unwrap();
	let db = heed::EnvOpenOptions::new()
		.max_dbs(100)
		.open(&path)
		.unwrap();
	let db = LmdbDb::init(db);
	test_suite(db);
	drop(path);
}

#[test]
#[cfg(feature = "sqlite")]
fn test_sqlite_db() {
	use crate::sqlite_adapter::SqliteDb;

	let manager = r2d2_sqlite::SqliteConnectionManager::memory();
	let db = SqliteDb::new(manager, false).unwrap();
	test_suite(db);
}
