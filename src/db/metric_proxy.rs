use std::path::PathBuf;
use std::time::Instant;

use crate::lmdb_adapter::{LmdbDb, LmdbTx};
use crate::{Bound, IDb, ITx, ITxFn, OnCommit, Result, TxResult, Value, ValueIter};
use opentelemetry::{
	global,
	metrics::{Counter, ValueRecorder},
	KeyValue,
};

pub struct MetricDbProxy {
	db: LmdbDb,
	op_counter: Counter<u64>,
	op_duration: ValueRecorder<f64>,
}

impl MetricDbProxy {
	pub fn init(db: LmdbDb) -> MetricDbProxy {
		let meter = global::meter("garage/web");
		Self {
			db,
			op_counter: meter
				.u64_counter("db.op_counter")
				.with_description("Number of operations on the local metadata engine")
				.init(),
			op_duration: meter
				.f64_value_recorder("db.op_duration")
				.with_description("Duration of operations on the local metadata engine")
				.init(),
		}
	}

	fn instrument<T>(
		&self,
		fx: impl FnOnce() -> T,
		op: &'static str,
		cat: &'static str,
		tx: &'static str,
	) -> T {
		let metric_tags = [
			KeyValue::new("op", op),
			KeyValue::new("cat", cat),
			KeyValue::new("tx", tx),
		];
		self.op_counter.add(1, &metric_tags);

		let request_start = Instant::now();
		let res = fx();
		self.op_duration.record(
			Instant::now()
				.saturating_duration_since(request_start)
				.as_secs_f64(),
			&metric_tags,
		);

		res
	}
}

impl IDb for MetricDbProxy {
	fn engine(&self) -> String {
		format!("Metric Proxy on {}", self.db.engine())
	}

	fn open_tree(&self, name: &str) -> Result<usize> {
		self.instrument(|| self.db.open_tree(name), "open_tree", "control", "no")
	}

	fn list_trees(&self) -> Result<Vec<String>> {
		self.instrument(|| self.db.list_trees(), "list_trees", "control", "no")
	}

	fn snapshot(&self, to: &PathBuf) -> Result<()> {
		self.instrument(|| self.db.snapshot(to), "snapshot", "control", "no")
	}

	// ---

	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value>> {
		self.instrument(|| self.db.get(tree, key), "get", "data", "no")
	}

	fn len(&self, tree: usize) -> Result<usize> {
		self.instrument(|| self.db.len(tree), "len", "data", "no")
	}

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<Option<Value>> {
		self.instrument(|| self.db.insert(tree, key, value), "insert", "data", "no")
	}

	fn remove(&self, tree: usize, key: &[u8]) -> Result<Option<Value>> {
		self.instrument(|| self.db.remove(tree, key), "remove", "data", "no")
	}

	fn clear(&self, tree: usize) -> Result<()> {
		self.instrument(|| self.db.clear(tree), "clear", "data", "no")
	}

	fn iter(&self, tree: usize) -> Result<ValueIter<'_>> {
		self.instrument(|| self.db.iter(tree), "iter", "data", "no")
	}

	fn iter_rev(&self, tree: usize) -> Result<ValueIter<'_>> {
		self.instrument(|| self.db.iter_rev(tree), "iter_rev", "data", "no")
	}

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		self.instrument(|| self.db.range(tree, low, high), "range", "data", "no")
	}

	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		self.instrument(
			|| self.db.range_rev(tree, low, high),
			"range_rev",
			"data",
			"no",
		)
	}

	// ----

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<OnCommit, ()> {
		self.instrument(|| self.db.transaction(f), "transaction", "control", "yes")
	}
}

struct MetricTxProxy<'a> {
	tx: LmdbTx<'a>,
}
