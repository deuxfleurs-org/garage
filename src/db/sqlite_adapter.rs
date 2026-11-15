use core::ops::Bound;

use std::marker::PhantomPinned;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, RwLock};

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Rows, Statement, Transaction};

use crate::{
	open::{Engine, OpenOpt},
	Db, DbError, DbResult, Error, IDb, ITx, ITxFn, OnCommit, TxError, TxFnResult, TxOpError,
	TxResult, TxValueIter, Value, ValueIter,
};

pub use rusqlite;

// ---- top-level open function

pub(crate) fn open_db(path: &PathBuf, opt: &OpenOpt) -> DbResult<Db> {
	info!("Opening Sqlite database at: {}", path.display());
	let manager = r2d2_sqlite::SqliteConnectionManager::file(path);
	SqliteDb::open(manager, opt.fsync)
}

// ----

type Connection = r2d2::PooledConnection<SqliteConnectionManager>;

// --- err

impl From<rusqlite::Error> for DbError {
	fn from(e: rusqlite::Error) -> DbError {
		DbError(format!("Sqlite: {}", e).into())
	}
}

impl From<rusqlite::Error> for Error {
	fn from(e: rusqlite::Error) -> Error {
		DbError::from(e).into()
	}
}

impl From<r2d2::Error> for DbError {
	fn from(e: r2d2::Error) -> DbError {
		DbError(format!("Sqlite: {}", e).into())
	}
}

impl From<r2d2::Error> for Error {
	fn from(e: r2d2::Error) -> Error {
		DbError::from(e).into()
	}
}

impl From<rusqlite::Error> for TxOpError {
	fn from(e: rusqlite::Error) -> TxOpError {
		DbError::from(e).into()
	}
}

// -- db

pub struct SqliteDb {
	db: Pool<SqliteConnectionManager>,
	trees: RwLock<Vec<Arc<str>>>,
	// All operations that might write on the DB must take this lock first.
	// This emulates LMDB's approach where a single writer can be
	// active at once.
	write_lock: Mutex<()>,
}

impl SqliteDb {
	pub fn open(manager: SqliteConnectionManager, sync_mode: bool) -> DbResult<Db> {
		let manager = manager.with_init(move |db| {
			db.pragma_update(None, "journal_mode", "WAL")?;
			db.pragma_update(None, "journal_size_limit", 1024 * 1024 * 1024)?;
			if sync_mode {
				db.pragma_update(None, "synchronous", "NORMAL")?;
			} else {
				db.pragma_update(None, "synchronous", "OFF")?;
			}
			Ok(())
		});
		let s = Self {
			db: Pool::builder().build(manager)?,
			trees: RwLock::new(vec![]),
			write_lock: Mutex::new(()),
		};
		Ok(Db(Arc::new(s)))
	}
}

impl SqliteDb {
	fn get_tree(&self, i: usize) -> DbResult<Arc<str>> {
		self.trees
			.read()
			.unwrap()
			.get(i)
			.cloned()
			.ok_or_else(|| DbError("invalid tree id".into()))
	}

	fn internal_get(&self, db: &Connection, tree: &str, key: &[u8]) -> DbResult<Option<Value>> {
		let mut stmt = db.prepare(&format!("SELECT v FROM {} WHERE k = ?1", tree))?;
		let mut res_iter = stmt.query([key])?;
		match res_iter.next()? {
			None => Ok(None),
			Some(v) => Ok(Some(v.get::<_, Vec<u8>>(0)?)),
		}
	}
}

impl IDb for SqliteDb {
	fn engine(&self) -> String {
		format!("sqlite3 v{} (using rusqlite crate)", rusqlite::version())
	}

	fn open_tree(&self, name: &str) -> DbResult<usize> {
		let name = format!("tree_{}", name.replace(':', "_COLON_"));
		let mut trees = self.trees.write().unwrap();

		if let Some(i) = trees.iter().position(|x| x.as_ref() == name) {
			Ok(i)
		} else {
			let db = self.db.get()?;
			trace!("create table {}", name);
			db.execute(
				&format!(
					"CREATE TABLE IF NOT EXISTS {} (
						k BLOB PRIMARY KEY,
						v BLOB
					)",
					name
				),
				[],
			)?;
			trace!("table created: {}, unlocking", name);

			let i = trees.len();
			trees.push(name.to_string().into_boxed_str().into());
			Ok(i)
		}
	}

	fn list_trees(&self) -> DbResult<Vec<String>> {
		let mut trees = vec![];

		let db = self.db.get()?;
		let mut stmt = db.prepare(
			"SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE 'tree_%'",
		)?;
		let mut rows = stmt.query([])?;
		while let Some(row) = rows.next()? {
			let name = row.get::<_, String>(0)?;
			let name = name.replace("_COLON_", ":");
			let name = name.strip_prefix("tree_").unwrap().to_string();
			trees.push(name);
		}
		Ok(trees)
	}

	fn snapshot(&self, base_path: &Path) -> DbResult<()> {
		std::fs::create_dir_all(base_path)?;
		let path = Engine::Sqlite
			.db_path(base_path)
			.into_os_string()
			.into_string()
			.map_err(|_| DbError("invalid sqlite path string".into()))?;

		info!("Start sqlite VACUUM INTO `{}`", path);
		self.db.get()?.execute("VACUUM INTO ?1", params![path])?;
		info!("Finished sqlite VACUUM INTO `{}`", path);

		Ok(())
	}

	// ----

	fn get(&self, tree: usize, key: &[u8]) -> DbResult<Option<Value>> {
		let tree = self.get_tree(tree)?;
		self.internal_get(&self.db.get()?, &tree, key)
	}

	fn approximate_len(&self, tree: usize) -> DbResult<usize> {
		let tree = self.get_tree(tree)?;
		let db = self.db.get()?;

		let mut stmt = db.prepare(&format!("SELECT COUNT(*) FROM {}", tree))?;
		let mut res_iter = stmt.query([])?;
		match res_iter.next()? {
			None => Ok(0),
			Some(v) => Ok(v.get::<_, usize>(0)?),
		}
	}

	fn is_empty(&self, tree: usize) -> DbResult<bool> {
		Ok(self.approximate_len(tree)? == 0)
	}

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> DbResult<()> {
		let tree = self.get_tree(tree)?;
		let db = self.db.get()?;
		let lock = self.write_lock.lock();

		let old_val = self.internal_get(&db, &tree, key)?;

		let sql = match &old_val {
			Some(_) => format!("UPDATE {} SET v = ?2 WHERE k = ?1", tree),
			None => format!("INSERT INTO {} (k, v) VALUES (?1, ?2)", tree),
		};
		let n = db.execute(&sql, params![key, value])?;
		assert_eq!(n, 1);

		drop(lock);
		Ok(())
	}

	fn remove(&self, tree: usize, key: &[u8]) -> DbResult<()> {
		let tree = self.get_tree(tree)?;
		let db = self.db.get()?;
		let lock = self.write_lock.lock();

		db.execute(&format!("DELETE FROM {} WHERE k = ?1", tree), params![key])?;

		drop(lock);
		Ok(())
	}

	fn clear(&self, tree: usize) -> DbResult<()> {
		let tree = self.get_tree(tree)?;
		let db = self.db.get()?;
		let lock = self.write_lock.lock();

		db.execute(&format!("DELETE FROM {}", tree), [])?;

		drop(lock);
		Ok(())
	}

	fn iter(&self, tree: usize) -> DbResult<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let sql = format!("SELECT k, v FROM {} ORDER BY k ASC", tree);
		DbValueIterator::make(self.db.get()?, &sql, [])
	}

	fn iter_rev(&self, tree: usize) -> DbResult<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let sql = format!("SELECT k, v FROM {} ORDER BY k DESC", tree);
		DbValueIterator::make(self.db.get()?, &sql, [])
	}

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> DbResult<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;

		let (bounds_sql, params) = bounds_sql(low, high);
		let sql = format!("SELECT k, v FROM {} {} ORDER BY k ASC", tree, bounds_sql);

		let params = params
			.iter()
			.map(|x| x as &dyn rusqlite::ToSql)
			.collect::<Vec<_>>();

		DbValueIterator::make::<&[&dyn rusqlite::ToSql]>(self.db.get()?, &sql, params.as_ref())
	}
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> DbResult<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;

		let (bounds_sql, params) = bounds_sql(low, high);
		let sql = format!("SELECT k, v FROM {} {} ORDER BY k DESC", tree, bounds_sql);

		let params = params
			.iter()
			.map(|x| x as &dyn rusqlite::ToSql)
			.collect::<Vec<_>>();

		DbValueIterator::make::<&[&dyn rusqlite::ToSql]>(self.db.get()?, &sql, params.as_ref())
	}

	// ----

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<OnCommit, ()> {
		let mut db = self.db.get().map_err(Error::from).map_err(TxError::Db)?;
		let trees = self.trees.read().unwrap();
		let lock = self.write_lock.lock();

		trace!("trying transaction");
		let mut tx = SqliteTx {
			tx: db.transaction().map_err(Error::from).map_err(TxError::Db)?,
			trees: &trees,
		};
		let res = match f.try_on(&mut tx) {
			TxFnResult::Ok(on_commit) => {
				tx.tx.commit().map_err(Error::from).map_err(TxError::Db)?;
				Ok(on_commit)
			}
			TxFnResult::Abort => {
				tx.tx.rollback().map_err(Error::from).map_err(TxError::Db)?;
				Err(TxError::Abort(()))
			}
			TxFnResult::DbErr => {
				tx.tx.rollback().map_err(Error::from).map_err(TxError::Db)?;
				Err(TxError::Db(
					DbError("(this message will be discarded)".into()).into(),
				))
			}
		};

		trace!("transaction done");
		drop(lock);
		res
	}
}

// ----

struct SqliteTx<'a> {
	tx: Transaction<'a>,
	trees: &'a [Arc<str>],
}

impl<'a> SqliteTx<'a> {
	fn get_tree(&self, i: usize) -> DbResult<&'_ str> {
		self.trees.get(i).map(Arc::as_ref).ok_or_else(|| {
			DbError(
				"invalid tree id (it might have been opened after the transaction started)".into(),
			)
		})
	}

	fn internal_get(&self, tree: &str, key: &[u8]) -> DbResult<Option<Value>> {
		let mut stmt = self
			.tx
			.prepare(&format!("SELECT v FROM {} WHERE k = ?1", tree))?;
		let mut res_iter = stmt.query([key])?;
		match res_iter.next()? {
			None => Ok(None),
			Some(v) => Ok(Some(v.get::<_, Vec<u8>>(0)?)),
		}
	}
}

impl<'a> ITx for SqliteTx<'a> {
	fn get(&self, tree: usize, key: &[u8]) -> DbResult<Option<Value>> {
		let tree = self.get_tree(tree)?;
		self.internal_get(tree, key)
	}
	fn len(&self, tree: usize) -> DbResult<usize> {
		let tree = self.get_tree(tree)?;
		let mut stmt = self.tx.prepare(&format!("SELECT COUNT(*) FROM {}", tree))?;
		let mut res_iter = stmt.query([])?;
		match res_iter.next()? {
			None => Ok(0),
			Some(v) => Ok(v.get::<_, usize>(0)?),
		}
	}

	fn insert(&mut self, tree: usize, key: &[u8], value: &[u8]) -> DbResult<()> {
		let tree = self.get_tree(tree)?;
		let sql = format!("INSERT OR REPLACE INTO {} (k, v) VALUES (?1, ?2)", tree);
		self.tx.execute(&sql, params![key, value])?;
		Ok(())
	}
	fn remove(&mut self, tree: usize, key: &[u8]) -> DbResult<()> {
		let tree = self.get_tree(tree)?;
		self.tx
			.execute(&format!("DELETE FROM {} WHERE k = ?1", tree), params![key])?;
		Ok(())
	}
	fn clear(&mut self, tree: usize) -> DbResult<()> {
		let tree = self.get_tree(tree)?;
		self.tx.execute(&format!("DELETE FROM {}", tree), [])?;
		Ok(())
	}

	fn iter(&self, tree: usize) -> DbResult<TxValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let sql = format!("SELECT k, v FROM {} ORDER BY k ASC", tree);
		TxValueIterator::make(self, &sql, [])
	}
	fn iter_rev(&self, tree: usize) -> DbResult<TxValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let sql = format!("SELECT k, v FROM {} ORDER BY k DESC", tree);
		TxValueIterator::make(self, &sql, [])
	}

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> DbResult<TxValueIter<'_>> {
		let tree = self.get_tree(tree)?;

		let (bounds_sql, params) = bounds_sql(low, high);
		let sql = format!("SELECT k, v FROM {} {} ORDER BY k ASC", tree, bounds_sql);

		let params = params
			.iter()
			.map(|x| x as &dyn rusqlite::ToSql)
			.collect::<Vec<_>>();

		TxValueIterator::make::<&[&dyn rusqlite::ToSql]>(self, &sql, params.as_ref())
	}
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> DbResult<TxValueIter<'_>> {
		let tree = self.get_tree(tree)?;

		let (bounds_sql, params) = bounds_sql(low, high);
		let sql = format!("SELECT k, v FROM {} {} ORDER BY k DESC", tree, bounds_sql);

		let params = params
			.iter()
			.map(|x| x as &dyn rusqlite::ToSql)
			.collect::<Vec<_>>();

		TxValueIterator::make::<&[&dyn rusqlite::ToSql]>(self, &sql, params.as_ref())
	}
}

// ---- iterators outside transactions ----
// complicated, they must hold the Statement and Row objects
// therefore quite some unsafe code (it is a self-referential struct)

struct DbValueIterator {
	db: Connection,
	// These two are not really static (they are actually self referential :o)
	stmt: Option<Statement<'static>>,
	iter: Option<Rows<'static>>,
	_pin: PhantomPinned,
}

impl DbValueIterator {
	fn make<'res, P: rusqlite::Params>(
		db: Connection,
		sql: &str,
		args: P,
	) -> DbResult<ValueIter<'res>> {
		let res = DbValueIterator {
			db,
			stmt: None,
			iter: None,
			_pin: PhantomPinned,
		};
		let mut boxed = Box::pin(res);
		trace!("make iterator with sql: {}", sql);

		// This unsafe allows us to bypass lifetime checks
		let db = unsafe { NonNull::from(&boxed.db).as_ref() };
		let stmt = db.prepare(sql)?;

		let mut_ref = Pin::as_mut(&mut boxed);
		// This unsafe allows us to write in a field of the pinned struct
		unsafe {
			Pin::get_unchecked_mut(mut_ref).stmt = Some(stmt);
		}

		// This unsafe allows us to bypass lifetime checks
		let stmt = unsafe { NonNull::from(&boxed.stmt).as_mut() };
		let iter = stmt.as_mut().unwrap().query(args)?;

		let mut_ref = Pin::as_mut(&mut boxed);
		// This unsafe allows us to write in a field of the pinned struct
		unsafe {
			Pin::get_unchecked_mut(mut_ref).iter = Some(iter);
		}

		Ok(Box::new(DbValueIteratorPin(boxed)))
	}
}

impl Drop for DbValueIterator {
	fn drop(&mut self) {
		trace!("drop iter");
		drop(self.iter.take());
		drop(self.stmt.take());
	}
}

struct DbValueIteratorPin(Pin<Box<DbValueIterator>>);

impl Iterator for DbValueIteratorPin {
	type Item = DbResult<(Value, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		let mut_ref = Pin::as_mut(&mut self.0);
		// This unsafe allows us to mutably access the iterator field
		let next = unsafe { Pin::get_unchecked_mut(mut_ref).iter.as_mut()?.next() };
		iter_next_row(next)
	}
}

// ---- iterators within transactions ----
// it's the same except we don't hold a mutex guard,
// only a Statement and a Rows object

struct TxValueIterator<'a> {
	stmt: Statement<'a>,
	iter: Option<Rows<'a>>,
	_pin: PhantomPinned,
}

impl<'a> TxValueIterator<'a> {
	fn make<P: rusqlite::Params>(
		tx: &'a SqliteTx<'a>,
		sql: &str,
		args: P,
	) -> DbResult<TxValueIter<'a>> {
		let stmt = tx.tx.prepare(sql)?;
		let res = TxValueIterator {
			stmt,
			iter: None,
			_pin: PhantomPinned,
		};
		let mut boxed = Box::pin(res);
		trace!("make iterator with sql: {}", sql);

		// This unsafe allows us to bypass lifetime checks
		let stmt = unsafe { NonNull::from(&boxed.stmt).as_mut() };
		let iter = stmt.query(args)?;

		let mut_ref = Pin::as_mut(&mut boxed);
		// This unsafe allows us to write in a field of the pinned struct
		unsafe {
			Pin::get_unchecked_mut(mut_ref).iter = Some(iter);
		}

		Ok(Box::new(TxValueIteratorPin(boxed)))
	}
}

impl<'a> Drop for TxValueIterator<'a> {
	fn drop(&mut self) {
		trace!("drop iter");
		drop(self.iter.take());
	}
}

struct TxValueIteratorPin<'a>(Pin<Box<TxValueIterator<'a>>>);

impl<'a> Iterator for TxValueIteratorPin<'a> {
	type Item = DbResult<(Value, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		let mut_ref = Pin::as_mut(&mut self.0);
		// This unsafe allows us to mutably access the iterator field
		let next = unsafe { Pin::get_unchecked_mut(mut_ref).iter.as_mut()?.next() };
		iter_next_row(next)
	}
}

// ---- utility ----

fn bounds_sql<'r>(low: Bound<&'r [u8]>, high: Bound<&'r [u8]>) -> (String, Vec<Vec<u8>>) {
	let mut sql = String::new();
	let mut params: Vec<Vec<u8>> = vec![];

	match low {
		Bound::Included(b) => {
			sql.push_str(" WHERE k >= ?1");
			params.push(b.to_vec());
		}
		Bound::Excluded(b) => {
			sql.push_str(" WHERE k > ?1");
			params.push(b.to_vec());
		}
		Bound::Unbounded => (),
	}

	match high {
		Bound::Included(b) => {
			if !params.is_empty() {
				sql.push_str(" AND k <= ?2");
			} else {
				sql.push_str(" WHERE k <= ?1");
			}
			params.push(b.to_vec());
		}
		Bound::Excluded(b) => {
			if !params.is_empty() {
				sql.push_str(" AND k < ?2");
			} else {
				sql.push_str(" WHERE k < ?1");
			}
			params.push(b.to_vec());
		}
		Bound::Unbounded => (),
	}

	(sql, params)
}

fn iter_next_row<E>(
	next_row: rusqlite::Result<Option<&rusqlite::Row>>,
) -> Option<std::result::Result<(Value, Value), E>>
where
	E: From<rusqlite::Error>,
{
	let row = match next_row {
		Err(e) => return Some(Err(e.into())),
		Ok(None) => return None,
		Ok(Some(r)) => r,
	};
	let k = match row.get::<_, Vec<u8>>(0) {
		Err(e) => return Some(Err(e.into())),
		Ok(x) => x,
	};
	let v = match row.get::<_, Vec<u8>>(1) {
		Err(e) => return Some(Err(e.into())),
		Ok(y) => y,
	};
	Some(Ok((k, v)))
}
