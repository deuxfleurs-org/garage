use std::collections::HashMap;
use std::str::FromStr;

use crate::error::{Error, OkOrMessage};
use crate::migrate::Migrate;
use crate::persister::PersisterShared;

pub struct BgVars {
	vars: HashMap<&'static str, Box<dyn BgVarTrait>>,
}

impl BgVars {
	pub fn new() -> Self {
		Self {
			vars: HashMap::new(),
		}
	}

	pub fn register_rw<V, T, GF, SF>(
		&mut self,
		p: &PersisterShared<V>,
		name: &'static str,
		get_fn: GF,
		set_fn: SF,
	) where
		V: Migrate + Default + Send + Sync,
		T: FromStr + ToString + Send + Sync + 'static,
		GF: Fn(&PersisterShared<V>) -> T + Send + Sync + 'static,
		SF: Fn(&PersisterShared<V>, T) -> Result<(), Error> + Send + Sync + 'static,
	{
		let p1 = p.clone();
		let get_fn = move || get_fn(&p1);

		let p2 = p.clone();
		let set_fn = move |v| set_fn(&p2, v);

		self.vars.insert(name, Box::new(BgVar { get_fn, set_fn }));
	}

	pub fn register_ro<V, T, GF>(&mut self, p: &PersisterShared<V>, name: &'static str, get_fn: GF)
	where
		V: Migrate + Default + Send + Sync,
		T: FromStr + ToString + Send + Sync + 'static,
		GF: Fn(&PersisterShared<V>) -> T + Send + Sync + 'static,
	{
		let p1 = p.clone();
		let get_fn = move || get_fn(&p1);

		let set_fn = move |_| Err(Error::Message(format!("Cannot set value of {}", name)));

		self.vars.insert(name, Box::new(BgVar { get_fn, set_fn }));
	}

	pub fn get(&self, var: &str) -> Result<String, Error> {
		Ok(self
			.vars
			.get(var)
			.ok_or_message("variable does not exist")?
			.get())
	}

	pub fn get_all(&self) -> Vec<(&'static str, String)> {
		self.vars.iter().map(|(k, v)| (*k, v.get())).collect()
	}

	pub fn set(&self, var: &str, val: &str) -> Result<(), Error> {
		self.vars
			.get(var)
			.ok_or_message("variable does not exist")?
			.set(val)
	}
}

impl Default for BgVars {
	fn default() -> Self {
		Self::new()
	}
}

// ----

trait BgVarTrait: Send + Sync + 'static {
	fn get(&self) -> String;
	fn set(&self, v: &str) -> Result<(), Error>;
}

struct BgVar<T, GF, SF>
where
	T: FromStr + ToString + Send + Sync + 'static,
	GF: Fn() -> T + Send + Sync + 'static,
	SF: Fn(T) -> Result<(), Error> + Sync + Send + 'static,
{
	get_fn: GF,
	set_fn: SF,
}

impl<T, GF, SF> BgVarTrait for BgVar<T, GF, SF>
where
	T: FromStr + ToString + Sync + Send + 'static,
	GF: Fn() -> T + Sync + Send + 'static,
	SF: Fn(T) -> Result<(), Error> + Sync + Send + 'static,
{
	fn get(&self) -> String {
		(self.get_fn)().to_string()
	}

	fn set(&self, vstr: &str) -> Result<(), Error> {
		let value = vstr
			.parse()
			.map_err(|_| Error::Message(format!("invalid value: {}", vstr)))?;
		(self.set_fn)(value)
	}
}
