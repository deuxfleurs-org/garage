use std::sync::Arc;

use arc_swap::ArcSwapOption;

lazy_static::lazy_static! {
	static ref VERSION: ArcSwapOption<&'static str> = ArcSwapOption::new(None);
	static ref FEATURES: ArcSwapOption<&'static [&'static str]> = ArcSwapOption::new(None);
}

pub fn garage_version() -> &'static str {
	VERSION.load().as_ref().unwrap()
}

pub fn garage_features() -> Option<&'static [&'static str]> {
	FEATURES.load().as_ref().map(|f| &f[..])
}

pub fn init_version(version: &'static str) {
	VERSION.store(Some(Arc::new(version)));
}

pub fn init_features(features: &'static [&'static str]) {
	FEATURES.store(Some(Arc::new(features)));
}

pub fn rust_version() -> &'static str {
	env!("RUSTC_VERSION")
}
