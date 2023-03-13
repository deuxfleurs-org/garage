use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapOption};

lazy_static::lazy_static! {
	static ref VERSION: ArcSwap<&'static str> = ArcSwap::new(Arc::new(git_version::git_version!(
		prefix = "git:",
		cargo_prefix = "cargo:",
		fallback = "unknown"
	)));
	static ref FEATURES: ArcSwapOption<&'static [&'static str]> = ArcSwapOption::new(None);
}

pub fn garage_version() -> &'static str {
	&VERSION.load()
}

pub fn garage_features() -> Option<&'static [&'static str]> {
	FEATURES.load().as_ref().map(|f| &f[..])
}

pub fn init_version(version: &'static str) {
	VERSION.store(Arc::new(version));
}

pub fn init_features(features: &'static [&'static str]) {
	FEATURES.store(Some(Arc::new(features)));
}

pub fn rust_version() -> &'static str {
	env!("RUSTC_VERSION")
}
