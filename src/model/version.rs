use std::sync::Arc;

use arc_swap::ArcSwapOption;

lazy_static::lazy_static! {
	static ref FEATURES: ArcSwapOption<&'static [&'static str]> = ArcSwapOption::new(None);
}

pub fn garage_version() -> &'static str {
	option_env!("GIT_VERSION").unwrap_or(git_version::git_version!(
		prefix = "git:",
		cargo_prefix = "cargo:",
		fallback = "unknown"
	))
}

pub fn garage_features() -> Option<&'static [&'static str]> {
	FEATURES.load().as_ref().map(|f| &f[..])
}

pub fn init_features(features: &'static [&'static str]) {
	FEATURES.store(Some(Arc::new(features)));
}
