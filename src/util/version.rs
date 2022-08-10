pub fn garage() -> &'static str {
	option_env!("GIT_VERSION").unwrap_or(git_version::git_version!(
		prefix = "git:",
		cargo_prefix = "cargo:",
		fallback = "unknown"
	))
}
