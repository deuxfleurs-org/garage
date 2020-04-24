all:
	cargo fmt || true
	RUSTFLAGS="-C link-arg=-fuse-ld=lld" cargo build
