.PHONY: doc

all:
	clear; cargo build

doc:
	cd doc/book; mdbook build

release:
	RUSTFLAGS="-C link-arg=-fuse-ld=lld -C target-cpu=x86-64 -C target-feature=+sse2" cargo build --release --no-default-features
