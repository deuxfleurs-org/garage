.PHONY: doc all run1 run2 run3

all:
	clear
	cargo build

# ----

run1:
	RUST_LOG=garage=debug ./target/debug/garage -c tmp/config1.toml server
run1rel:
	RUST_LOG=garage=debug ./target/release/garage -c tmp/config1.toml server

run2:
	RUST_LOG=garage=debug ./target/debug/garage -c tmp/config2.toml server
run2rel:
	RUST_LOG=garage=debug ./target/release/garage -c tmp/config2.toml server

run3:
	RUST_LOG=garage=debug ./target/debug/garage -c tmp/config3.toml server
run3rel:
	RUST_LOG=garage=debug ./target/release/garage -c tmp/config3.toml server
