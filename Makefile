.PHONY: doc all release shell

all:
	clear; cargo build --features k2v

doc:
	cd doc/book; mdbook build

release:
	nix-build --arg release true

shell:
	nix-shell
