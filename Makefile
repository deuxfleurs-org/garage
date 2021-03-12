BIN=target/release/garage
DOCKER=lxpz/garage_amd64

all:
	#cargo fmt || true
	#RUSTFLAGS="-C link-arg=-fuse-ld=lld" cargo build
	clear; cargo build

$(BIN):
	#RUSTFLAGS="-C link-arg=-fuse-ld=lld" cargo build --release
	cargo build --release

$(BIN).stripped: $(BIN)
	cp $^ $@
	strip $@

docker: $(BIN).stripped
	docker build -t $(DOCKER):$(TAG) .
	docker push $(DOCKER):$(TAG)
	docker tag $(DOCKER):$(TAG) $(DOCKER):latest
	docker push $(DOCKER):latest
	
