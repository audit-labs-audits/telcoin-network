.PHONY: help check-deps lattice test integration-test lint node gateway engine purge-mac purge-linux tx1 tx2 tx3 tx4 tx5

# full path for the Makefile
ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
BASE_DIR:=$(shell basename $(ROOT_DIR))

.DEFAULT: help

help:
	@echo ;
	@echo "make check-deps" ;
	@echo "    :::> Check unused dependencies in the entire project by package." ;
	@echo "    :::> Dev needs 'cargo-udeps' installed." ;
	@echo "    :::> Dev also needs rust nightly and protobuf (on mac). ";
	@echo "    :::> To install run: 'cargo install cargo-udeps --locked'." ;
	@echo ;
	@echo "make test" ;
	@echo "    :::> Run all tests in workspace with all features using 4 threads." ;
	@echo ;
	@echo "make lattice" ;
	@echo "    :::> Run the stand-alone binary for lattice binary." ;
	@echo ;
	@echo "make lint" ;
	@echo "    :::> Format all code the project." ;
	@echo ;

# check for unused dependencies
check-deps:
	find . -type f -name Cargo.toml -exec sed -rne 's/^name = "(.*)"/\1/p' {} + | xargs -I {} sh -c "echo '\n\n{}:' && cargo +nightly udeps --package {}" ;

check:
	cargo check --all --all-features ;

# run tests
test:
	cargo test --workspace --all-features -- --test-threads 4 ;

fmt:
	cargo +nightly fmt ;

clippy:
	cargo +nightly clippy --all --all-features --fix ;

integration-tests:
	cargo test --color=always --test integration tests ;

docker-login:
	gcloud auth application-default login ;
	gcloud auth configure-docker us-docker.pkg.dev ;
	
docker-adiri:
	docker buildx build -f ./etc/Dockerfile --platform linux/amd64,linux/arm64 -t us-docker.pkg.dev/telcoin-network/tn-public/adiri . --push ;

docker-push:
	docker push us-docker.pkg.dev/telcoin-network/tn-public/adiri:latest ;

docker-builder:
	docker buildx create --name tn-builder --use ;

docker-builder-init:
	docker buildx inspect --bootstrap ;

up:
	docker compose -f ./etc/compose.yaml up --build --remove-orphans --detach ;

down:
	docker compose -f ./etc/compose.yaml down --remove-orphans -v ;

validators:
	./etc/local-testnet.sh ;

# run an http rpc server on default port
node:
	cargo run --bin telcoin-network -- node --dev --http --http.api eth,net,web3 ;
