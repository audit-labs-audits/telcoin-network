.PHONY: help udeps check test test-faucet fmt clippy docker-login docker-adiri docker-push docker-builder docker-builder-init up down validators

# full path for the Makefile
ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
BASE_DIR:=$(shell basename $(ROOT_DIR))

.DEFAULT: help

help:
	@echo ;
	@echo "make udeps ;
	@echo "    :::> Check unused dependencies in the entire project by package." ;
	@echo "    :::> Dev needs 'cargo-udeps' installed." ;
	@echo "    :::> Dev also needs rust nightly and protobuf (on mac). ";
	@echo "    :::> To install run: 'cargo install cargo-udeps --locked'." ;
	@echo ;
	@echo "make check" ;
	@echo "    :::> Cargo check workspace with all features activated." ;
	@echo ;
	@echo "make test" ;
	@echo "    :::> Run all tests in workspace with all features using 4 threads." ;
	@echo ;
	@echo "make test-faucet" ;
	@echo "    :::> Test faucet integration test in main binary." ;
	@echo ;
	@echo "make fmt" ;
	@echo "    :::> cargo +nightly fmt" ;
	@echo ;
	@echo "make clippy" ;
	@echo "    :::> Cargo +nightly clippy for all features with fix enabled." ;
	@echo ;
	@echo "make docker-login" ;
	@echo "    :::> Setup docker registry using gcloud artifacts." ;
	@echo ;
	@echo "make docker-adiri" ;
	@echo "    :::> Build telcoin-network binary and push to gcloud artifact registry with latest image tag." ;
	@echo ;
	@echo "make docker-push" ;
	@echo "    :::> Push adiri:latest image to gcloud artifact registry." ;
	@echo ;
	@echo "make docker-builder" ;
	@echo "    :::> Create docker builder for building telcoin-network binary container images." ;
	@echo ;
	@echo "make docker-builder-init" ;
	@echo "    :::> Bootstrap the docker builder for building telcoin-network binary container images." ;
	@echo ;
	@echo "make up" ;
	@echo "    :::> Launch docker compose file with 4 local validators in detached state." ;
	@echo ;
	@echo "make down" ;
	@echo "    :::> Bring the docker compose containers down and remove orphans and volumes." ;
	@echo ;
	@echo "make validators" ;
	@echo "    :::> Run 4 validators locally (outside of docker)." ;
	@echo ;

# check for unused dependencies
udeps:
	find . -type f -name Cargo.toml -exec sed -rne 's/^name = "(.*)"/\1/p' {} + | xargs -I {} sh -c "echo '\n\n{}:' && cargo +nightly udeps --package {}" ;

check:
	cargo check --workspace --all-features ;

# run tests
test:
	cargo test --workspace --all-features -- --test-threads 4 ;

test-faucet:
	cargo test --package telcoin-network --features faucet --test it ;

fmt:
	cargo +nightly fmt ;

clippy:
	cargo +nightly clippy --all --all-features --fix ;

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
