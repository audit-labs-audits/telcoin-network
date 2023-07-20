.PHONY: help check-deps lattice test integration-test lint node gateway engine purge-mac purge-linux tx1 tx2 tx3 tx4 tx5

# full path for the Makefile
ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
BASE_DIR:=$(shell basename $(ROOT_DIR))

.DEFAULT: help check-deps

help:
	@echo ;
	@echo "make check-deps" ;
	@echo "    :::> Check unused dependencies in the entire project by package." ;
	@echo "    :::> Dev needs 'cargo-udeps' installed." ;
	@echo "    :::> Dev also needs rust nightly and protobuf (on mac). ";
	@echo "    :::> To install run: 'cargo install cargo-udeps --locked'." ;
	@echo ;
	@echo "make test" ;
	@echo "    :::> Run all tests." ;
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

# run tests
test:
	cargo test --all --all-features;

fmt:
	cargo +nightly fmt -- --check

clippy:
	cargo +nightly clippy --all --all-features -- -D warnings

integration-tests:
	cargo test --color=always --test integration tests ;

# format and lint
lint:
	cargo +nightly clippy --fix --all ;

# run an http rpc server on default port
node:
	cargo run --bin lattice -- node ;

# run an http rpc server on default port
execution:
	cargo run --bin lattice -- execution --dev --http --http.api eth,net,web3 ;

# purge the default lattice dir on mac
purge-mac:
	rm -rf $$HOME/Library/Application\ Support/lattice ;

# purge the default lattice dir on linux
purge-linux:
	rm -rf $$HOME/.local/share/lattice ;

# bob to learnweb3
tx1:
	curl http://127.0.0.1:8545 \
		-X POST \
		-H "Content-Type: application/json" \
		--data '{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xf86c808252088252089470c22051eeca7bd6e29cd6d24dd38fb5824aedc4891b1ae4d6e2ef50000080821473a096aae8f99e51c13545b3b75792d73b086900300c838c2c529f0e987b7950b774a019203e30d04e46ea5be16c48577541725e7080ca3b7ff7542634252494ea6d1b"],"id":1}'

# bob to alice
tx2:
	curl http://127.0.0.1:8545 \
		-X POST \
		-H "Content-Type: application/json" \
		--data '{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xf86d018252088252089486fc4954d645258e68e71de59a41066c55bd99668a021e0c0013070adc000080821474a02bd8909bc7e9247a668e4ed24ec81b93675465b67dcba80d182c121b2e1fb20ba0100b96d1df848a9d347094e59b38d12dde3d2a3ef0b28bf6dd2c15b7e7b0223a"],"id":1}'

# alice to learnweb3
tx3:
	curl http://127.0.0.1:8545 \
		-X POST \
		-H "Content-Type: application/json" \
		--data '{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xf86c808252088252089470c22051eeca7bd6e29cd6d24dd38fb5824aedc4898cf23f909c0fa0000080821474a00bbacc08385fe50f4ac52e304022b71e9139a86b817a1754a56d2c4b8238bcd7a0101e36540ffcc44df662adf21141b8e0d5e6e878cb14bbe0ac62ae8eef268095"],"id":1}'

# learnweb3 to random
tx5:
	curl http://127.0.0.1:8545 \
		-X POST \
		-H "Content-Type: application/json" \
		--data '{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xf86c808252088252089470c22051eeca7bd6e29cd6d24dd38fb5824aedc489056bc75e2d6310000080821473a0bd779dfa5a7e6a254b0716a0425dbdebf058330d5941763ee53f7a5ea978fcbaa05098f4bf1798f34670fd7592da1a81d4b04ea5b45f35c26b07b1045240e70d5b"],"id":1}'
