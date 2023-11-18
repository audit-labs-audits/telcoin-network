# Telcoin Network
Consensus layer (CL) is an implemntation of Narwhal and Bullshark.
Execution layer (EL) is an implementation of rETH.

## Helpful CURL commands
### faucet request
curl http://localhost:8545 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"faucet_transfer","params":["0x6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b"],"id":1,"jsonrpc":"2.0" }' 
--data '{"method":"faucet_transfer","params":["0x86FC4954D645258e68E71de59A41066C55bd9966"],"id":1,"jsonrpc":"2.0" }' 

### get block number
curl http://localhost:8545 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"eth_blockNumber","params":[],"id":1,"jsonrpc":"2.0"}'
 
### get block by number
curl http://localhost:8545 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"eth_getBlockByNumber","params":["1"],"id":1,"jsonrpc":"2.0"}'

### get gas price
curl http://localhost:8545 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"eth_gasPrice","params":[],"id":1,"jsonrpc":"2.0"}'
 
### get balance for test wallet
curl http://localhost:8545 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"eth_getBalance","params":["0x6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b", "latest"],"id":1,"jsonrpc":"2.0"}'

### transactions pool status
curl http://localhost:8545 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"txpool_status","params":[],"id":1,"jsonrpc":"2.0"}'

### transactions content
curl http://localhost:8545 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"txpool_content","params":[],"id":1,"jsonrpc":"2.0"}'  

### transactions by hash
curl http://localhost:8545 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"eth_getTransactionByHash","params":["0x82a124595cc3793dff50bfcc3f0b6729e16c1f26316f57764e3966e303923ea7"],"id":1,"jsonrpc":"2.0"}'
