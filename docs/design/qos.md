# Quality of Service (QoS)
"for us we need to focus on QoS or high value transactions entering the mempool and how to avoid such sandwich attacks"

MEV attacks are only possible by MNO operators who construct batches.

## Exclusive Worker
Validators could agree to operate one worker each exclusively for their own MNO, QoS transactions.

MNOs would presumeably own multiple wallets. These wallets could be whitelisted to access the special worker's transaction pool. This tx pool would only accept transactions coming from whitelisted addresses. Addresses should only be whitelisted by one validator.

If base fees are calculated per worker, then these special workers with limited access can protect high value transfers from attack.

The idea is that each validator would have their own special access to submit secure transactions through batches only they have access to. Other validators would validate these batches and approve their execution.
