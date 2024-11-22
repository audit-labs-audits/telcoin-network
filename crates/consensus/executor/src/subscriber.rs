// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{errors::SubscriberResult, SubscriberError};
use consensus_metrics::spawn_logged_monitored_task;
use fastcrypto::hash::Hash;
use futures::{
    stream::{FuturesOrdered, FuturesUnordered},
    StreamExt,
};
use reth_primitives::{Address, B256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
    vec,
};
use tn_config::ConsensusConfig;
use tn_network::{
    anemo_ext::{NetworkExt, WaitingPeer},
    local::LocalNetwork,
    PrimaryToWorkerClient,
};
use tn_network_types::{ConsensusOutputRequest, FetchBlocksRequest, PrimaryToPrimaryClient};
use tn_primary::ConsensusBus;
use tn_storage::{
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks},
    traits::{Database, DbTxMut},
};
use tn_types::{
    AuthorityIdentifier, BlockHash, Certificate, CommittedSubDag, Committee, ConsensusHeader,
    ConsensusOutput, ConsensusOutputDigest, NetworkPublicKey, Noticer, Timestamp, TnReceiver,
    TnSender, WorkerBlock, WorkerCache, WorkerId,
};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// The `Subscriber` receives certificates sequenced by the consensus and waits until the
/// downloaded all the transactions references by the certificates; it then
/// forward the certificates to the Executor.
pub struct Subscriber<DB> {
    /// Receiver for shutdown
    rx_shutdown: Noticer,
    /// Used to get the sequence receiver
    consensus_bus: ConsensusBus,
    /// Consensus configuration (contains the consensus DB)
    config: ConsensusConfig<DB>,
    /// Inner state.
    inner: Arc<Inner>,
}

struct Inner {
    authority_id: AuthorityIdentifier,
    worker_cache: WorkerCache,
    committee: Committee,
    client: LocalNetwork,
    network: anemo::Network,
}

pub fn spawn_subscriber<DB: Database>(
    config: ConsensusConfig<DB>,
    rx_shutdown: Noticer,
    consensus_bus: ConsensusBus,
    last_executed_consensus_hash: B256,
    network: anemo::Network,
) -> JoinHandle<()> {
    let authority_id = config.authority().id();
    let worker_cache = config.worker_cache().clone();
    let committee = config.committee().clone();
    let client = config.local_network().clone();

    spawn_logged_monitored_task!(
        async move {
            info!(target: "telcoin::subscriber", "Starting subscriber");
            let subscriber = Subscriber {
                rx_shutdown,
                consensus_bus,
                config,
                inner: Arc::new(Inner { authority_id, committee, worker_cache, client, network }),
            };
            subscriber.run(last_executed_consensus_hash).await.expect("Failed to run subscriber")
        },
        "SubscriberTask"
    )
}

impl<DB: Database> Subscriber<DB> {
    /// Returns the max number of sub-dag to fetch payloads concurrently.
    const MAX_PENDING_PAYLOADS: usize = 1000;

    /// Write the consensus header and it's digest to number index to the consensus DB.
    ///
    /// An error here indicates a critical node failure.
    /// Note this function both logs and returns the error due to it's severity.
    fn save_consensus(&self, consensus_output: ConsensusOutput) -> SubscriberResult<()> {
        match self.config.database().write_txn() {
            Ok(mut txn) => {
                let header: ConsensusHeader = consensus_output.into();
                if let Err(e) = txn.insert::<ConsensusBlocks>(&header.number, &header) {
                    tracing::error!(target: "engine", ?e, "error saving a consensus header to persistant storage!");
                    return Err(e.into());
                }
                if let Err(e) =
                    txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)
                {
                    tracing::error!(target: "engine", ?e, "error saving a consensus header number to persistant storage!");
                    return Err(e.into());
                }
                if let Err(e) = txn.commit() {
                    tracing::error!(target: "engine", ?e, "error saving committing to persistant storage!");
                    return Err(e.into());
                }
            }
            Err(e) => {
                tracing::error!(target: "engine", ?e, "error getting a transaction on persistant storage!");
                return Err(e.into());
            }
        }
        Ok(())
    }

    /// Return the max consensus chain block number from peers.
    async fn max_consensus_number(
        clients: &mut [PrimaryToPrimaryClient<WaitingPeer>],
    ) -> Option<u64> {
        let mut waiting = FuturesUnordered::new();
        // Ask all our peers for their latest consensus height.
        let threshhold = ((clients.len() + 1) * 2) / 3;
        let num_peers = clients.len();
        for client in clients.iter_mut() {
            waiting.push(client.request_consensus(ConsensusOutputRequest::default()));
        }
        let mut responses = 0;
        let mut outputs = HashMap::new();
        // XXXX should probably have some timeouts and also try 2-3 times in case of a race
        // condition with peers.
        while let Some(res) = waiting.next().await {
            match res {
                Ok(res) => {
                    responses += 1;
                    let output = res.into_body().output;
                    println!("XXXX got response {}: {output:?}", output.digest());
                    if let Some((_, count)) = outputs.get_mut(&output.digest()) {
                        if (*count + 1) >= threshhold {
                            tracing::info!(target: "telcoin::subscriber", "reached consensus on current chain height of {} with {} out of {num_peers} peers agreeing out of {responses} responses",
                                output.number, (*count + 1));
                            return Some(output.number);
                        }
                        *count += 1;
                    } else {
                        outputs.insert(output.digest(), (output, 1_usize));
                    }
                }
                Err(e) => {
                    // An error with one peer should not derail us...  But log it.
                    tracing::error!(target: "telcoin::subscriber", "error requesting peer consensus {e:?}")
                }
            }
        }
        None
    }

    /// This function will resolve to Ok once it has caught up with the current state of the chain
    /// (synced).
    ///
    /// It will first handle any consensus output messages that were restored due to a restart
    /// (these headers are already in our DB). It will then download and apply ConsensusOutputs
    /// from the consensus chain suplied by our peers. On error the node should probably die,
    /// something has gone very wrong. On success it will return a HashSet of all the committed
    /// sub dags it executed (to avoid duplicate execution) as well as the hash and number of
    /// the last consensus block to build off of. NOTE: This is syncing the blockchain and it
    /// may take a LONG time.  It can be interupted and should restart where it left off.
    async fn catch_up(
        &self,
        last_executed_consensus_hash: B256,
    ) -> SubscriberResult<(HashSet<ConsensusOutputDigest>, BlockHash, u64)> {
        // Get the DB and load our last executed consensus block (note there may be unexecuted
        // blocks, catch up will execute them).
        let db = self.config.database();
        let last_executed_block = if let Ok(Some(number)) =
            db.get::<ConsensusBlockNumbersByDigest>(&last_executed_consensus_hash)
        {
            if let Ok(Some(block)) = db.get::<ConsensusBlocks>(&number) {
                block
            } else {
                ConsensusHeader::default()
            }
        } else {
            ConsensusHeader::default()
        };
        // Edge case, in case we don't here from peers but have un-executed blocks...
        // Not sure we should handle this, but it hurts nothing.
        let (_, last_db_block) = db
            .last_record::<ConsensusBlocks>()
            .unwrap_or_else(|| (last_executed_block.number, last_executed_block.clone()));
        // Note use last_executed_block here because
        let mut last_parent = last_executed_block.digest();
        let mut last_number = last_executed_block.number;
        let mut caught_up_subdags = HashSet::new();

        // XXXX catch up with peers...
        let mut clients: Vec<PrimaryToPrimaryClient<_>> = self
            .config
            .committee()
            .others_primaries_by_id(self.config.authority().id())
            .into_iter()
            .map(|(_, _, peer_id)| {
                PrimaryToPrimaryClient::new(
                    self.inner.network.waiting_peer(anemo::PeerId(peer_id.0.to_bytes())),
                )
            })
            .collect();
        let mut max_consensus_height =
            Self::max_consensus_number(&mut clients).await.unwrap_or(last_db_block.number);
        // Catch up to the current chain state if we need to.
        let mut last_consensus_height = last_executed_block.number;
        let clients_len = clients.len();
        println!("XXXXX catch up from {last_consensus_height} to {max_consensus_height}");
        while last_consensus_height < max_consensus_height {
            for number in last_consensus_height + 1..=max_consensus_height {
                // Check if we already have this consensus output in our local DB.
                // This will happen if we had outputs that were not applied before last shutdown
                // (for instance). This will also allow us to pre load other
                // consensus blocks as a future optimization.
                let output = if let Ok(Some(block)) = db.get::<ConsensusBlocks>(&number) {
                    block
                } else {
                    let mut try_num = 0;
                    loop {
                        // XXXX stop trying at some point...
                        // rotate through clients attempting to get the headers.
                        let client =
                            clients.get_mut((number as usize + try_num) % clients_len).unwrap();
                        let req = ConsensusOutputRequest { number: Some(number), hash: None };
                        match client.request_consensus(req).await {
                            Ok(res) => break res.into_body().output,
                            Err(e) => {
                                tracing::error!(target: "telcoin::subscriber", "error requesting peer consensus {e:?}");
                                try_num += 1;
                                continue;
                            }
                        }
                    }
                };
                println!("XXXX trying to get consensus block {number}");
                let parent_hash = last_parent;
                let number = last_number + 1;
                last_parent =
                    ConsensusHeader::digest_from_parts(parent_hash, &output.sub_dag, number);
                last_number += 1;
                if last_parent != output.digest() {
                    tracing::error!(target: "telcoin::subscriber", "failed to execute consensus!");
                    return Err(SubscriberError::UnexpectedProtocolMessage);
                }
                // XXXX should also verify that the block output is building on is in fact
                // the head of our chain.
                caught_up_subdags.insert(output.sub_dag.digest());
                let consensus_output = self.fetch_blocks(output.sub_dag, parent_hash, number).await;
                self.save_consensus(consensus_output.clone())?;
                if let Err(e) = self.consensus_bus.consensus_output().send(consensus_output).await {
                    error!(target: "telcoin::subscriber", "error broadcasting consensus output for authority {}: {}", self.inner.authority_id, e);
                    return Err(SubscriberError::ClosedChannel("consensus_output".to_string()));
                }
                last_consensus_height = number;
            }
            max_consensus_height =
                Self::max_consensus_number(&mut clients).await.unwrap_or(last_consensus_height);
        }
        Ok((caught_up_subdags, last_parent, last_number))
    }

    /// Main loop connecting to the consensus to listen to sequence messages.
    async fn run(self, last_executed_consensus_hash: B256) -> SubscriberResult<()> {
        // It's important to have the futures in ordered fashion as we want
        // to guarantee that will deliver to the executor the certificates
        // in the same order we received from rx_sequence. So it doesn't
        // matter if we somehow managed to fetch the blocks from a later
        // certificate. Unless the earlier certificate's payload has been
        // fetched, no later certificate will be delivered.
        let mut waiting = FuturesOrdered::new();

        // XXXX
        let (caught_up_subdags, mut last_parent, mut last_number) =
            self.catch_up(last_executed_consensus_hash).await?;
        // If we made it to here we whould be caught up, join into consensus now...

        let mut rx_sequence = self.consensus_bus.sequence().subscribe();
        // Listen to sequenced consensus message and process them.
        loop {
            tokio::select! {
                // Receive the ordered sequence of consensus messages from a consensus node.
                Some(sub_dag) = rx_sequence.recv(), if waiting.len() < Self::MAX_PENDING_PAYLOADS => {
                    if caught_up_subdags.contains(&sub_dag.digest()) {
                        // If we have already seen this subdag then move along.
                        continue;
                    }
                    // XXXX  sanity check sub_dag?  Maybe better to get rejections form execution below.  If
                    // we are behind then we may get sub_dags we can not execute yet...

                    // We can schedule more then MAX_PENDING_PAYLOADS payloads but
                    // don't process more consensus messages when more
                    // then MAX_PENDING_PAYLOADS is pending
                    let parent_hash = last_parent;
                    let number = last_number + 1;
                    last_parent = ConsensusHeader::digest_from_parts(parent_hash, &sub_dag, number);
                    last_number += 1;
                    waiting.push_back(self.fetch_blocks(sub_dag, parent_hash, number));
                },

                // Receive consensus messages after all transaction data is downloaded
                // then send to the execution layer for final block production.
                //
                // NOTE: this broadcasts to all subscribers, but lagging receivers will lose messages
                Some(message) = waiting.next() => {
                    self.save_consensus(message.clone())?;
                    println!("XXXX saved consensus {}", message.number);
                    if let Err(e) = self.consensus_bus.consensus_output().send(message).await {
                        error!(target: "telcoin::subscriber", "error broadcasting consensus output for authority {}: {}", self.inner.authority_id, e);
                        return Ok(());
                    }
                },

                _ = &self.rx_shutdown => {
                    return Ok(())
                }

            }

            self.consensus_bus
                .executor_metrics()
                .waiting_elements_subscriber
                .set(waiting.len() as i64);
        }
    }

    /// Returns ordered vector of futures for downloading blocks for certificates
    /// Order of futures returned follows order of blocks in the certificates.
    /// See BlockFetcher for more details.
    async fn fetch_blocks(
        &self,
        deliver: CommittedSubDag,
        parent_hash: B256,
        number: u64,
    ) -> ConsensusOutput {
        let num_blocks = deliver.num_blocks();
        let num_certs = deliver.len();

        // get the execution address of the authority or use zero address
        let leader = self.inner.committee.authority(&deliver.leader.origin());
        let address = if let Some(authority) = leader {
            authority.execution_address()
        } else {
            warn!("Execution address missing for {}", &deliver.leader.origin());
            Address::ZERO
        };

        if num_blocks == 0 {
            debug!("No blocks to fetch, payload is empty");
            return ConsensusOutput {
                sub_dag: Arc::new(deliver),
                blocks: vec![],
                beneficiary: address,
                block_digests: VecDeque::new(),
                parent_hash,
                number,
            };
        }

        let sub_dag = Arc::new(deliver);
        let mut subscriber_output = ConsensusOutput {
            sub_dag: sub_dag.clone(),
            blocks: Vec::with_capacity(num_certs),
            beneficiary: address,
            block_digests: VecDeque::new(),
            parent_hash,
            number,
        };

        let mut batch_digests_and_workers: HashMap<
            NetworkPublicKey,
            (HashSet<BlockHash>, HashSet<NetworkPublicKey>),
        > = HashMap::new();

        for cert in &sub_dag.certificates {
            for (digest, (worker_id, _)) in cert.header().payload().iter() {
                let own_worker_name = self
                    .inner
                    .worker_cache
                    .worker(
                        self.inner
                            .committee
                            .authority(&self.inner.authority_id)
                            .unwrap()
                            .protocol_key(),
                        worker_id,
                    )
                    .unwrap_or_else(|_| panic!("worker_id {worker_id} is not in the worker cache"))
                    .name;
                let workers = Self::workers_for_certificate(&self.inner, cert, worker_id);
                let (batch_set, worker_set) =
                    batch_digests_and_workers.entry(own_worker_name).or_default();
                batch_set.insert(*digest);
                subscriber_output.block_digests.push_back(*digest);
                worker_set.extend(workers);
            }
        }

        let fetched_batches_timer = self
            .consensus_bus
            .executor_metrics()
            .block_fetch_for_committed_subdag_total_latency
            .start_timer();
        self.consensus_bus
            .executor_metrics()
            .committed_subdag_block_count
            .observe(num_blocks as f64);
        let fetched_batches = self.fetch_blocks_from_workers(batch_digests_and_workers).await;
        drop(fetched_batches_timer);

        // Map all fetched batches to their respective certificates and submit as
        // consensus output
        for cert in &sub_dag.certificates {
            let mut output_batches = Vec::with_capacity(cert.header().payload().len());

            self.consensus_bus.executor_metrics().subscriber_current_round.set(cert.round() as i64);

            self.consensus_bus
                .executor_metrics()
                .subscriber_certificate_latency
                .observe(cert.created_at().elapsed().as_secs_f64());

            for (digest, (_, _)) in cert.header().payload().iter() {
                self.consensus_bus.executor_metrics().subscriber_processed_blocks.inc();
                let batch = fetched_batches
                    .get(digest)
                    .expect("[Protocol violation] Batch not found in fetched batches from workers of certificate signers");

                debug!(
                    "Adding fetched batch {digest} from certificate {} to consensus output",
                    cert.digest()
                );
                output_batches.push(batch.clone());
            }
            subscriber_output.blocks.push(output_batches);
        }
        subscriber_output
    }

    fn workers_for_certificate(
        inner: &Inner,
        certificate: &Certificate,
        worker_id: &WorkerId,
    ) -> Vec<NetworkPublicKey> {
        // Can include own authority and worker, but worker will always check local storage when
        // fetching paylods.
        let authorities = certificate.signed_authorities_with_committee(&inner.committee);
        authorities
            .into_iter()
            .filter_map(|authority| {
                let worker = inner.worker_cache.worker(&authority, worker_id);
                match worker {
                    Ok(worker) => Some(worker.name),
                    Err(err) => {
                        error!(target: "telcoin::subscriber",
                            "Worker {} not found for authority {}: {:?}",
                            worker_id, authority, err
                        );
                        None
                    }
                }
            })
            .collect()
    }

    async fn fetch_blocks_from_workers(
        &self,
        block_digests_and_workers: HashMap<
            NetworkPublicKey,
            (HashSet<BlockHash>, HashSet<NetworkPublicKey>),
        >,
    ) -> HashMap<BlockHash, WorkerBlock> {
        let mut fetched_blocks = HashMap::new();

        for (worker_name, (digests, known_workers)) in block_digests_and_workers {
            debug!(
                "Attempting to fetch {} digests from {} known workers, {worker_name}'s",
                digests.len(),
                known_workers.len()
            );
            // TODO: Can further parallelize this by worker if necessary. Maybe move the logic
            // to LocalNetwork.
            // Only have one worker for now so will leave this for a future
            // optimization.
            let request = FetchBlocksRequest { digests, known_workers };
            let blocks = loop {
                match self.inner.client.fetch_blocks(worker_name.clone(), request.clone()).await {
                    Ok(resp) => break resp.blocks,
                    Err(e) => {
                        error!("Failed to fetch blocks from worker {worker_name}: {e:?}");
                        // Loop forever on failure. During shutdown, this should get cancelled.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            };
            for (digest, block) in blocks {
                self.record_fetched_block_metrics(&block, &digest);
                fetched_blocks.insert(digest, block);
            }
        }

        fetched_blocks
    }

    fn record_fetched_block_metrics(&self, block: &WorkerBlock, digest: &BlockHash) {
        if let Some(received_at) = block.received_at() {
            let remote_duration = received_at.elapsed().as_secs_f64();
            debug!(
                "Batch was fetched for execution after being received from another worker {}s ago.",
                remote_duration
            );
            self.consensus_bus
                .executor_metrics()
                .block_execution_local_latency
                .with_label_values(&["other"])
                .observe(remote_duration);
        } else {
            let local_duration = block.created_at().elapsed().as_secs_f64();
            debug!(
                "Batch was fetched for execution after being created locally {}s ago.",
                local_duration
            );
            self.consensus_bus
                .executor_metrics()
                .block_execution_local_latency
                .with_label_values(&["own"])
                .observe(local_duration);
        };

        let block_fetch_duration = block.created_at().elapsed().as_secs_f64();
        self.consensus_bus.executor_metrics().block_execution_latency.observe(block_fetch_duration);
        debug!(
            "Block {:?} took {} seconds since it has been created to when it has been fetched for execution",
            digest,
            block_fetch_duration,
        );
    }
}

// TODO: add a unit test
