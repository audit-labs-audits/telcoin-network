//! Mock implementations for Primary to Primary/Worker.
use anemo::async_trait;
use std::collections::HashMap;
use tn_network_types::{
    FetchBatchResponse, FetchBatchesRequest, PrimaryToWorker, PrimaryToWorkerServer,
    WorkerSynchronizeMessage,
};
use tn_types::{traits::KeyPair as _, Multiaddr, NetworkKeypair};

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::info;

pub struct PrimaryToWorkerMockServer {
    // TODO: refactor tests to use mockall for this.
    synchronize_sender: Sender<WorkerSynchronizeMessage>,
}

impl PrimaryToWorkerMockServer {
    pub fn spawn(
        keypair: NetworkKeypair,
        address: Multiaddr,
    ) -> (Receiver<WorkerSynchronizeMessage>, anemo::Network) {
        let addr = address.to_anemo_address().unwrap();
        let (synchronize_sender, synchronize_receiver) = channel(1);
        let service = PrimaryToWorkerServer::new(Self { synchronize_sender });

        let routes = anemo::Router::new().add_rpc_service(service);
        let network = anemo::Network::bind(addr)
            .server_name("tn-test")
            .private_key(keypair.private().0.to_bytes())
            .start(routes)
            .unwrap();
        info!("starting network on: {}", network.local_addr());
        (synchronize_receiver, network)
    }
}

#[async_trait]
impl PrimaryToWorker for PrimaryToWorkerMockServer {
    async fn synchronize(
        &self,
        request: anemo::Request<WorkerSynchronizeMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let message = request.into_body();
        self.synchronize_sender.send(message).await.unwrap();
        Ok(anemo::Response::new(()))
    }

    async fn fetch_batches(
        &self,
        _request: anemo::Request<FetchBatchesRequest>,
    ) -> Result<anemo::Response<FetchBatchResponse>, anemo::rpc::Status> {
        Ok(anemo::Response::new(FetchBatchResponse { batches: HashMap::new() }))
    }
}
