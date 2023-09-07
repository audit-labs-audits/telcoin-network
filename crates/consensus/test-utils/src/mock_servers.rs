// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Mock server implementations.
//! 
//! Simulate network traffic between nodes and worker/primary.
use anemo::async_trait;
use consensus_network::Multiaddr;
use fastcrypto::traits::KeyPair as _;
use std::collections::HashMap;
use tn_types::consensus::{
    crypto::NetworkKeyPair, FetchBatchesRequest,
    FetchBatchesResponse, FetchCertificatesRequest, FetchCertificatesResponse,
    GetCertificatesRequest, GetCertificatesResponse,
    PayloadAvailabilityRequest, PayloadAvailabilityResponse, PrimaryToPrimary,
    PrimaryToPrimaryServer, PrimaryToWorker, PrimaryToWorkerServer, RequestBatchRequest,
    RequestBatchResponse, RequestBatchesRequest, RequestBatchesResponse, RequestVoteRequest,
    RequestVoteResponse, SendCertificateRequest, SendCertificateResponse,
    WorkerBatchMessage, WorkerDeleteBatchesMessage,
    WorkerSynchronizeMessage, WorkerToWorker, WorkerToWorkerServer,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::info;


#[derive(Clone)]
pub struct PrimaryToPrimaryMockServer {
    sender: Sender<SendCertificateRequest>,
}

impl PrimaryToPrimaryMockServer {
    pub fn spawn(
        network_keypair: NetworkKeyPair,
        address: Multiaddr,
    ) -> (Receiver<SendCertificateRequest>, anemo::Network) {
        let addr = address.to_anemo_address().unwrap();
        let (sender, receiver) = channel(1);
        let service = PrimaryToPrimaryServer::new(Self { sender });

        let routes = anemo::Router::new().add_rpc_service(service);
        let network = anemo::Network::bind(addr)
            .server_name("lattice")
            .private_key(network_keypair.private().0.to_bytes())
            .start(routes)
            .unwrap();
        info!("starting network on: {}", network.local_addr());
        (receiver, network)
    }
}

#[async_trait]
impl PrimaryToPrimary for PrimaryToPrimaryMockServer {
    async fn send_certificate(
        &self,
        request: anemo::Request<SendCertificateRequest>,
    ) -> Result<anemo::Response<SendCertificateResponse>, anemo::rpc::Status> {
        let message = request.into_body();

        self.sender.send(message).await.unwrap();

        Ok(anemo::Response::new(SendCertificateResponse { accepted: true }))
    }

    async fn request_vote(
        &self,
        _request: anemo::Request<RequestVoteRequest>,
    ) -> Result<anemo::Response<RequestVoteResponse>, anemo::rpc::Status> {
        unimplemented!()
    }
    async fn get_certificates(
        &self,
        _request: anemo::Request<GetCertificatesRequest>,
    ) -> Result<anemo::Response<GetCertificatesResponse>, anemo::rpc::Status> {
        unimplemented!()
    }
    async fn fetch_certificates(
        &self,
        _request: anemo::Request<FetchCertificatesRequest>,
    ) -> Result<anemo::Response<FetchCertificatesResponse>, anemo::rpc::Status> {
        unimplemented!()
    }

    async fn get_payload_availability(
        &self,
        _request: anemo::Request<PayloadAvailabilityRequest>,
    ) -> Result<anemo::Response<PayloadAvailabilityResponse>, anemo::rpc::Status> {
        unimplemented!()
    }
}

pub struct PrimaryToWorkerMockServer {
    // TODO: refactor tests to use mockall for this.
    synchronize_sender: Sender<WorkerSynchronizeMessage>,
}

impl PrimaryToWorkerMockServer {
    pub fn spawn(
        keypair: NetworkKeyPair,
        address: Multiaddr,
    ) -> (Receiver<WorkerSynchronizeMessage>, anemo::Network) {
        let addr = address.to_anemo_address().unwrap();
        let (synchronize_sender, synchronize_receiver) = channel(1);
        let service = PrimaryToWorkerServer::new(Self { synchronize_sender });

        let routes = anemo::Router::new().add_rpc_service(service);
        let network = anemo::Network::bind(addr)
            .server_name("lattice")
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
    ) -> Result<anemo::Response<FetchBatchesResponse>, anemo::rpc::Status> {
        Ok(anemo::Response::new(FetchBatchesResponse { batches: HashMap::new() }))
    }

    // TODO: delete this - only used by external consensus
    // async fn delete_batches(
    //     &self,
    //     _request: anemo::Request<WorkerDeleteBatchesMessage>,
    // ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
    //     tracing::error!("Not implemented PrimaryToWorkerMockServer::delete_batches");
    //     Err(anemo::rpc::Status::internal("Unimplemented"))
    // }
}

pub struct WorkerToWorkerMockServer {
    batch_sender: Sender<WorkerBatchMessage>,
}

impl WorkerToWorkerMockServer {
    pub fn spawn(
        keypair: NetworkKeyPair,
        address: Multiaddr,
    ) -> (Receiver<WorkerBatchMessage>, anemo::Network) {
        let addr = address.to_anemo_address().unwrap();
        let (batch_sender, batch_receiver) = channel(1);
        let service = WorkerToWorkerServer::new(Self { batch_sender });

        let routes = anemo::Router::new().add_rpc_service(service);
        let network = anemo::Network::bind(addr)
            .server_name("lattice")
            .private_key(keypair.private().0.to_bytes())
            .start(routes)
            .unwrap();
        info!("starting network on: {}", network.local_addr());
        (batch_receiver, network)
    }
}

#[async_trait]
impl WorkerToWorker for WorkerToWorkerMockServer {
    async fn report_batch(
        &self,
        request: anemo::Request<WorkerBatchMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let message = request.into_body();

        self.batch_sender.send(message).await.unwrap();

        Ok(anemo::Response::new(()))
    }
    async fn request_batch(
        &self,
        _request: anemo::Request<RequestBatchRequest>,
    ) -> Result<anemo::Response<RequestBatchResponse>, anemo::rpc::Status> {
        tracing::error!("Not implemented WorkerToWorkerMockServer::request_batch");
        Err(anemo::rpc::Status::internal("Unimplemented"))
    }

    async fn request_batches(
        &self,
        _request: anemo::Request<RequestBatchesRequest>,
    ) -> Result<anemo::Response<RequestBatchesResponse>, anemo::rpc::Status> {
        tracing::error!("Not implemented WorkerToWorkerMockServer::request_batches");
        Err(anemo::rpc::Status::internal("Unimplemented"))
    }
}
