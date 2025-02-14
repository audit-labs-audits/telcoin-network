use tn_network_libp2p::{
    error::NetworkError,
    types::{IdentTopic, NetworkHandle, NetworkResult},
    PeerId,
};
use tn_network_types::FetchCertificatesRequest;
use tn_types::{encode, BlockHash, Certificate, CertificateDigest, ConsensusHeader, Header, Vote};

use crate::network::message::PrimaryRPCError;

use super::{
    message::PrimaryGossip, MissingCertificatesRequest, PrimaryRequest, PrimaryResponse, Req, Res,
};

#[derive(Clone)]
pub struct NetworkClient {
    network_handle: NetworkHandle<Req, Res>,
    peer: PeerId,
}

impl std::fmt::Debug for NetworkClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Primary Network Client[{}]", self.peer)
    }
}

/// Responses to a vote request.
#[derive(Clone, Debug, PartialEq)]
pub enum RequestVoteResult {
    /// The peer's vote if the peer considered the proposed header valid.
    Vote(Vote),
    /// Missing certificates in order to vote.
    ///
    /// If the peer was unable to verify parents for a proposed header, they respond requesting
    /// the missing certificate by digest.
    MissingParents(Vec<CertificateDigest>),
}

impl NetworkClient {
    pub fn new(network_handle: NetworkHandle<Req, Res>, peer: PeerId) -> Self {
        Self { network_handle, peer }
    }

    /// Publish a certificate to the consensus network.
    /// NOTE: this is a publish, it is not specific to this client but here for convience.
    pub async fn publish_certificate(&self, certificate: Certificate) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::Certificate(certificate));
        self.network_handle.publish(IdentTopic::new("tn-primary"), data).await?;
        Ok(())
    }

    /// Request a vote for header from the peer.
    /// Can return a response of Vote or MissingParents, other responses will be an error.
    pub async fn request_vote(
        &self,
        header: Header,
        parents: Vec<Certificate>,
    ) -> NetworkResult<RequestVoteResult> {
        let request = PrimaryRequest::Vote { header, parents };
        let res = self.network_handle.send_request(request, self.peer).await?;
        let res = res.await??;
        match res {
            PrimaryResponse::Vote(vote) => Ok(RequestVoteResult::Vote(vote)),
            PrimaryResponse::Error(PrimaryRPCError(s)) => Err(NetworkError::RPCError(s)),
            PrimaryResponse::RequestedCertificates(_vec) => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is requested certificates!".to_string(),
            )),
            PrimaryResponse::MissingParents(parents) => {
                Ok(RequestVoteResult::MissingParents(parents))
            }
            PrimaryResponse::ConsensusHeader(_consensus_header) => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is consensus header!".to_string(),
            )),
        }
    }

    pub async fn fetch_certificates(
        &self,
        request: FetchCertificatesRequest,
    ) -> NetworkResult<Vec<Certificate>> {
        let FetchCertificatesRequest { exclusive_lower_bound, skip_rounds, max_items } = request;
        let request = PrimaryRequest::MissingCertificates {
            inner: MissingCertificatesRequest { exclusive_lower_bound, skip_rounds, max_items },
        };
        let res = self.network_handle.send_request(request, self.peer).await?;
        let res = res.await??;
        match res {
            PrimaryResponse::RequestedCertificates(certs) => Ok(certs),
            PrimaryResponse::Error(PrimaryRPCError(s)) => Err(NetworkError::RPCError(s)),
            _ => Err(NetworkError::RPCError("Got wrong response, not a certificate!".to_string())),
        }
    }

    pub async fn request_consensus(
        &self,
        number: Option<u64>,
        hash: Option<BlockHash>,
    ) -> NetworkResult<ConsensusHeader> {
        let request = PrimaryRequest::ConsensusHeader { number, hash };
        let res = self.network_handle.send_request(request, self.peer).await?;
        let res = res.await??;
        match res {
            PrimaryResponse::ConsensusHeader(header) => Ok(header),
            PrimaryResponse::Error(PrimaryRPCError(s)) => Err(NetworkError::RPCError(s)),
            _ => Err(NetworkError::RPCError(
                "Got wrong response, not a consensus header!".to_string(),
            )),
        }
    }
}
