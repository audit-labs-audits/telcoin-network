// SPDX-License-Identifier: MIT or Apache-2.0
//! Peer-to-peer network interface for Telcoin Network built using libp2p.

mod codec;
mod consensus;
pub mod error;
pub mod types;

// export types
pub use codec::{TNCodec, TNMessage};
pub use consensus::ConsensusNetwork;

// re-export specific libp2p types
pub use libp2p::{
    gossipsub::Message as GossipMessage, identity::PeerId, request_response::ResponseChannel,
    Multiaddr,
};

use tn_types::NetworkPublicKey;

/// Convert an existing NetworkPublicKey into a libp2p PeerId.
pub fn network_public_key_to_libp2p(fastcrypto: &NetworkPublicKey) -> PeerId {
    let bytes = fastcrypto.as_ref().to_vec();
    let ed_public_key = libp2p::identity::ed25519::PublicKey::try_from_bytes(&bytes)
        .expect("invalid public key, not able to convert to peer id!");
    libp2p::PeerId::from_public_key(&ed_public_key.into())
}
