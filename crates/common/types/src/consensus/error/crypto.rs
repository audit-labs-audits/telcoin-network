use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum CryptoError {
    #[error("Signature is invalid.")]
    InvalidSignature,
}