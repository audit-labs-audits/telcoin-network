// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Provide abstractions over sync chanel code.
//! This will allow us to insulate from specific implementations and more easily swap
//! as needed (for instance moving from MPSC to Broadcast).

use std::{
    error::Error,
    fmt::Display,
    future::Future,
    task::{Context, Poll},
};

/// The default channel capacity for each channel.
pub const CHANNEL_CAPACITY: usize = 10_000;

/// Error returned by `try_recv`.
/// This is just a trivial abstraction over the tokio version.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum TryRecvError {
    /// This **channel** is currently empty, but the **Sender**(s) have not yet
    /// disconnected, so data may yet become available.
    Empty,
    /// The **channel**'s sending half has become disconnected, and there will
    /// never be any more data received on it.
    Disconnected,
}

impl Error for TryRecvError {}

impl Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "recv error: Empty"),
            TryRecvError::Disconnected => write!(f, "recv error: Disconnected"),
        }
    }
}

impl From<tokio::sync::mpsc::error::TryRecvError> for TryRecvError {
    fn from(value: tokio::sync::mpsc::error::TryRecvError) -> Self {
        match value {
            tokio::sync::mpsc::error::TryRecvError::Empty => Self::Empty,
            tokio::sync::mpsc::error::TryRecvError::Disconnected => Self::Disconnected,
        }
    }
}

/// Error returned by the `TnSender`.
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct SendError<T>(pub T);

impl<T> Error for SendError<T> {}

impl<T> Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "send error")
    }
}

impl<T> std::fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SendError!")
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for SendError<T> {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> SendError<T> {
        SendError(value.0)
    }
}

/// This enumeration is the list of the possible error outcomes for the
/// [`try_send`](TnSender::try_send) method.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum TrySendError<T> {
    /// The data could not be sent on the channel because the channel is
    /// currently full and sending would require blocking.
    Full(T),

    /// The receive half of the channel was explicitly closed or has been
    /// dropped.
    Closed(T),
}

impl<T> Error for TrySendError<T> {}

impl<T> Display for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "Send Error: Full"),
            TrySendError::Closed(_) => write!(f, "Send Error: Closed"),
        }
    }
}

impl<T> std::fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "Send Error: Full"),
            TrySendError::Closed(_) => write!(f, "Send Error: Closed"),
        }
    }
}

impl<T> From<tokio::sync::mpsc::error::TrySendError<T>> for TrySendError<T> {
    fn from(value: tokio::sync::mpsc::error::TrySendError<T>) -> TrySendError<T> {
        match value {
            tokio::sync::mpsc::error::TrySendError::Full(t) => TrySendError::Full(t),
            tokio::sync::mpsc::error::TrySendError::Closed(t) => TrySendError::Closed(t),
        }
    }
}

pub trait TnReceiver<T>: Send + Unpin {
    /// Receives the next value for this channel.
    /// Signature is desugared async fn recv(&mut self) -> Option<T> with Send added.
    fn recv(&mut self) -> impl Future<Output = Option<T>> + Send;

    /// Attempts to receive the next value for this channel.
    fn try_recv(&mut self) -> Result<T, TryRecvError>;

    /// Polls to receive the next message on this channel.
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>>;
}

pub trait TnSender<T>: Unpin + Clone {
    /// Sends a value, waiting until there is capacity.
    /// Signature is desugared async fn send(&self, value: T) -> Result<(), SendError<T>> with Send
    /// added.
    fn send(&self, value: T) -> impl Future<Output = Result<(), SendError<T>>> + Send;

    /// Attempts to immediately send a message on this `Sender`
    fn try_send(&self, message: T) -> Result<(), TrySendError<T>>;

    /// Get a reciever for this TnSender.
    /// For an MPSC or other limited channel this may panic if called more than once.
    fn subscribe(&self) -> impl TnReceiver<T>;

    /// Borrow a reciever for this TnSender.
    /// This should try to return a receiver that once dropped can be reused.
    /// This is in contrast to subscribe which may give out the only receiver
    /// for some channel types (MPSC for instance).
    fn borrow_subscriber(&self) -> impl TnReceiver<T>;
}
