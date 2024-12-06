// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use std::{future::Future, pin::pin};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::task::JoinHandle;

pub struct TaskManager {
    handles: Vec<(String, JoinHandle<()>)>,
    submanagers: Vec<(String, TaskManager)>,
}

impl TaskManager {
    pub fn new() -> Self {
        Self { handles: Vec::new(), submanagers: Vec::new() }
    }

    pub fn spawn_task<F>(&mut self, name: String, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = tokio::spawn(async move {
            future.await;
        });
        self.handles.push((name, handle));
    }

    pub fn add_task_manager(&mut self, name: String, manager: TaskManager) {
        self.submanagers.push((name, manager));
    }

    pub async fn join(self) {
        let mut future_tasks: FuturesUnordered<_> =
            self.handles.into_iter().map(|(name, h)| async move { (h.await, name) }).collect();
        let mut future_managers: FuturesUnordered<_> = self
            .submanagers
            .into_iter()
            .map(|(name, m)| async move { (m.join().await, name) })
            .collect();
        tokio::select!(
            res = future_tasks.next() => {
                match res {
                    Some((Ok(_), name)) => {}
                    Some((Err(join_err), name)) => {}
                    None => {}
                }

            }
            res = future_managers.next() => {
                match res {
                    Some((_, name)) => {}
                    None => {}
                }

            }
        )
    }

    pub async fn join_until_exit(self) {
        let mut future_tasks: FuturesUnordered<_> =
            self.handles.into_iter().map(|(name, h)| async move { (h.await, name) }).collect();
        let mut future_managers: FuturesUnordered<_> = self
            .submanagers
            .into_iter()
            .map(|(name, m)| async move { (m.join().await, name) })
            .collect();

        tokio::select! {
            _ = Self::exit() => {
                tracing::info!(target: "tn:cli", "Node exiting");
            },
            res = future_tasks.next() => {
                match res {
                    Some((Ok(_), name)) => {}
                    Some((Err(join_err), name)) => {}
                    None => {}
                }

            }
            res = future_managers.next() => {
                match res {
                    Some((_, name)) => {}
                    None => {}
                }

            }
        }
    }

    async fn exit() {
        #[cfg(unix)]
        {
            let mut stream =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("could not config sigterm");
            let sigterm = stream.recv();
            let sigterm = pin!(sigterm);
            let ctrl_c = pin!(tokio::signal::ctrl_c());

            tokio::select! {
                _ = ctrl_c => {
                    tracing::info!(target: "tn:cli", "Received ctrl-c");
                },
                _ = sigterm => {
                    tracing::info!(target: "tn::cli", "Received SIGTERM");
                },
            }
        }

        #[cfg(not(unix))]
        {
            let _ = ctrl_c().await;
        }
    }
}
