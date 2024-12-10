// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Debug, Display},
    future::Future,
    pin::pin,
    sync::Arc,
};

use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use tokio::task::JoinHandle;

struct Task {
    name: String,
    handle: JoinHandle<()>,
}

#[derive(Clone)]
struct Submanager {
    name: String,
    manager: TaskManager,
}

#[derive(Clone)]
pub struct TaskManager {
    handles: Arc<Mutex<Vec<Task>>>,
    submanagers: Vec<Submanager>,
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskManager {
    pub fn new() -> Self {
        Self { handles: Arc::new(Mutex::new(Vec::new())), submanagers: Vec::new() }
    }

    pub fn spawn_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let name = name.to_string();
        let handle = tokio::spawn(async move {
            future.await;
        });
        self.handles.lock().push(Task { name, handle });
    }

    pub fn add_task_manager<S: ToString>(&mut self, name: S, manager: TaskManager) {
        self.submanagers.push(Submanager { name: name.to_string(), manager });
    }

    pub async fn join(self) {
        let mut future_tasks: FuturesUnordered<_> = {
            let mut handles = self.handles.lock();
            handles.drain(..).map(|task| async move { (task.handle.await, task.name) }).collect()
        };
        let mut future_managers: FuturesUnordered<_> = self
            .submanagers
            .into_iter()
            .map(|sub| async move { (sub.manager.join().await, sub.name) })
            .collect();
        let fut_mans = async move {
            if future_managers.is_empty() {
                futures::future::pending().await
            } else {
                future_managers.next().await
            }
        };
        tokio::select!(
            res = future_tasks.next() => {
                match res {
                    Some((Ok(_), name)) => {
                        tracing::error!("JOIN OK for {name}");
                    }
                    Some((Err(join_err), name)) => {
                        tracing::error!("JOIN ERROR for {name}: {join_err}");
                    }
                    None => {}
                }

            }
            res = fut_mans => {
                if let Some((_, _name)) = res {}
            }
        )
    }

    pub async fn join_until_exit(self) {
        let mut future_tasks: FuturesUnordered<_> = {
            let mut handles = self.handles.lock();
            handles.drain(..).map(|task| async move { (task.handle.await, task.name) }).collect()
        };
        let mut future_managers: FuturesUnordered<_> = self
            .submanagers
            .into_iter()
            .map(|sub| async move { (sub.manager.join().await, sub.name) })
            .collect();
        let fut_mans = async move {
            if future_managers.is_empty() {
                futures::future::pending().await
            } else {
                future_managers.next().await
            }
        };

        tokio::select! {
            _ = Self::exit() => {
                tracing::info!(target: "tn:cli", "Node exiting");
                //break;
            },
            res = future_tasks.next() => {
                match res {
                    Some((Ok(_), name)) => {
                        tracing::error!("JOIN OK for {name}");
                    }
                    Some((Err(join_err), name)) => {
                        tracing::error!("JOIN ERROR for {name}: {join_err}");
                    }
                    None => {}
                }

            }
            res = fut_mans => {
                if let Some((_, _name)) = res {}

            }
        }
    }

    /// Abort all of our direct tasks (not sub task managers though).
    /// This is included for some tests, should not use in real code.
    pub fn abort(&self) {
        for task in self.handles.lock().iter() {
            task.handle.abort();
        }
    }

    /// Will resolve when ctrl-c is pressed or a SIGTERM is received.
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

impl Display for TaskManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for task in self.handles.lock().iter() {
            writeln!(f, "Task: {}", task.name)?;
        }
        for sub in &self.submanagers {
            writeln!(f, "++++++++++++++++++++++++++++++++++++++++++++++++++++")?;
            writeln!(f, "Sub Tasks: {}\n{}", sub.name, sub.manager)?;
            writeln!(f, "++++++++++++++++++++++++++++++++++++++++++++++++++++")?;
        }
        Ok(())
    }
}
impl Debug for TaskManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl reth_tasks::TaskSpawner for TaskManager {
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        tokio::spawn(fut)
    }

    fn spawn_critical(&self, name: &'static str, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        let (tx, mut rx) = tokio::sync::broadcast::channel(1);
        // Need two join handles so do this channel dance to get them.
        // Required because the task manager needs one and this foreign Reth interface return one.
        let f = async move {
            let value = fut.await;
            let _ = tx.send(value);
        };
        let join = tokio::spawn(async move {
            let _ = rx.recv().await;
        });
        self.spawn_task(name.to_string(), f);
        join
    }

    fn spawn_blocking(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || handle.block_on(fut))
    }

    fn spawn_critical_blocking(
        &self,
        name: &'static str,
        fut: BoxFuture<'static, ()>,
    ) -> JoinHandle<()> {
        let (tx, mut rx) = tokio::sync::broadcast::channel(1);
        // Need two join handles so do this channel dance to get them.
        // Required because the task manager needs one and this foreign Reth interface return one.
        let f = async move {
            let value = fut.await;
            let _ = tx.send(value);
        };
        let join = tokio::spawn(async move {
            let _ = rx.recv().await;
        });
        let handle = tokio::runtime::Handle::current();
        let join_handle = tokio::task::spawn_blocking(move || handle.block_on(f));
        self.handles.lock().push(Task { name: name.to_string(), handle: join_handle });
        join
    }
}
