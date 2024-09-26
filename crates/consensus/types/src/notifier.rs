use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use parking_lot::Mutex;

/// A Noticer is a future that will resolve when the Notifier it is subscribed to is notified.
/// Used for simple notification schemes (like shutdown signals).
#[derive(Clone, Debug)]
pub struct Noticer {
    lock: Arc<Mutex<(bool, Option<Waker>)>>,
}

/// Simple notifier.
///
/// Will hand out a future on a subscribe() call that will resolve after
/// notify() if called.  Can manage any number of "subscribers".
/// Once notify() if called the resolved subscribers will be cleared.
pub struct Notifier {
    noticers: Vec<Noticer>,
}

impl Notifier {
    /// Create a new empty Notifier.
    pub fn new() -> Self {
        Self { noticers: Vec::new() }
    }

    /// Get a Noticer that will resolve once this Notifier is notified.
    pub fn subscribe(&mut self) -> Noticer {
        let noticer = Noticer { lock: Arc::new(Mutex::new((false, None))) };
        self.noticers.push(noticer.clone());
        noticer
    }

    /// Resolve all the subscribed Noticers.
    pub fn notify(&mut self) {
        for n in self.noticers.iter_mut() {
            let mut guard = n.lock.lock();
            let wake = guard.1.take();
            guard.0 = true;
            if let Some(wake) = wake {
                wake.wake();
            }
        }
        // Done with these, they can all resolve now.
        self.noticers.clear();
    }
}

impl Default for Notifier {
    fn default() -> Self {
        Self::new()
    }
}

impl Noticer {
    fn poll_int(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut guard = self.lock.lock();
        if guard.0 {
            guard.1 = None;
            Poll::Ready(())
        } else {
            guard.1 = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Future for Noticer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.poll_int(cx)
    }
}

impl Future for &Noticer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.poll_int(cx)
    }
}
