use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use parking_lot::Mutex;

#[derive(Clone, Debug)]
pub struct Noticer {
    lock: Arc<Mutex<(bool, Option<Waker>)>>,
}

pub struct Notifier {
    noticers: Vec<Noticer>,
}

impl Notifier {
    pub fn new() -> Self {
        Self { noticers: Vec::new() }
    }

    pub fn subscribe(&mut self) -> Noticer {
        let noticer = Noticer { lock: Arc::new(Mutex::new((false, None))) };
        self.noticers.push(noticer.clone());
        noticer
    }

    pub fn notify(&mut self) {
        for n in self.noticers.iter_mut() {
            let mut guard = n.lock.lock();
            let wake = guard.1.take();
            guard.0 = true;
            if let Some(wake) = wake {
                wake.wake();
            }
        }
    }
}

impl Default for Notifier {
    fn default() -> Self {
        Self::new()
    }
}

impl Future for Noticer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut guard = this.lock.lock();
        if guard.0 {
            guard.1 = None;
            Poll::Ready(())
        } else {
            guard.1 = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Future for &Noticer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut guard = this.lock.lock();
        if guard.0 {
            guard.1 = None;
            Poll::Ready(())
        } else {
            guard.1 = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
