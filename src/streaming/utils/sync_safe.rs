use std::pin::Pin;
use std::task::{Context, Poll};
use futures::{Stream, StreamExt};
use futures_util::FutureExt;

// One of the streams returned by the grpc library, only implements Send, not Sync. This wrapper
// safely makes a Send value Sync by wrapping in the correct synchronisation primitives. In the
// long term, we should revisit whether it is really necessary to make all the Streams in the
// library Sync. I added it in the beginning as a hedge to avoid running into annoying issues
// with the rust compiler, but sharing a &Stream between multiple threads is really useless.
// Implementation inspired by https://internals.rust-lang.org/t/what-shall-sync-mean-across-an-await/12020/2
pub struct SyncSafe<S> {
    inner: S,
    lock: parking_lot::Mutex<()>,
}

impl <S> SyncSafe<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            lock: parking_lot::Mutex::new(()),
        }
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<T> SyncSafe<T> {
    pub fn get_mut(&mut self) -> &mut T { &mut self.inner }
}

impl <S> Future for SyncSafe<S>
where
    S: Future + Send + Unpin,
    S::Output: Send,
{
    type Output = S::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // No need to lock the mutex here as the exclusive reference protects against concurrent
        // access
        self.inner.poll_unpin(cx)
    }
}

// Provide a convenience stream method that avoids locking when using a mutable reference
impl <S> Stream for SyncSafe<S>
where
    S: Stream + Send + Unpin,
    S::Item: Send,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // No need to lock the mutex here as the exclusive reference protects against concurrent
        // access
        self.inner.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // The chance that some implementation of the size_hint method would behave badly when
        // accessed concurrently from different threads is extremely low. Despite that, we must
        // still use the lock here to abide by the guarantee that a lock will protect all accesses
        // from shared references.
        let guard = self.lock.lock();
        let hint = self.inner.size_hint();
        drop(guard);
        hint
    }
}

unsafe impl<S> Send for SyncSafe<S> where S: Send {}
unsafe impl<S> Sync for SyncSafe<S> where S: Send {}
