use tokio::sync::Semaphore;

// A struct that allows many threads to wait for a single notification. Once the notification is
// sent, all waiting threads are woken up, and all future calls to wait will resolve immediately.
pub struct OnceNotify {
    semaphore: Semaphore,
}

impl OnceNotify {
    pub fn new() -> Self {
        Self { semaphore: Semaphore::new(0) }
    }

    pub fn notify(&self) {
        self.semaphore.close();
    }

    pub async fn wait(&self) {
        match self.semaphore.acquire().await {
            // The semaphore is never given a permit
            Ok(_) => unreachable!(),
            // Will trigger when the semaphore is closed, indicating that the object has already
            // been notified
            Err(_) => {}
        }
    }
}
