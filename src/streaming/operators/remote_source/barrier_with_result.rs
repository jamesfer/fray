use std::pin::Pin;
use futures_util::future::{Shared};
use futures_util::FutureExt;
use tokio::sync::Mutex;
use tokio::sync::oneshot::{Receiver, Sender};
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 'a>>;

enum BarrierInternalError {
    AllFinished
}

type ComputationSender<T> = Sender<Result<T, String>>;
type ComputationReceiver<T> = Shared<Receiver<Result<T, String>>>;
type SyncQueue<T> = Vec<Result<ComputationSender<T>, ComputationReceiver<T>>>;

struct InnerQueueState<T> {
    count: usize,
    computation_function: Option<Box<dyn FnOnce() -> (BoxFuture<'static, Result<T, DataFusionError>>) + Send + Sync + 'static>>,
    synchronisation_queue: Option<SyncQueue<T>>,
}

impl <T> InnerQueueState<T>
where T: Clone
{
    fn new(count: usize) -> Self {
        Self {
            count,
            computation_function: None,
            synchronisation_queue: Self::create_state_queue(count),
        }
    }

    pub async fn compute_result<F, Fut>(&mut self, f: F) -> Result<Result<T, DataFusionError>, ComputationReceiver<T>>
    where
        F: FnOnce() -> (Fut) + Send + Sync + 'static,
        Fut: Future<Output=Result<T, DataFusionError>> + Send + Sync + 'static
    {
        self.computation_function.get_or_insert(Self::wrap_function(f));
        self.pop_queue_inner().await
    }

    pub async fn remove_participant(&mut self) {
        // Reduce the number of users by one
        self.count = self.count.saturating_sub(1);

        // Remove our entry from the queue.
        // This match is empty, but is elaborated to make it clearer why we don't need to use the
        // result in either case.
        match self.pop_queue_inner().await {
            Ok(_result) => {
                // This case means we were the last participant. We calculated the result using the
                // stored function and sent it to the other receivers. We don't need to do anything
                // else here.
            }
            Err(_receiver) => {
                // If we are not the last participant, we can just drop our receiver as we don't
                // need the value.
            }
        };
    }

    async fn pop_queue_inner(
        &mut self,
    ) -> Result<Result<T, DataFusionError>, ComputationReceiver<T>> {
        let next_item = match &mut self.synchronisation_queue {
            Some(queue) => queue.pop()
                .expect("Barrier ran out of states. Are there more consumers than expected?"),
            // When the queue is None, it means there is only one user of the barrier, so we can
            // short-circuit all the synchronisation logic.
            None => return Ok(match self.computation_function.take() {
                Some(f) => f().await,
                None => Err(internal_datafusion_err!("All users of the barrier have finished, there is no result to share")),
            }),
        };

        match next_item {
            // We are not the last thread, so we need to wait for the result
            Err(receiver) => Err(receiver),
            // We are the last thread, so we are responsible for sending the result
            Ok(sender) => {
                let result = match self.computation_function.take() {
                    Some(f) => f().await,
                    None => Err(internal_datafusion_err!("All users of the barrier have finished, there is no result to share")),
                };

                // Restart the state using a possible changed count
                self.synchronisation_queue = Self::create_state_queue(self.count);

                let sendable_result = match &result {
                    Ok(value) => Ok((*value).clone()),
                    Err(e) => Err(format!("Error computing result: {}", e)),
                };
                match sender.send(sendable_result) {
                    Ok(()) => (),
                    Err(_) => {
                        return Ok(Err(internal_datafusion_err!("Barrier receiver was dropped before the result was sent.")));
                    }
                }

                Ok(result)
            }
        }
    }

    fn create_state_queue(count: usize) -> Option<SyncQueue<T>> {
        // In these cases, there is no need to create a queue
        if count <= 1 {
            return None;
        }

        Some(Self::create_state_queue_unchecked(count))
    }

    fn create_state_queue_unchecked(count: usize) -> SyncQueue<T> {
        assert!(count > 1, "Barrier count must be greater than 1");

        let (sender, receiver) = tokio::sync::oneshot::channel();
        std::iter::once(Ok(sender))
            .chain(std::iter::repeat_n(receiver.shared(), count - 1).map(Err))
            .collect()
    }

    fn wrap_function<F, Fut>(f: F) -> Box<dyn FnOnce() -> (BoxFuture<'static, Result<T, DataFusionError>>) + Send + Sync>
    where
        F: FnOnce() -> (Fut) + Send + Sync + 'static,
        Fut: Future<Output=Result<T, DataFusionError>> + Send + Sync + 'static,
    {
        Box::new(|| Box::pin(f()))
    }
}

pub struct BarrierWithResult<T> {
    inner_queue: Mutex<InnerQueueState<T>>,
}

impl <T> BarrierWithResult<T>
where T: Clone
{
    pub fn new(count: usize) -> Self {
        Self {
            inner_queue: Mutex::new(InnerQueueState::new(count)),
        }
    }

    pub async fn compute_result<F, Fut>(&self, f: F) -> Result<T, DataFusionError>
    where
        F: FnOnce() -> (Fut) + Send + Sync + 'static,
        Fut: Future<Output=Result<T, DataFusionError>> + Send + Sync + 'static
    {
        let result = {
            let mut queue_guard = self.inner_queue.lock().await;
            queue_guard.compute_result(f).await
        };
        match result {
            Ok(immediate_result) => immediate_result,
            Err(receiver) => match receiver.await {
                Ok(Ok(value)) => Ok(value),
                // Error from compute function
                Ok(Err(message)) => Err(internal_datafusion_err!("{}", message)),
                // Error from receiver
                Err(_) => Err(internal_datafusion_err!("Barrier sender was dropped before the result was received.")),
            },
        }
    }

    // TODO participants need to call a non-async function when they are dropped. Currently, if one
    //  of the participants is dropped, all others will wait indefinitely.
    pub async fn remove_participant(&self) {
        let mut queue_guard = self.inner_queue.lock().await;
        queue_guard.remove_participant().await;
    }
}
