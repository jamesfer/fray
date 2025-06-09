use std::collections::{HashMap, VecDeque};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use futures::Stream;
use tokio::sync::{Notify, RwLock};
use datafusion::common::{internal_datafusion_err, DataFusionError};
use crate::streaming::action_stream::StreamItem;

struct DataChannelState {
    data: RwLock<(VecDeque<StreamItem>, Option<Arc<Notify>>)>,
    status: std::sync::RwLock<ChannelStatus>,
}

pub struct DataChannelSender {
    state: Arc<DataChannelState>,
    next_index: usize,
    last_checkpoint: usize,
}

impl DataChannelSender {
    fn new(state: Arc<DataChannelState>) -> Self {
        Self {
            state,
            next_index: 0,
            last_checkpoint: 0,
        }
    }

    pub async fn send(&mut self, item: Result<StreamItem, DataFusionError>) {
        match item {
            Ok(item) => {
                let mut data_state = self.state.data.write().await;
                let (vec, notification) = data_state.deref_mut();

                // Write the item to the channel
                self.next_index += 1;
                if let StreamItem::Marker(marker) = &item {
                    self.last_checkpoint = marker.checkpoint_number as usize;
                }
                vec.push_back(item);

                // Notify all waiters and reset the notification
                let notification = mem::replace(notification, Some(Arc::new(Notify::new())));
                // Notification is only ever None after the channel has been dropped
                notification.unwrap().notify_waiters()
            },
            Err(err) => {
                let mut status = self.state.status.write().unwrap();
                *status = ChannelStatus::Error(Arc::new(err));
            },
        }
    }
}

impl Drop for DataChannelSender {
    fn drop(&mut self) {
        let mut status = self.state.status.write().unwrap();
        *status = ChannelStatus::Cancelled {
            last_checkpoint: self.last_checkpoint,
            valid_until: self.next_index,
        };
    }
}

enum ChannelStatus {
    Running,
    Finished,
    Error(Arc<DataFusionError>),
    Cancelled { last_checkpoint: usize, valid_until: usize,},
}

// An array of data that allows you to wait for the next element to be available
struct DataChannel {
    state: Arc<DataChannelState>,
}

impl DataChannel {
    fn create() -> (Self, DataChannelSender) {
        let state = Arc::new(DataChannelState {
            // sender_alive: AtomicBool::new(true),
            data: RwLock::new((VecDeque::new(), Some(Arc::new(Notify::new())))),
            status: std::sync::RwLock::new(ChannelStatus::Running),
        });

        let channel = Self::new(state.clone());
        let sender = DataChannelSender::new(state);

        (channel, sender)
    }

    fn new(state: Arc<DataChannelState>) -> Self {
        Self { state }
    }

    // This is explicitly marked as 'static to tell rust that it doesn't use the lifetime of &self
    pub fn stream(&self) -> impl Stream<Item=Result<StreamItem, Arc<DataFusionError>>> + Sync + Send + 'static + use<> {
        let state = self.state.clone();

        futures_util::stream::try_unfold(0, move |index| {
            let state = state.clone();
            async move {
                loop {
                    // Check the state
                    let is_finished = {
                        let status = state.status.read().unwrap();
                        match *status.deref() {
                            ChannelStatus::Running => false,
                            ChannelStatus::Finished => true,
                            // Immediately return the error
                            ChannelStatus::Error(ref e) => return Err(e.clone()),
                            ChannelStatus::Cancelled { last_checkpoint, valid_until } => {
                                // Check if we have passed the last completed checkpoint
                                if index == valid_until {
                                    return Ok(None);
                                } else if index >= valid_until {
                                    return Err(Arc::new(DataFusionError::Execution(format!(
                                        "Channel was cancelled at checkpoint {}, last valid index {}",
                                        last_checkpoint, valid_until
                                    ))));
                                } else {
                                    true
                                }
                            }
                        }
                    };

                    // Attempt to read the next item
                    let next_item_result = {
                        let guard = state.data.read().await;
                        let (vec, notification) = guard.deref();
                        vec.get(index)
                            .cloned()
                            .ok_or_else(|| notification.clone())
                    };
                    match next_item_result {
                        // Item is ready, return it
                        Ok(item) => {
                            return Ok(Some((item, index + 1)));
                        },
                        // No item was ready
                        Err(Some(notification)) => {
                            if is_finished {
                                // If the channel is finished, return None because no other items
                                // are coming
                                return Ok(None);
                            }
                            // Otherwise, wait for the next notification
                            notification.notified().await;
                        },
                        Err(None) => {
                            // If there is no notification, it means the channel has been closed
                            return Err(Arc::new(DataFusionError::Execution("Channel has been closed".to_string())));
                        }
                    }
                }
            }
        })
    }
}

struct OutputStream {
    channel: DataChannel,
    partitions: Vec<usize>,
}

pub struct ExchangeChannelStore {
    channels: RwLock<HashMap<String, OutputStream>>,
}

impl ExchangeChannelStore {
    pub(crate) fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create(&self, stream_id: String, partitions: Vec<usize>) -> DataChannelSender {
        let (channel, sender) = DataChannel::create();
        let output_stream = OutputStream {
            channel,
            partitions,
        };

        let mut channels = self.channels.write().await;
        channels.insert(stream_id, output_stream);
        sender
    }

    pub async fn stream(&self, stream_id: &str, partitions: &[usize]) -> Result<impl Stream<Item=Result<StreamItem, Arc<DataFusionError>>> + Sync + Send + use<>, DataFusionError> {
        let channels = self.channels.read().await;
        let channel = channels.get(stream_id)
            .ok_or_else(|| internal_datafusion_err!("No channel found for stream id: {}", stream_id))?;
        if channel.partitions != partitions {
            // TODO, when we have better partitioning support, allow for partial matches of
            // partitions
            return Err(internal_datafusion_err!(
                "Requested partitions {:?} do not match available partitions {:?}",
                partitions, channel.partitions
            ));
        }
        Ok(channel.channel.stream())
    }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use datafusion::common::record_batch;
    use crate::streaming::action_stream::StreamItem;
    use crate::streaming::runtime::exchange_manager::data_channels::ExchangeChannelStore;

    #[tokio::test]
    async fn fails_when_stream_id_does_not_exist() {
        let store = ExchangeChannelStore::new();
        let _sender = store.create("test_stream".to_string(), vec![0]).await;
        let result = store.stream("other_stream", &[0]).await;
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("No channel found for stream id: other_stream"));
    }

    #[tokio::test]
    async fn fails_when_partitions_do_not_match() {
        let store = ExchangeChannelStore::new();
        let _sender = store.create("test_stream".to_string(), vec![0]).await;
        let result = store.stream("test_stream", &[1]).await;
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("Requested partitions [1] do not match available partitions [0]"));
    }

    #[tokio::test]
    async fn can_create_and_stream_from_channel() {
        let record_batch_1 = record_batch!(("a", Int32, vec![1, 2, 3])).unwrap();
        let record_batch_2 = record_batch!(("a", Int32, vec![4, 5, 6])).unwrap();
        let store = ExchangeChannelStore::new();

        // Ensure the sender is dropped
        {
            let mut sender = store.create("test_stream".to_string(), vec![0]).await;
            sender.send(Ok(StreamItem::RecordBatch(record_batch_1.clone()))).await;
            sender.send(Ok(StreamItem::RecordBatch(record_batch_2.clone()))).await;
        }

        // Stream the items
        let mut stream = Box::pin(store.stream("test_stream", &[0]).await.unwrap());

        assert_eq!(stream.next().await.unwrap().unwrap(), StreamItem::RecordBatch(record_batch_1));
        assert_eq!(stream.next().await.unwrap().unwrap(), StreamItem::RecordBatch(record_batch_2));
        assert!(stream.next().await.is_none());
    }
}
