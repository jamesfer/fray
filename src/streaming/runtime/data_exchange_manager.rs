use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use std::collections::{HashMap, VecDeque};
use futures::Stream;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use tonic::{Request, Response, Status};
use arrow_flight::Ticket;
use std::net::IpAddr;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;
use std::mem;
use std::ops::{Deref, DerefMut};
use futures_util::TryFutureExt;
use prost::Message;
use crate::flight::{DoGetStream, FlightHandler, FlightServ};
use crate::proto::generated::streaming::StreamingFlightTicketData;
use crate::streaming::action_stream::StreamItem;
use crate::streaming::processor::stream_serialization::encode_stream_to_flight;

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
        // TODO cancel the channel
        let mut status = self.state.status.write().unwrap();
        *status = ChannelStatus::Cancelled {
            last_checkpoint: self.last_checkpoint,
            valid_until: self.next_index,
        };
    }
}

// #[derive(Clone)]
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
    pub fn stream(&self) -> impl Stream<Item=Result<StreamItem, Arc<DataFusionError>>> + Sync + Send + 'static {
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

struct ExchangeChannelStore {
    channels: RwLock<HashMap<String, OutputStream>>,
}

impl ExchangeChannelStore {
    fn new() -> Self {
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
}

pub struct DataExchangeManager {
    listening_address: String,
    exchange_channel_store: Arc<ExchangeChannelStore>,
    close_signal_sender: Option<tokio::sync::oneshot::Sender<()>>,
    handle: JoinHandle<Result<(), DataFusionError>>,
}

impl DataExchangeManager {
    pub async fn start(local_ip_address: IpAddr) -> Result<Self, DataFusionError> {
        let listener = Self::create_listener(local_ip_address).await?;
        let listening_address = listener.local_addr()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .to_string();

        let exchange_channel_store = Arc::new(ExchangeChannelStore::new());
        let exchange_service = DataExchangeFlightService {
            exchange_channel_store: exchange_channel_store.clone(),
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let flight_service_handle = Self::start_flight_service(listener, exchange_service, receiver).await?;

        Ok(Self {
            listening_address,
            exchange_channel_store,
            close_signal_sender: Some(sender),
            handle: flight_service_handle,
        })
    }

    pub fn get_exchange_address(&self) -> &str {
        self.listening_address.as_str()
    }

    pub async fn create_channel(&self, stream_id: String, partitions: Vec<usize>) -> DataChannelSender {
        self.exchange_channel_store.create(stream_id, partitions).await
    }

    async fn create_listener(local_ip_address: IpAddr) -> Result<TcpListener, DataFusionError> {
        TcpListener::bind(&format!("{local_ip_address}:0"))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn start_flight_service(
        listener: TcpListener,
        exchange_service: DataExchangeFlightService,
        close_signal: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<JoinHandle<Result<(), DataFusionError>>, DataFusionError>{
        let flight_server = FlightServiceServer::new(FlightServ {
            handler: Arc::new(exchange_service),
        });

        let close_signal_future = async move {
            // Whether the close signal finishes with an error or not, we will shut down the server
            close_signal.await.unwrap_or(())
        };

        let result = tokio::spawn(
            Server::builder()
                .add_service(flight_server)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    close_signal_future,
                )
                .map_err(|transport_error| DataFusionError::External(Box::new(transport_error)))
        );
        Ok(result)
    }

    pub fn close(&mut self) {
        match self.close_signal_sender.take() {
            None => println!("Close signal sender was already taken, ignoring close request."),
            Some(sender) => {
                match sender.send(()) {
                    Ok(_) => {}
                    Err(_) => println!("Failed to send close signal, it may have already been closed.")
                }
            }
        }
    }
}

struct DataExchangeFlightService {
    exchange_channel_store: Arc<ExchangeChannelStore>,
}

#[async_trait]
impl FlightHandler for DataExchangeFlightService {
    async fn get_stream(&self, request: Request<Ticket>) -> Result<Response<DoGetStream>, Status> {
        let bytes = request.into_inner().ticket;
        let ticket = StreamingFlightTicketData::decode(bytes)
            .map_err(|decode_error| {
                Status::invalid_argument(format!("Failed to decode ticket data: {}", decode_error))
            })?;

        // TODO
        let request_checkpoint = 123usize;
        let request_stream_id = ticket.stream_id;
        let request_partitions = ticket.partitions.into_iter().map(|p| p as usize).collect::<Vec<_>>();

        let data_stream = {
            let channels = self.exchange_channel_store.channels.read().await;
            let channel = channels.get(&request_stream_id)
                .ok_or_else(|| Status::not_found(format!("No channel found for stream id: {}", request_stream_id)))?;
            if channel.partitions != request_partitions {
                return Err(Status::not_found(format!(
                    "Requested partitions {:?} do not match available partitions {:?}",
                    request_partitions, channel.partitions
                )));
            }
            channel.channel.stream()
        };

        Ok(Response::new(Box::pin(encode_stream_to_flight(data_stream))))
    }
}
