use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::sync::atomic::AtomicI32;
use std::task::{Context, Poll};
use futures::Stream;
use futures_util::FutureExt;
use pin_project::pin_project;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Mutex, MutexGuard, TryLockError};

struct SharedConstructorState<F> {
    f: F,
    senders: Vec<(usize, Vec<tokio::sync::oneshot::Sender<usize>>)>,
}

struct SharedConstructorReceiver<F> {
    shared_state: Arc<SharedConstructorState<F>>,
    receiver: tokio::sync::oneshot::Receiver<usize>,
}

fn lazily_evaluate_constructor<T, F, Fut, S>(sizes: Vec<(usize, usize)>, constructor: F) -> Vec<(usize, Vec<impl Stream<Item=T>>)>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output=Vec<(usize, Vec<S>)>>,
    S: Stream<Item=T>
{
    // Create each of the senders and receivers
    let (senders, receivers) = sizes.into_iter()
        .map(|(ordinal, count)| {
            let (senders, receivers) = (0..count)
                .map(|_| tokio::sync::oneshot::channel())
                .unzip::<_, _, Vec<_>, Vec<_>>();
            ((ordinal, senders), (ordinal, receivers))
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();

    let run_constructor = Arc::new(move || async {
        let mut sender_map = senders.into_iter()
            .collect::<HashMap<_, _>>();

        for (ordinal, streams) in constructor().await {
            let channels = sender_map.remove(&ordinal).unwrap();
            for (channel, stream) in channels.into_iter().zip(streams.into_iter()) {
                // Send the stream to the receiver
                channel.send(stream)
                    .map_err(|_| "Failed to send stream")
                    .unwrap();
            }
        }
    });

    let length = receivers.len();
    receivers.into_iter()
        .zip(vec![run_constructor; length])
        .map(|((ordinal, receivers), run_constructor)| {
            let length = receivers.len();
            let streams = receivers.into_iter()
                .zip(vec![run_constructor; length])
                .map(|(receiver, run_constructor)| {
                    (async move {
                        // Attempt to consume the arc if we are the last one to use it
                        if let Some(constructor) = Arc::into_inner(run_constructor) {
                            constructor().await;
                        }

                        // Wait for the receiver to be ready
                        receiver.await.unwrap()
                    }).flatten_stream()
                })
                .collect();

            (ordinal, streams)
        })
        .collect()
}

#[cfg(test)]
mod lazy_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use futures_util::{poll, StreamExt};
    use tokio::time::sleep;
    use tokio_stream::{iter, once};
    use crate::streaming::operators::utils::lazy_operator::lazily_evaluate_constructor;

    #[tokio::test]
    async fn works() {
        let func_called = Arc::new(AtomicBool::new(false));
        let mut streams = lazily_evaluate_constructor::<usize, _, _, _>(vec![(0, 2), (1, 1)], {
            let func_called = func_called.clone();
            move || {
                func_called.store(true, Ordering::Relaxed);

                async move {
                    vec![
                        (0, vec![once(1), once(2)]),
                        (1, vec![once(3)]),
                    ]
                }
            }
        });

        // Check dimensions of return streams
        assert_eq!(streams.len(), 2);
        assert_eq!(streams[0].1.len(), 2);
        assert_eq!(streams[1].1.len(), 1);

        let stream1 = streams[0].1.pop().unwrap();
        let stream0 = streams[0].1.pop().unwrap();
        let stream2 = streams[1].1.pop().unwrap();

        let task0 = tokio::spawn(async {
            stream0.collect::<Vec<_>>().await
        });
        sleep(Duration::from_millis(100)).await;
        assert_eq!(func_called.load(Ordering::Relaxed), false);

        let task1 = tokio::spawn(async {
            stream1.collect::<Vec<_>>().await
        });
        sleep(Duration::from_millis(100)).await;
        assert_eq!(func_called.load(Ordering::Relaxed), false);

        let task2 = tokio::spawn(async {
            stream2.collect::<Vec<_>>().await
        });
        sleep(Duration::from_millis(100)).await;
        assert_eq!(func_called.load(Ordering::Relaxed), true);

        let (results0, results1, results2) = tokio::try_join!(task0, task1, task2).unwrap();
        assert_eq!(results0, vec![1]);
        assert_eq!(results1, vec![2]);
        assert_eq!(results2, vec![3]);
    }
}



// struct SharedStateNoSend {
//     future: RefCell<Box<dyn Future<Output=()>>>,
// }
//
// struct SharedState {
//     future: Mutex<Box<dyn Future<Output=()>>>,
// }
//
// // TODO probably don't need pin project since receiver is unpin
// #[pin_project]
// struct SingleStream<T> {
//     shared_state: Arc<SharedState>,
//     future_is_complete: bool,
//     #[pin]
//     receiver: tokio::sync::mpsc::Receiver<T>
// }
//
// impl <T> SingleStream<T> {
//     async fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
//         let mut this = self.project();
//
//         // If the future is complete, we just need to read from the receiver until it is finished
//         if *this.future_is_complete {
//             return this.receiver.poll_recv(cx);
//         }
//
//         // First check if there is an item waiting for us
//         match this.receiver.try_recv() {
//             Ok(item) => {
//                 // If we have an item, return it
//                 return Poll::Ready(Some(item));
//             },
//             Err(TryRecvError::Disconnected) => {
//                 // If the channel is closed, return None
//                 return Poll::Ready(None);
//             },
//             // Otherwise the channel doesn't have anything in it right now, so we can attempt to
//             // progress the future
//             Err(TryRecvError::Empty) => {},
//         }
//
//
//         // Try to lock the future and poll it
//         let complete_result = this.shared_state.future.try_lock()
//             // If the future returns ready for any reason, it is complete
//             .map(|locked| matches!(locked.poll(cx), Poll::Ready(_)));
//         if let Ok(complete) = complete_result {
//             this.future_is_complete = complete;
//         }
//
//         // In the meantime we can also poll our receiver, in case another thread
//         // makes progress on the future and something arrives for us to process
//         this.receiver.poll_recv(cx)
//     }
// }
//
// fn make_shared_split_stream<T>(
//     stream_counts: Vec<(usize, usize)>,
//     f: impl FnOnce(Vec<(usize, Vec<tokio::sync::mpsc::Sender<T>>)>) -> Box<dyn Future<Output=()>>,
// ) -> Vec<(usize, Vec<SingleStream<T>>)> {
//     let (senders, receivers) = stream_counts.into_iter()
//         .map(|(ordinal, count)| {
//             let (senders, receivers) = (0..count)
//                 .map(|_| tokio::sync::mpsc::channel(1))
//                 .unzip::<_, _, Vec<_>, Vec<_>>();
//             ((ordinal, senders), (ordinal, receivers))
//         })
//         .unzip::<_, _, Vec<_>, Vec<_>>();
//
//     let shared_state = Arc::new(SharedState {
//         future: Mutex::new(f(senders)),
//     });
//
//     receivers.into_iter()
//         .map(|(ordinal, receivers)| {
//             let streams = receivers.into_iter()
//                 .map(|receiver| SingleStream {
//                     shared_state: shared_state.clone(),
//                     future_is_complete: false,
//                     receiver,
//                 })
//                 .collect();
//             (ordinal, streams)
//         })
//         .collect()
// }
//
// #[cfg(test)]
// mod tests {
//     use crate::streaming::operators::utils::lazy_operator::make_shared_split_stream;
//
//     #[test]
//     fn works() {
//         let receivers = make_shared_split_stream(vec![(0, 2), (1, 1)], |senders| {
//             assert_eq!(senders.len(), 2);
//             assert_eq!(senders[0].1.len(), 2);
//             assert_eq!(senders[1].1.len(), 1);
//
//             async move {
//                 senders[0].1[0].send(1).await.unwrap();
//                 senders[0].1[0].send(2).await.unwrap();
//                 senders[0].1[1].send(3).await.unwrap();
//                 senders[1].1[0].send(4).await.unwrap();
//                 senders[0].1[0].send(5).await.unwrap();
//             }
//         });
//
//
//     }
// }
//
// // pub fn make_lazy_operator<T, F, Fut>(stream_counts: Vec<(usize, usize)>, f: F) -> Vec<(usize, Vec<impl Stream<Item=T>>)>
// // where
// //     F: FnOnce() -> Fut,
// //     Fut: Future<Output=()>,
// // {
// //
// // }
