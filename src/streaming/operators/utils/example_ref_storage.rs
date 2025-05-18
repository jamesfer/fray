use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use futures_util::stream::iter;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use tokio::sync::Mutex;

enum StoreItem<T> {
    Waiting(tokio::sync::oneshot::Sender<T>),
    Stored(Option<T>),
}

pub struct Store<T> {
    map: Mutex<HashMap<String, StoreItem<T>>>,
}

impl <T> Store<T> {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub async fn insert(&self, key: String, value: T) -> Result<(), DataFusionError> {
        let mut map = self.map.lock().await;
        match map.entry(key) {
            Entry::Occupied(_) => Err(internal_datafusion_err!("Spot in store already taken")),
            Entry::Vacant(slot) => {
                slot.insert(StoreItem::Stored(Some(value)));
                Ok(())
            },
        }
    }

    pub async fn take(&self, key: String) -> Result<T, DataFusionError> {
        let receiver = {
            let mut map = self.map.lock().await;
            match map.entry(key.clone()) {
                Entry::Occupied(mut slot) => {
                    return match slot.get_mut() {
                        StoreItem::Stored(option) => {
                            option.take()
                                .ok_or(internal_datafusion_err!("Another operator already took this value {}", key))
                        },
                        StoreItem::Waiting(_) => {
                            Err(internal_datafusion_err!("Another operator was already waiting for item {}", key))
                        }
                    }
                },
                Entry::Vacant(slot) => {
                    let (sender, receiver) = tokio::sync::oneshot::channel();
                    slot.insert(StoreItem::Waiting(sender));
                    receiver
                }
            }
        };

        receiver.await.map_err(|_| internal_datafusion_err!("Receive error"))
    }
}

async fn m() {
    let a = String::from("a");
    let b = String::from("b");

    {
        let c = String::from("c");

        // The store always lasts for less time than the strings
        let mut store = Store::new();

        store.insert("a".to_string(), &a).await.unwrap();
        store.insert("b".to_string(), &b).await.unwrap();
        store.insert("c".to_string(), &c).await.unwrap();

        store.take("a".to_string()).await.unwrap();
        store.take("b".to_string()).await.unwrap();
        store.take("c".to_string()).await.unwrap();
    }

    println!("{} {}", a, b);
}

async fn n() {
    let s1 = iter(vec![1, 2, 3]);
    let s2 = iter(vec![4, 5, 6]);

    {
        let s3 = iter(vec![7, 8, 9]);

        // The store always lasts for less time than the strings
        let mut store = Store::new();

        store.insert("a".to_string(), &s1).await.unwrap();
        store.insert("b".to_string(), &s2).await.unwrap();
        store.insert("c".to_string(), &s3).await.unwrap();

        store.take("a".to_string()).await.unwrap();
        store.take("b".to_string()).await.unwrap();
        store.take("c".to_string()).await.unwrap();
    }
}
