use tokio::sync::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use arrow_flight::FlightClient;
use datafusion::common::DataFusionError;
use std::collections::hash_map::Entry;
use crate::util::make_client;

pub struct DataClientManager {
    client_map: RwLock<HashMap<String, Arc<Mutex<FlightClient>>>>,
}

impl DataClientManager {
    pub fn new() -> Self {
        Self {
            client_map: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_client(&self, address: &str) -> Result<Arc<Mutex<FlightClient>>, DataFusionError> {
        // Try to find an existing client
        {
            let client_map = self.client_map.read().await;
            if let Some(client) = client_map.get(address) {
                return Ok(client.clone());
            }
        }

        // Otherwise, create a new one
        let new_client = Arc::new(Mutex::new(make_client(address).await?));
        let mut client_map = self.client_map.write().await;
        match client_map.entry(address.to_string()) {
            Entry::Occupied(occupied) => {
                // Return the existing client and drop the one we just created
                Ok(occupied.get().clone())
            },
            Entry::Vacant(vacant) => {
                vacant.insert(new_client.clone());
                Ok(new_client)
            }
        }
    }
}
