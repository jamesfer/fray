use datafusion::common::DataFusionError;
use std::net::IpAddr;
use crate::streaming::runtime::data_client_manager::DataClientManager;
use crate::streaming::runtime::data_exchange_manager::DataExchangeManager;

pub struct Runtime {
    data_client_manager: DataClientManager,
    data_exchange_manager: DataExchangeManager,
}

impl Runtime {
    pub async fn start(local_ip_address: IpAddr) -> Result<Self, DataFusionError> {
        let data_exchange_manager = DataExchangeManager::start(local_ip_address).await?;
        Ok(Self {
            data_client_manager: DataClientManager::new(),
            data_exchange_manager,
        })
    }

    pub fn data_client_manager(&self) -> &DataClientManager {
        &self.data_client_manager
    }
    pub fn data_exchange_manager(&self) -> &DataExchangeManager {
        &self.data_exchange_manager
    }
}
