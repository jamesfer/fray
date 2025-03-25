// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::error::Error;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow_flight::FlightClient;
use arrow_flight::error::FlightError;
use datafusion::common::{internal_datafusion_err, DataFusionError};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_python::utils::wait_for_future;
use futures::{Stream, TryStreamExt, StreamExt};
use local_ip_address::local_ip;
use log::{debug, error, info, trace};
use tokio::net::TcpListener;

use tonic::transport::Server;
use tonic::{Request, Response, Status, async_trait};

use datafusion::error::Result as DFResult;

use arrow_flight::{Ticket, flight_service_server::FlightServiceServer};
use bytes::Bytes;
use itertools::Itertools;
use pyo3::prelude::*;

use parking_lot::{Mutex, RwLock};
use prost::Message;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use crate::flight::{FlightHandler, FlightServ};
use crate::isolator::PartitionGroup;
use crate::proto::generated::streaming::StreamingFlightTicketData;
use crate::streaming::action_stream::StreamItem;
use crate::streaming::checkpoint_storage_manager::CheckpointStorageManager;
use crate::streaming::input_manager::InputManager;
use crate::streaming::output_manager::OutputManager;
use crate::streaming::processor::flight_data_encoder::{FlightDataEncoderBuilder, FlightDataItem};
use crate::streaming::processor::flight_handler::ProcessorFlightHandler;
use crate::streaming::task_definition::TaskDefinition;
use crate::streaming::task_runner::TaskRunner;
use crate::util::{
    ResultExt, bytes_to_physical_plan, display_plan_with_partition_counts, extract_ticket,
    input_stage_ids, make_client, register_object_store_for_paths_in_plan,
};


struct DFRayStreamingTaskExecutor {
    stage_id: usize,
    output_stream_address_map: HashMap<String, String>,
    task_runner: TaskRunner,
}

impl DFRayStreamingTaskExecutor {
    pub fn start(
        stage_id: usize,
        output_stream_address_map: HashMap<String, String>,
        task_definition: TaskDefinition,
        input_manager: Arc<InputManager>,
        output_manager: Arc<OutputManager>,
        checkpoint_storage_manager: Arc<CheckpointStorageManager>,
    ) -> Result<Self, DataFusionError> {
        let function = task_definition.spec.into_task_function();
        let task_runner = TaskRunner::start(
            format!("stage_{}", stage_id),
            function,
            task_definition.inputs,
            task_definition.output_stream_id,
            task_definition.output_schema,
            task_definition.output_partitioning,
            task_definition.checkpoint_id,
            None,
            input_manager,
            output_manager,
            checkpoint_storage_manager,
        );

        Ok(Self {
            stage_id,
            output_stream_address_map,
            task_runner,
        })
    }
}

struct DFRayStreamingTaskRunner {
    /// our name, useful for logging
    name: String,
    /// Inner state of the handler
    inner: RwLock<Option<DFRayStreamingTaskExecutor>>,
    /// Output channels
    output_manager: Arc<OutputManager>,
    checkpoint_storage_manager: Arc<CheckpointStorageManager>,
}

impl DFRayStreamingTaskRunner {
    pub fn new(name: String, output_manager: Arc<OutputManager>) -> Self {
        let inner = RwLock::new(None);

        Self {
            name,
            inner,
            output_manager,
            checkpoint_storage_manager: Arc::new(CheckpointStorageManager::new()),
        }
    }

    async fn run_task(
        &self,
        stage_id: usize,
        output_stream_address_map: HashMap<String, String>,
        task_definition: TaskDefinition,
    ) -> DFResult<()> {
        let input_manager = Arc::new(InputManager::new(output_stream_address_map.clone()).await?);

        // Wait to start the executor until we can acquire the lock
        let inner_ref = &mut *self.inner.write();
        let inner = DFRayStreamingTaskExecutor::start(
            stage_id,
            output_stream_address_map,
            task_definition,
            input_manager,
            self.output_manager.clone(),
            self.checkpoint_storage_manager.clone(),
        )?;
        inner_ref.replace(inner);

        Ok(())
    }
}

/// DFRayProcessorService is an Arrow Flight service that serves streams of
/// partitions from a hosted Physical Plan
///
/// It only responds to the DoGet Arrow Flight method
#[pyclass]
pub struct DFRayStreamingProcessorService {
    name: String,
    listener: Option<TcpListener>,
    runner: Arc<DFRayStreamingTaskRunner>,
    flight_handler: Arc<ProcessorFlightHandler>,
    addr: Option<String>,
    all_done_tx: Arc<Mutex<Sender<()>>>,
    all_done_rx: Option<Receiver<()>>,
}

#[pymethods]
impl DFRayStreamingProcessorService {
    #[new]
    pub fn new(name: String) -> PyResult<Self> {
        let name = format!("[{}]", name);
        let listener = None;
        let addr = None;

        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        let output_manager = Arc::new(OutputManager::new());
        let runner = Arc::new(DFRayStreamingTaskRunner::new(name.clone(), output_manager.clone()));
        let flight_handler = Arc::new(ProcessorFlightHandler::new(name.clone(), output_manager.clone()));

        Ok(Self {
            name,
            listener,
            runner,
            flight_handler,
            addr,
            all_done_tx,
            all_done_rx: Some(all_done_rx),
        })
    }

    /// bind the listener to a socket.  This method must complete
    /// before any other methods are called.   This is separate
    /// from new() because Ray does not let you wait (AFAICT) on Actor inits to complete
    /// and we will want to wait on this with ray.get()
    pub fn start_up(&mut self, py: Python) -> PyResult<()> {
        let my_local_ip = local_ip().to_py_err()?;
        let my_host_str = format!("{my_local_ip}:0");

        self.listener = Some(wait_for_future(py, TcpListener::bind(&my_host_str)).to_py_err()?);

        self.addr = Some(format!(
            "{}",
            self.listener.as_ref().unwrap().local_addr().unwrap()
        ));

        Ok(())
    }

    /// get the address of the listing socket for this service
    pub fn addr(&self) -> PyResult<String> {
        self.addr.clone().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "{},Couldn't get addr",
                self.name
            ))
        })
    }

    /// signal to the service that we can shutdown
    ///
    /// returns a python coroutine that should be awaited
    pub fn all_done<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let sender = self.all_done_tx.lock().clone();

        let fut = async move {
            sender.send(()).await.to_py_err()?;
            Ok(())
        };
        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }

    /// replace the plan that this service was providing, we will do this when we want
    /// to reuse the DFRayProcessorService for a subsequent query
    ///
    /// returns a python coroutine that should be awaited
    pub fn update_plan<'a>(
        &self,
        py: Python<'a>,
        stage_id: usize,
        output_stream_address_map: HashMap<String, String>,
        plan_bytes: &[u8],
    ) -> PyResult<Bound<'a, PyAny>> {

        debug!(
            "{} Received New Plan: Stage:{} my addr: {}, output address map:\n{:?}\nplan:\nTODO",
            self.name,
            stage_id,
            self.addr()?,
            output_stream_address_map,
            // display_plan_with_partition_counts(&plan)
        );

        let task_def = TaskDefinition::try_decode_from_bytes(plan_bytes, &SessionContext::new())?;
        let handler = self.runner.clone();
        let name = self.name.clone();
        let fut = async move {
            handler
                .run_task(stage_id, output_stream_address_map, task_def)
                .await
                .to_py_err()?;
            info!(
                "{} [stage: {}] updated plan",
                name, stage_id
            );
            Ok(())
        };

        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }

    /// start the service
    /// returns a python coroutine that should be awaited
    pub fn serve<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let mut all_done_rx = self.all_done_rx.take().unwrap();

        let signal = async move {
            all_done_rx
                .recv()
                .await
                .expect("problem receiving shutdown signal");
        };

        let service = FlightServ {
            handler: self.flight_handler.clone(),
        };
        let svc = FlightServiceServer::new(service);

        let listener = self.listener.take().unwrap();
        let name = self.name.clone();

        let serv = async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    signal,
                )
                .await
                .inspect_err(|e| error!("{}, ERROR serving {e}", name))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{e}")))?;
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        };

        let fut = async move {
            serv.await.to_py_err()?;
            Ok(())
        };

        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }
}
