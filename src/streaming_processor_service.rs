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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow_flight::error::FlightError;
use arrow_flight::FlightClient;
use datafusion::common::{internal_datafusion_err, DataFusionError};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_python::utils::wait_for_future;
use futures::{Stream, StreamExt, TryStreamExt};
use local_ip_address::local_ip;
use log::{debug, error, info, trace};
use tokio::net::TcpListener;

use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status};

use datafusion::error::Result as DFResult;

use arrow_flight::{flight_service_server::FlightServiceServer, Ticket};
use bytes::Bytes;
use itertools::Itertools;
use pyo3::prelude::*;

use crate::flight::{FlightHandler, FlightServ};
use crate::streaming::worker_process::WorkerProcess;
use crate::util::{
    bytes_to_physical_plan, display_plan_with_partition_counts, extract_ticket, input_stage_ids,
    make_client, register_object_store_for_paths_in_plan, ResultExt,
};
use parking_lot::{Mutex, RwLock};
use prost::Message;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// DFRayProcessorService is an Arrow Flight service that serves streams of
/// partitions from a hosted Physical Plan
///
/// It only responds to the DoGet Arrow Flight method
#[pyclass]
pub struct DFRayStreamingProcessorService {
    name: String,
    listener: Option<TcpListener>,
    worker_process: Arc<WorkerProcess>,
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

        let worker_process = Arc::new(WorkerProcess::new(name.clone()));

        Ok(Self {
            name,
            listener,
            worker_process,
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
        let task_def = self.worker_process.parse_task_definition(plan_bytes).to_py_err()?;

        debug!(
            "{} Received New Plan: Stage:{} my addr: {}, output address map:\n{:?}\nplan:\nTODO",
            self.name,
            stage_id,
            self.addr()?,
            output_stream_address_map,
            // display_plan_with_partition_counts(&plan)
        );

        let worker = self.worker_process.clone();
        let name = self.name.clone();
        let fut = async move {
            worker
                // TODO add input spec
                .start_task(stage_id, output_stream_address_map, task_def)
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
            handler: self.worker_process.make_flight_handler(),
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
