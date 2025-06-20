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

use arrow::array::RecordBatch;
use arrow::pyarrow::ToPyArrow;
use datafusion::common::internal_datafusion_err;
use datafusion::common::internal_err;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::{displayable, PlanProperties};
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::DataFrame;
use datafusion_python::errors::PyDataFusionError;
use datafusion_python::physical_plan::PyExecutionPlan;
use datafusion_python::sql::logical::PyLogicalPlan;
use datafusion_python::utils::wait_for_future;
use futures::stream::StreamExt;
use itertools::Itertools;
use log::trace;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use std::borrow::Cow;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::isolator::PartitionIsolatorExec;
use crate::max_rows::MaxRowsExec;
use crate::pre_fetch::PrefetchExec;
use crate::stage::DFRayStageExec;
use crate::stage_reader::DFRayStageReaderExec;
use crate::util::ResultExt;
use crate::util::collect_from_stage;
use crate::util::display_plan_with_partition_counts;
use crate::util::physical_plan_to_bytes;

/// Internal rust class beyind the DFRayDataFrame python object
///
/// It is a container for a plan for a query, as we would expect.
///
/// This class plays two important roles.  First, it defines the stages of the plan
/// by walking the plan provided to us in the constructor inside our dataframe.
/// That plan contains RayStageExec nodes, where are merely markers, that incidate to us where
/// to split the plan into descrete stages that can be hosted by a StageService.
///
/// The second role of this object is to be able to fetch record batches from the final_
/// stage in the plan and return them to python.
#[pyclass]
pub struct DFRayDataFrame {
    /// holds the logical plan of the query we will execute
    df: DataFrame,
    /// the physical plan we will use to consume the final stage.
    /// created when stages is run
    final_plan: Option<Arc<dyn ExecutionPlan>>,
}

// impl DFRayDataFrame {
//     pub fn new(df: DataFrame) -> Self {
//         Self {
//             df,
//             final_plan: None,
//         }
//     }
// }
//
// #[pymethods]
// impl DFRayDataFrame {
//     fn streaming_stages(
//         &mut self,
//         py: Python,
//     ) -> PyResult<Vec<PyDFRayStreamingStage>> {
//         // Translate the plan from datafusion into tasks
//         let physical_plan = wait_for_future(py, self.df.clone().create_physical_plan())?;
//         let tasks = translate_physical_plan(&physical_plan)?;
//
//         let stages = tasks.into_iter()
//             .enumerate()
//             .map(|(stage_id, task)| {
//                 PyDFRayStreamingStage::new(
//                     stage_id,
//                     task.output_stream_id.clone(),
//                     task,
//                     false,
//                 )
//             })
//             .collect::<Vec<_>>();
//
//         let reader_plan = Arc::new(DFRayStageReaderExec::try_new_from_input(
//             physical_plan,
//             stages[0].stage_id,
//         )?) as Arc<dyn ExecutionPlan>;
//
//         self.final_plan = Some(reader_plan);
//
//         Ok(stages)
//     }
//
//     #[pyo3(signature = (batch_size, prefetch_buffer_size, partitions_per_worker=None))]
//     fn stages(
//         &mut self,
//         py: Python,
//         batch_size: usize,
//         prefetch_buffer_size: usize,
//         partitions_per_worker: Option<usize>,
//     ) -> PyResult<Vec<PyDFRayStage>> {
//         let mut stages = vec![];
//
//         let mut partition_groups = vec![];
//         let mut full_partitions = false;
//         // We walk up the tree from the leaves to find the stages, record ray stages, and replace
//         // each ray stage with a corresponding ray reader stage.
//         let up = |plan: Arc<dyn ExecutionPlan>| {
//             trace!(
//                 "Examining plan up: {}",
//                 displayable(plan.as_ref()).one_line()
//             );
//
//             if let Some(stage_exec) = plan.as_any().downcast_ref::<DFRayStageExec>() {
//                 trace!("ray stage exec");
//                 let input = plan.children();
//                 assert!(input.len() == 1, "RayStageExec must have exactly one child");
//                 let input = input[0];
//
//                 let replacement = Arc::new(DFRayStageReaderExec::try_new(
//                     plan.output_partitioning().clone(),
//                     input.schema(),
//                     stage_exec.stage_id,
//                 )?) as Arc<dyn ExecutionPlan>;
//
//                 let stage = PyDFRayStage::new(
//                     stage_exec.stage_id,
//                     input.clone(),
//                     partition_groups.clone(),
//                     full_partitions,
//                 );
//                 partition_groups = vec![];
//                 full_partitions = false;
//
//                 stages.push(stage);
//                 Ok(Transformed::yes(replacement))
//             } else if plan.as_any().downcast_ref::<RepartitionExec>().is_some() {
//                 trace!("repartition exec");
//                 let (calculated_partition_groups, replacement) = build_replacement(
//                     plan,
//                     prefetch_buffer_size,
//                     partitions_per_worker,
//                     true,
//                     batch_size,
//                     batch_size,
//                 )?;
//                 partition_groups = calculated_partition_groups;
//
//                 Ok(Transformed::yes(replacement))
//             } else if plan.as_any().downcast_ref::<SortExec>().is_some() {
//                 trace!("sort exec");
//                 let (calculated_partition_groups, replacement) = build_replacement(
//                     plan,
//                     prefetch_buffer_size,
//                     partitions_per_worker,
//                     false,
//                     batch_size,
//                     batch_size,
//                 )?;
//                 partition_groups = calculated_partition_groups;
//                 full_partitions = true;
//
//                 Ok(Transformed::yes(replacement))
//             } else if plan.as_any().downcast_ref::<NestedLoopJoinExec>().is_some() {
//                 trace!("nested loop join exec");
//                 // NestedLoopJoinExec must be on a stage by itself as it materializes the entire left
//                 // side of the join and is not suitable to be executed in a partitioned manner.
//                 let mut replacement = plan.clone();
//                 let partition_count = plan.output_partitioning().partition_count();
//                 trace!("nested join output partitioning {}", partition_count);
//
//                 replacement = Arc::new(MaxRowsExec::new(
//                     Arc::new(CoalesceBatchesExec::new(replacement, batch_size))
//                         as Arc<dyn ExecutionPlan>,
//                     batch_size,
//                 )) as Arc<dyn ExecutionPlan>;
//
//                 if prefetch_buffer_size > 0 {
//                     replacement = Arc::new(PrefetchExec::new(replacement, prefetch_buffer_size))
//                         as Arc<dyn ExecutionPlan>;
//                 }
//                 partition_groups = vec![(0..partition_count).collect()];
//                 full_partitions = true;
//                 Ok(Transformed::yes(replacement))
//             } else {
//                 trace!("not special case");
//                 Ok(Transformed::no(plan))
//             }
//         };
//
//         let physical_plan = wait_for_future(py, self.df.clone().create_physical_plan())?;
//
//         physical_plan.transform_up(up)?;
//
//         // add coalesce and max rows to last stage
//         let mut last_stage = stages
//             .pop()
//             .ok_or(internal_datafusion_err!("No stages found"))?;
//
//         if last_stage.num_output_partitions() > 1 {
//             return internal_err!("Last stage expected to have one partition").to_py_err();
//         }
//
//         last_stage = PyDFRayStage::new(
//             last_stage.stage_id,
//             Arc::new(MaxRowsExec::new(
//                 Arc::new(CoalesceBatchesExec::new(last_stage.plan, batch_size))
//                     as Arc<dyn ExecutionPlan>,
//                 batch_size,
//             )) as Arc<dyn ExecutionPlan>,
//             vec![vec![0]],
//             true,
//         );
//
//         // done fixing last stage
//
//         let reader_plan = Arc::new(DFRayStageReaderExec::try_new_from_input(
//             last_stage.plan.clone(),
//             last_stage.stage_id,
//         )?) as Arc<dyn ExecutionPlan>;
//
//         stages.push(last_stage);
//
//         self.final_plan = Some(reader_plan);
//
//         Ok(stages)
//     }
//
//     fn execution_plan(&self, py: Python) -> PyResult<PyExecutionPlan> {
//         let plan = wait_for_future(py, self.df.clone().create_physical_plan())?;
//         Ok(PyExecutionPlan::new(plan))
//     }
//
//     fn display_execution_plan(&self, py: Python) -> PyResult<String> {
//         let plan = wait_for_future(py, self.df.clone().create_physical_plan())?;
//         Ok(display_plan_with_partition_counts(&plan).to_string())
//     }
//
//     fn logical_plan(&self) -> PyResult<PyLogicalPlan> {
//         Ok(PyLogicalPlan::new(self.df.logical_plan().clone()))
//     }
//
//     fn schema(&self, py: Python) -> PyResult<PyObject> {
//         self.df.schema().as_arrow().to_pyarrow(py)
//     }
//
//     fn optimized_logical_plan(&self) -> PyResult<PyLogicalPlan> {
//         Ok(PyLogicalPlan::new(self.df.clone().into_optimized_plan()?))
//     }
//
//     fn read_final_stage(
//         &mut self,
//         py: Python,
//         stage_id: usize,
//         stage_addr: &str,
//     ) -> PyResult<PyRecordBatchStream> {
//         Err(PyErr::new(
//             internal_datafusion_err!("read_final_stage is not implemented, use read_final_streaming_stage instead"),
//         ))
//         // wait_for_future(
//         //     py,
//         //     collect_from_stage(
//         //         stage_id,
//         //         0,
//         //         stage_addr,
//         //         self.final_plan.take().unwrap().clone(),
//         //     ),
//         // )
//         // .map(PyRecordBatchStream::new)
//         // .to_py_err()
//     }
//
//     fn read_final_streaming_stage(
//         &mut self,
//         py: Python,
//         stage_id: usize,
//         address: &str,
//         output_stream_id: &str,
//     ) -> PyResult<PyRecordBatchStream> {
//         wait_for_future(
//             py,
//             collect_from_stage_streaming(
//                 stage_id,
//                 address,
//                 output_stream_id,
//                 self.df.schema().inner().clone(),
//             ),
//         )
//         .map(PyRecordBatchStream::new)
//         .to_py_err()
//     }
// }
//
// #[allow(clippy::type_complexity)]
// fn build_replacement(
//     plan: Arc<dyn ExecutionPlan>,
//     prefetch_buffer_size: usize,
//     partitions_per_worker: Option<usize>,
//     isolate: bool,
//     max_rows: usize,
//     inner_batch_size: usize,
// ) -> Result<(Vec<Vec<usize>>, Arc<dyn ExecutionPlan>), DataFusionError> {
//     let mut replacement = plan.clone();
//     let children = plan.children();
//     assert!(children.len() == 1, "Unexpected plan structure");
//
//     let child = children[0];
//     let partition_count = child.output_partitioning().partition_count();
//     trace!(
//         "build_replacement for {}, partition_count: {}",
//         displayable(plan.as_ref()).one_line(),
//         partition_count
//     );
//
//     let partition_groups = match partitions_per_worker {
//         Some(p) => (0..partition_count)
//             .chunks(p)
//             .into_iter()
//             .map(|chunk| chunk.collect())
//             .collect(),
//         None => vec![(0..partition_count).collect()],
//     };
//
//     if isolate && partition_groups.len() > 1 {
//         let new_child = Arc::new(PartitionIsolatorExec::new(
//             child.clone(),
//             partitions_per_worker.unwrap(), // we know it is a Some, here.
//         ));
//         replacement = replacement.clone().with_new_children(vec![new_child])?;
//     }
//     // insert a coalescing batches here too so that we aren't sending
//     // too small (or too big) of batches over the network
//     replacement = Arc::new(MaxRowsExec::new(
//         Arc::new(CoalesceBatchesExec::new(replacement, inner_batch_size)) as Arc<dyn ExecutionPlan>,
//         max_rows,
//     )) as Arc<dyn ExecutionPlan>;
//
//     if prefetch_buffer_size > 0 {
//         replacement = Arc::new(PrefetchExec::new(replacement, prefetch_buffer_size))
//             as Arc<dyn ExecutionPlan>;
//     }
//
//     Ok((partition_groups, replacement))
// }
//
// /// A Python class to hold a PHysical plan of a single stage
// #[pyclass]
// pub struct PyDFRayStage {
//     /// our stage id
//     stage_id: usize,
//     /// the physical plan of our stage
//     plan: Arc<dyn ExecutionPlan>,
//     /// the partition groups for this stage.
//     partition_groups: Vec<Vec<usize>>,
//     /// Are we hosting the complete partitions?  If not
//     /// then RayStageReaderExecs will be inserted to consume its desired partition
//     /// from all stages with this same id, and merge the results.  Using a
//     /// CombinedRecordBatchStream
//     full_partitions: bool,
// }
//
// impl PyDFRayStage {
//     fn new(
//         stage_id: usize,
//         plan: Arc<dyn ExecutionPlan>,
//         partition_groups: Vec<Vec<usize>>,
//         full_partitions: bool,
//     ) -> Self {
//         Self {
//             stage_id,
//             plan,
//             partition_groups,
//             full_partitions,
//         }
//     }
// }
//
// #[pymethods]
// impl PyDFRayStage {
//     #[getter]
//     fn stage_id(&self) -> usize {
//         self.stage_id
//     }
//
//     #[getter]
//     fn partition_groups(&self) -> Vec<Vec<usize>> {
//         self.partition_groups.clone()
//     }
//
//     #[getter]
//     fn full_partitions(&self) -> bool {
//         self.full_partitions
//     }
//
//     /// returns the number of output partitions of this stage
//     #[getter]
//     fn num_output_partitions(&self) -> usize {
//         self.plan.output_partitioning().partition_count()
//     }
//
//     /// returns the stage ids of that we need to read from in order to execute
//     #[getter]
//     pub fn child_stage_ids(&self) -> PyResult<Vec<usize>> {
//         let mut result = vec![];
//         self.plan
//             .clone()
//             .transform_down(|node: Arc<dyn ExecutionPlan>| {
//                 if let Some(reader) = node.as_any().downcast_ref::<DFRayStageReaderExec>() {
//                     result.push(reader.stage_id);
//                 }
//                 Ok(Transformed::no(node))
//             })?;
//         Ok(result)
//     }
//
//     pub fn execution_plan(&self) -> PyExecutionPlan {
//         PyExecutionPlan::new(self.plan.clone())
//     }
//
//     fn display_execution_plan(&self) -> PyResult<String> {
//         Ok(display_plan_with_partition_counts(&self.plan).to_string())
//     }
//
//     pub fn plan_bytes(&self) -> PyResult<Cow<[u8]>> {
//         let plan_bytes = physical_plan_to_bytes(self.plan.clone())?;
//         Ok(Cow::Owned(plan_bytes))
//     }
// }
//
// /// A Python class to hold a streaming plan
// #[pyclass]
// pub struct PyDFRayStreamingStage {
//     /// our stage id
//     stage_id: usize,
//     output_stream_id: String,
//     /// the physical plan of our stage
//     task: TaskDefinition,
//     /// Are we hosting the complete partitions?  If not
//     /// then RayStageReaderExecs will be inserted to consume its desired partition
//     /// from all stages with this same id, and merge the results.  Using a
//     /// CombinedRecordBatchStream
//     full_partitions: bool,
// }
//
// impl PyDFRayStreamingStage {
//     fn new(
//         stage_id: usize,
//         output_stream_id: String,
//         task: TaskDefinition,
//         full_partitions: bool,
//     ) -> Self {
//         Self {
//             stage_id,
//             output_stream_id,
//             task,
//             full_partitions,
//         }
//     }
// }
//
// #[pymethods]
// impl PyDFRayStreamingStage {
//     #[getter]
//     fn stage_id(&self) -> usize {
//         self.stage_id
//     }
//
//     #[getter]
//     fn output_stream_id(&self) -> String {
//         self.output_stream_id.clone()
//     }
//
//     #[getter]
//     fn full_partitions(&self) -> bool {
//         self.full_partitions
//     }
//
//     // /// returns the stage ids of that we need to read from in order to execute
//     // #[getter]
//     // pub fn child_stage_ids(&self) -> PyResult<Vec<usize>> {
//     //     let mut result = vec![];
//     //     self.plan
//     //         .clone()
//     //         .transform_down(|node: Arc<dyn ExecutionPlan>| {
//     //             if let Some(reader) = node.as_any().downcast_ref::<DFRayStageReaderExec>() {
//     //                 result.push(reader.stage_id);
//     //             }
//     //             Ok(Transformed::no(node))
//     //         })?;
//     //     Ok(result)
//     // }
//
//     // pub fn execution_plan(&self) -> PyExecutionPlan {
//     //     PyExecutionPlan::new(self.plan.clone())
//     // }
//     //
//     // fn display_execution_plan(&self) -> PyResult<String> {
//     //     Ok(display_plan_with_partition_counts(&self.plan).to_string())
//     // }
//
//     pub fn plan_bytes(&self) -> PyResult<Cow<[u8]>> {
//         Ok(Cow::Owned(self.task.encode_to_bytes()))
//     }
// }

// PyRecordBatch and PyRecordBatchStream are borrowed, and slightly modified from datafusion-python
// they are not publicly exposed in that repo

#[pyclass]
pub struct PyRecordBatch {
    pub batch: RecordBatch,
}

#[pymethods]
impl PyRecordBatch {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        self.batch.to_pyarrow(py)
    }
}

impl From<RecordBatch> for PyRecordBatch {
    fn from(batch: RecordBatch) -> Self {
        Self { batch }
    }
}

// #[pyclass]
// pub struct PyRecordBatchStream {
//     stream: Arc<Mutex<SendableRecordBatchStream>>,
// }
//
// impl PyRecordBatchStream {
//     pub fn new(stream: SendableRecordBatchStream) -> Self {
//         Self {
//             stream: Arc::new(Mutex::new(stream)),
//         }
//     }
// }
//
// #[pymethods]
// impl PyRecordBatchStream {
//     fn next(&mut self, py: Python) -> PyResult<PyObject> {
//         let stream = self.stream.clone();
//         wait_for_future(py, next_stream(stream, true)).and_then(|b| b.to_pyarrow(py))
//     }
//
//     fn __next__(&mut self, py: Python) -> PyResult<PyObject> {
//         self.next(py)
//     }
//
//     fn __anext__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
//         let stream = self.stream.clone();
//         pyo3_async_runtimes::tokio::future_into_py(py, next_stream(stream, false))
//     }
//
//     fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
//         slf
//     }
//
//     fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
//         slf
//     }
// }
//
// async fn next_stream(
//     stream: Arc<Mutex<SendableRecordBatchStream>>,
//     sync: bool,
// ) -> PyResult<PyRecordBatch> {
//     let mut stream = stream.lock().await;
//     match stream.next().await {
//         Some(Ok(batch)) => Ok(batch.into()),
//         Some(Err(e)) => Err(PyDataFusionError::from(e))?,
//         None => {
//             // Depending on whether the iteration is sync or not, we raise either a
//             // StopIteration or a StopAsyncIteration
//             if sync {
//                 Err(PyStopIteration::new_err("stream exhausted"))
//             } else {
//                 Err(PyStopAsyncIteration::new_err("stream exhausted"))
//             }
//         }
//     }
// }
