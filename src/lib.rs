// #![feature(iterator_try_reduce)]

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

extern crate core;

use pyo3::prelude::*;
use std::env;

mod proto;
pub use proto::generated::protobuf;

pub mod codec;
pub mod context;
pub mod dataframe;
pub mod flight;
pub mod isolator;
pub mod max_rows;
pub mod physical;
pub mod pre_fetch;
pub mod processor_service;
pub mod stage;
pub mod stage_reader;
pub mod streaming;
pub mod util;
mod python;
// #[pymodule]
// fn _datafusion_ray_internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
//     setup_logging();
//     m.add_class::<context::DFRayContext>()?;
//     m.add_class::<dataframe::DFRayDataFrame>()?;
//     m.add_class::<dataframe::PyDFRayStage>()?;
//     m.add_class::<processor_service::DFRayProcessorService>()?;
//     m.add_class::<streaming_processor_service::DFRayStreamingProcessorService>()?;
//     m.add_class::<util::LocalValidator>()?;
//     m.add_function(wrap_pyfunction!(util::prettify, m)?)?;
//     m.add_function(wrap_pyfunction!(entrypoint::get_tasks, m)?)?;
//     m.add_function(wrap_pyfunction!(entrypoint::schedule_without_partitions, m)?)?;
//     m.add_function(wrap_pyfunction!(entrypoint::collect, m)?)?;
//     Ok(())
// }
