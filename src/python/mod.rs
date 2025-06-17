pub mod streaming_processor_service;
mod py_task_definition;
mod py_scheduling_details;
pub mod py_utils;
mod py_entrypoint;
pub mod remote_processor;

use std::env;
use pyo3::{pymodule, wrap_pyfunction, Bound, PyResult};
use pyo3::types::{PyModule, PyModuleMethods};
use crate::{context, dataframe, processor_service, util};


#[pymodule]
fn _datafusion_ray_internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    setup_logging();
    m.add_class::<context::DFRayContext>()?;
    m.add_class::<dataframe::DFRayDataFrame>()?;
    // m.add_class::<dataframe::PyDFRayStage>()?;
    m.add_class::<processor_service::DFRayProcessorService>()?;
    m.add_class::<streaming_processor_service::DFRayStreamingProcessorService>()?;
    m.add_class::<util::LocalValidator>()?;
    m.add_function(wrap_pyfunction!(util::prettify, m)?)?;
    m.add_function(wrap_pyfunction!(py_entrypoint::get_tasks, m)?)?;
    m.add_function(wrap_pyfunction!(py_utils::schedule_without_partitions, m)?)?;
    m.add_function(wrap_pyfunction!(py_utils::collect, m)?)?;
    m.add_function(wrap_pyfunction!(py_entrypoint::entrypoint, m)?)?;
    Ok(())
}

fn setup_logging() {
    // ensure this python logger will route messages back to rust
    pyo3_pylogger::register("datafusion_ray");

    let dfr_env = env::var("DATAFUSION_RAY_LOG_LEVEL").unwrap_or("WARN".to_string());
    let rust_log_env = env::var("RUST_LOG").unwrap_or("WARN".to_string());

    let combined_env = format!("{rust_log_env},datafusion_ray={dfr_env}");

    env_logger::Builder::new()
        .parse_filters(&combined_env)
        .init();
}
