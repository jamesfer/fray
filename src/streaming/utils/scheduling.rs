use std::collections::HashMap;
use crate::streaming::task_definition::TaskDefinition;

pub fn assign_tasks_to_workers(worker_ids: &Vec<String>, tasks: Vec<TaskDefinition>) -> (HashMap<String, String>, Vec<(String, TaskDefinition)>) {
    // Assign each task to a worker
    let mut assigned_tasks: Vec<(String, TaskDefinition)> = tasks.into_iter()
        .zip(worker_ids.iter().cycle())
        .map(|(task, worker_id)| (worker_id.clone(), task))
        .collect();

    // Create lookup map for each output stream
    let mut output_stream_to_worker: HashMap<String, String> = assigned_tasks.iter()
        .map(|(worker_id, task)| {
            (task.output_stream_id.clone(), worker_id.clone())
        })
        .collect();

    // Update each task definition, replacing the placeholder address with a real address
    for &mut (_, ref mut task) in assigned_tasks.iter_mut() {
        for phase in task.inputs.phases.iter_mut() {
            for generation in phase.generations.iter_mut() {
                for stream in generation.streams.iter_mut() {
                    for address in stream.addresses.iter_mut() {
                        if address.address == "__PLACEHOLDER__" {
                            let worker_id = output_stream_to_worker.get(&address.stream_id)
                                .expect(&format!("Could not find worker for stream {}", address.stream_id));
                            address.address = worker_id.clone();
                        }
                    }
                }
            }
        }
    }

    (output_stream_to_worker, assigned_tasks)
}
