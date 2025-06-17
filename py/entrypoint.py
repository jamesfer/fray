import ray

from datafusion_ray._datafusion_ray_internal import get_tasks, entrypoint, schedule_without_partitions, collect
from datafusion_ray.core import df_ray_runtime_env
from datafusion_ray.friendly import new_friendly_name

ray.init(runtime_env=df_ray_runtime_env)

@ray.remote
class Processor:
    def __init__(self):
        # Import the rust extension module
        from datafusion_ray._datafusion_ray_internal import (
            DFRayStreamingProcessorService,
        )

        name = new_friendly_name()
        self.processor_service = DFRayStreamingProcessorService(name)

    async def start_up(self):
        # Waits for the worker to be ready
        self.processor_service.start_up()
        return self.processor_service.addr()

    async def all_done(self):
        await self.processor_service.all_done()

    async def update_plan(
        self,
        plan_bytes: bytes,
        initial_schedule_details_bytes: bytes,
    ):
        self.processor_service.start_task(
            plan_bytes,
            initial_schedule_details_bytes,
        )



def main():
    entrypoint(Processor, ray.get)
    # tasks = get_tasks()
    # print(f"Found {len(tasks)} tasks")
    #
    # record_batches = run_tasks(tasks)
    #
    # print([record_batch.to_pyarrow() for record_batch in record_batches])


def run_tasks(tasks):
    # Start up all processors
    processors = [Processor.remote() for _ in range(len(tasks))]
    addrs = ray.get([processor.start_up.remote() for processor in processors])
    print(f"Started workers {addrs}")

    # Assign tasks to processors by zipping tasks, processors and addrs together
    assigned_tasks = list(zip(tasks, addrs))
    scheduling_details = schedule_without_partitions(assigned_tasks)

    # Start the tasks in the background
    for (task, addr), processor in zip(assigned_tasks, processors):
        print(f"Assigning task to processor {addr}")
        processor.update_plan.remote(task.task_bytes(), scheduling_details.bytes())

    # Collect results from the last task
    output_task, output_addr = assigned_tasks[-1]
    record_batches = collect(output_addr, output_task.output_streams()[0])
    return record_batches


try:
    main()
except Exception as e:
    print(e)
