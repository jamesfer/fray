import ray
import signal
import sys

from datafusion_ray._datafusion_ray_internal import get_tasks, entrypoint, schedule_without_partitions, collect
from datafusion_ray.core import df_ray_runtime_env
from datafusion_ray.friendly import new_friendly_name

# Initialize Ray with shutdown handler
ray.init(runtime_env=df_ray_runtime_env)

def signal_handler(signum, frame):
    print("\nReceived interrupt signal, shutting down Ray cluster...")
    try:
        ray.shutdown()
    except Exception as e:
        print(f"Error during Ray shutdown: {e}")
    sys.exit(0)

# Register signal handlers for graceful shutdown
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

@ray.remote
class Processor:
    def __init__(self, remote_storage_dir=None):
        # Import the rust extension module
        from datafusion_ray._datafusion_ray_internal import (
            DFRayStreamingProcessorService,
        )

        name = new_friendly_name()
        self.processor_service = DFRayStreamingProcessorService(name, remote_storage_dir)

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

    async def update_plan_with_checkpoint(
        self,
        plan_bytes: bytes,
        initial_schedule_details_bytes: bytes,
        starting_checkpoint
    ):
        self.processor_service.start_task_from(
            plan_bytes,
            initial_schedule_details_bytes,
            starting_checkpoint,
        )



def main():
    entrypoint(Processor, ray.get)
    print("Python main finished")


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
finally:
    print("Shutting down Ray cluster...")
    ray.shutdown()
