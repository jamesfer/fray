# Fray

This is a proof of concept streaming engine inspired by DataFusion ray and Flink.

## The concept

Take a distributed DataFusion engine like DataFusion ray, and add Flink’s state checkpointing 
approach to build support for saving the progress of a query. Creating a single engine that can run 
both low latency interactive queries and long-running streaming pipelines. In addition, this project
has a few ideas for improving Flink's model to support faster autoscaling and recovery from failed
tasks. It's still theoretical for now, but the enhancements should allow a running pipeline to
scale up or down, or restart operators with zero downtime.

## Current state

This is still very much a proof of concept, and there are many parts of the project that only work
or are tested in theory.

Next goals
- Create a thin compatibility layer with DataFusion to allow the engine to run SQL queries and
  general datafusion plans.
- Expand support for restarting tasks from existing checkpoints and resolve various bugs in the
  algorithm.
- Create a basic coordinator with explicit commands for scaling up or restarting a node to
  prove that the theory can work in practice.

## A few implementation details

### Operator model

DataFusion’s operators form a tree structure. When execute is called on the root plan, it calls the
same method on its children which, in turn, starts all the operators in the query. The execute 
method returns a stream which exists for the entire lifetime of the query; once it finishes, a
consumer knows that the operator has completed.

There are a few changes to this model in this library. Common streaming pipelines need to support
DAGs and even circular pipelines, so the tree structure wasn't possible to maintain. Instead, a 
higher level function starts all the operators in a plan and passes each of them their own input 
streams as an argument to the main method; each operator doesn't need to call anything on its
children.

Operators that consume multiple input streams are supported. Each input is identified by an ordinal
number, so joins and other types of operators can identify which side is the build side. Operators
can also return multiple output streams that can be consumed in parallel.

Operators also have a load method to move to a particular offset in the stream. When called, operators
should reload their state from that checkpoint.

### State checkpointing

Flink's algorithm works by inserting checkpoint markers into the data streams. When an operator
receives a checkpoint marker, it saves its current in memory state to a state backend before
continuing with stream processing. If a pipeline ever needs to be restarted from a previous point in
the stream, all operators load their in memory state from the backend before continuing to process the stream.

For inputs, their current offset is stored in their state, so they can restart from the same point
in the data set.

### Partitioning

Just like DataFusion ray, the full input plan is broken up into chunks that can be executed on a 
single machine, and different fragments communicate with Arrow Flight. Incoming data can also be
partitioned between multiple copies of the same fragment. When reading data from another fragment,
an exchange operator can ask for just a certain partition of the data, much like in fan-out plans,
and a single exchange operator can read data from multiple plans, such as in fan-in plans.

Each operator's state can also be partitioned.

## Getting Started

After cloning, install all of the submodules:
```shell
git submodule init
```

Then you can try running some of the tests
```shell
cargo test --package datafusion_ray --lib streaming::worker_process::tests
```

To run the project on ray, you will need to use the python entrypoint (requires 
[poetry](https://python-poetry.org/docs/) for python deps and 
[maturin](https://www.maturin.rs/installation.html) for rust).

```shell
# Create a venv and install the dependencies
python -m venv .venv
source .venv/bin/activate

# Install dependencies from pyproject.toml
poetry install

# Build the rust project into a python extension
maturin dev

# Start the project on ray
python py/entrypoint.py
```

The Python entrypoint calls the rust file `src/python/py_entrypoint.rs` to get the list of tasks
that need to be started; the query pipeline itself is defined in rust.
