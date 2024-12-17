# MapReduce Project

## Overview

This project implements a distributed system for running jobs in parallel using the MapReduce programming model. This model is the foundation for large-scale data processing systems like Hadoop and Amazon EMR.

## How It Works

### Workflow

1. **Worker Initialization**: A worker process starts and requests tasks from the coordinator.
2. **Task Assignment**: The coordinator maintains a list of pending tasks and assigns either a map task or a reduce task to the worker.
3. **Task Execution**: The worker executes the assigned task:
   - **Map Task**: Processes input data and produces intermediate key-value pairs.
   - **Reduce Task**: Aggregates intermediate data to produce the final output.
4. **Intermediate Storage**: The worker stores the intermediate results in a designated location.
5. **Task Completion**: The worker notifies the coordinator of task completion and requests the next task.
6. **Reduce Phase**: The reduce phase begins only after all map tasks are completed.

## Benefits

- **Parallel Processing**: Efficiently runs jobs in parallel, reducing overall processing time.
- **Scalability**: Can handle large datasets by distributing tasks across multiple workers.
- **Fault Tolerance**: Automatically reassigns tasks if a worker fails.

## Getting Started

### Prerequisites

- Go programming language (version 1.15 or higher)

### Running the Project

1. **Build the Coordinator and Worker**:
    ```sh
    go build -o mrcoordinator main/mrcoordinator.go
    go build -o mrworker main/mrworker.go
    go build -buildmode=plugin -o wc.so mrapps/wc.go
    ```

2. **Run the Coordinator**:
    ```sh
    ./mrcoordinator pg-*.txt &
    ```

3. **Run the Workers**:
    ```sh
    ./mrworker wc.so
    ```

4. **Check the Output**:
    The output files will be generated in the current directory.


## Cleanup

To clean up the generated files, run:
```sh
rm -f mrcoordinator mrworker wc.so mr-out-*
```

## Testing

For testing, I set up a `mr-test.sh` file to run the jobs. I also added delays to the worker processes, ran a bunch of them manually, and killed the processes to see if failovers and task assignment for unfinished work can be assigned effectively.

## Future Developments
1. **Holistic Tests**: Add more comprehensive tests to check for race conditions and handle edge cases more effectively.
2. **Efficiency Improvements**: Optimize the system to prioritize new jobs over timeouts, as timeouts might resolve themselves.
3. **Scalability**: Address potential bottlenecks caused by mutex blocking inside the coordinator to improve performance at scale.
4. **Streaming Data**: Enhance the system to handle large-scale streaming data, allowing map and reduce operations to run in parallel while tracking the end of the file.



