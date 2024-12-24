# MapReduce Framework

A distributed computing framework implementation based on Google's MapReduce paper, supporting both sequential and distributed execution modes.

## Model Definition

### Map Function
Each Map function processes input data blocks of identical structure and size, generating multiple intermediate data blocks.

### Reduce Function
Reduce nodes receive and consolidate intermediate data, processing it to generate final results.

## Implementation Steps

1. **Data Preparation**
   - Large dataset is divided into equal-sized blocks
   - Job programs are prepared for processing

2. **System Architecture**
   - Master node for job scheduling
   - Worker nodes for Map and Reduce processing

3. **Job Execution Flow**
   - User submits task to master node
   - Master assigns Map tasks to available workers
   - Master assigns Reduce tasks to available workers
   - Map workers execute programs and process data
   - Map workers generate intermediate results
   - Map workers notify master of completion and result locations
   - Master waits for all Map tasks to complete
   - Reduce workers fetch intermediate results
   - Reduce workers consolidate final results

## Execution Modes

1. **Sequential Execution**
   - Single-threaded execution
   - Useful for debugging and testing

2. **Concurrent Execution**
   - Distributed processing
   - Parallel execution of tasks
   - Worker pool management
   - Fault tolerance

## Key Features

- Dynamic worker registration
- Automatic task retry on failures
- RPC-based communication
- Unix domain socket support
- Intermediate result management
- Task scheduling and coordination

## Project Structur
.
├── common.go # Common types and utilities
├── common_map.go # Map phase implementation
├── common_reduce.go # Reduce phase implementation
├── common_rpc.go # RPC communication
├── master.go # Master node implementation
├── master_rpc.go # Master RPC server
├── master_splitmerge.go# File splitting and merging
├── schedule.go # Task scheduling
└── worker.go # Worker node implementation

## Implementation Details

The framework follows the MapReduce paper's design principles:
1. Master-Worker architecture
2. Fault-tolerant task execution
3. Intermediate result handling
4. Automatic task redistribution
5. Worker pool management