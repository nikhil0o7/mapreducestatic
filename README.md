# MapReduce Using Cloud Functions

## Introduction
This assignment implements MapReduce using Google Cloud Functions to perform operations on large datasets in a distributed setting, achieving parallelism and concurrency.

## Implementation Overview
- **Master Program**: Coordinates the MapReduce process, including communication between mappers and reducers.
- **Mappers**: Cloud functions creating intermediate files, managed by the master.
- **Reducers**: Cloud functions processing mapper data and updating a Redis database.
- **Scalability & Performance**: Parallel processing and optimized performance through tuned mappers and reducers.
- **Trigger Component**: Initiates the mapper process upon file uploads.

## Components

### Master
- **Function**: Manages file assignments to mappers and initiates reducers.
- **Child Component**: `mapper_inverted`, a cloud trigger for input directory changes.
- **Files**: `master.py`, `master_cloud.py`, `master_inverted.py`, `init_master.py`

### Mapper
- **Function**: Processes words from files, assigns keys to reducers, and manages output format.
- **Files**: `mapper.py`, `mapper_cloud.py`

### Reducer
- **Function**: Combines mapper outputs and updates Redis database.
- **Files**: `reducer.py`, `reducer_cloud.py`

### Distributed Group by
- **Implementation**: Split into two stages - sorting keys (Mapper) and grouping/reducing (Reducer).

### Parallelism and Concurrency
- **Mappers**: Concurrent and parallel operations.
- **Reducers**: Concurrent grouping and reduction using multiprocessing.

## Front End
- **Functionality**: Offers search and status options.
- **URL**: (https://nikhil0o7.github.io/mapreducestatic/)

## Logs
- Essential for troubleshooting and error handling, accessible within cloud functions.

## Setup
- **Dependencies**: Redis, requests, Google Storage.
- **Installation Commands**: 
  ```bash
  chmod +x dependencies.sh
  ./dependencies.sh
