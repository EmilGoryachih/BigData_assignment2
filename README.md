# big-data-assignment2

## Current repository state

This repository now includes a working end-to-end baseline solution for the assignment:

1. Hadoop and Cassandra services are started with Docker Compose.
2. A local text corpus is uploaded to HDFS.
3. PySpark prepares `/input/data` or `/input/data_100` as one partition.
4. Hadoop Streaming MapReduce jobs build:
   - document metadata
   - postings
   - vocabulary with document frequencies
   - corpus statistics for BM25
5. The index is loaded into Cassandra tables.
6. `search.sh` runs a PySpark BM25 ranker on YARN and prints top results.

By default, `docker compose up` will:

1. Start the Hadoop master, Hadoop worker, and Cassandra containers.
2. Create or reuse the Python virtual environment inside the master container.
3. Upload the full corpus from `app/data` to HDFS as `/data`.
4. Build the prepared input dataset in HDFS as `/input/data`.
5. Build the index in HDFS and store it in Cassandra.
6. Run a few sample search queries.

The master container stays alive after bootstrap so you can continue working inside it.

## Prerequisites

1. Docker
2. Docker Compose

## How to start

Run:

```bash
docker compose up
```

Optional environment switches:

```bash
DATASET_MODE=debug docker compose up
```

`DATASET_MODE=full` uses `app/data` and is the default final-run mode.
`DATASET_MODE=debug` uses `app/data_100`.

## What to do next inside the master container

For the debug dataset:

1. Re-run the full index flow with `bash index.sh /input/data_100`.
2. Run searches with `bash search.sh "your query"`.

For the full dataset:

1. Start the stack with `docker compose up`.
2. Use `bash index.sh /input/data` and `bash search.sh "your query"` if you want to re-run manually.
