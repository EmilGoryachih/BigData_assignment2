# big-data-assignment2

## Current repository state

This repository now includes a working end-to-end baseline solution for the assignment:

1. Hadoop and Cassandra services are started with Docker Compose.
2. A local text corpus is uploaded to HDFS, or it can be reproduced from a parquet file with PySpark before upload.
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

The repository ships with a ready plain-text corpus in `app/data` and `app/data_100` so the grader can run the project immediately. In addition, the repository now includes an optional parquet-to-text reproduction step. If you place a parquet file inside `app/`, for example `app/a.parquet`, the same environment can recreate the local text corpus with PySpark before continuing with the normal HDFS preparation and indexing flow.

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

## Optional parquet reproduction flow

If you want to reproduce the corpus from parquet before indexing, place the parquet file inside `app/` so it is visible inside the master container as `/app/<name>.parquet`.

Example for the full run:

```bash
PARQUET_INPUT_PATH=/app/a.parquet docker compose up
```

Example for the debug run on 100 documents:

```bash
DATASET_MODE=debug PARQUET_INPUT_PATH=/app/a.parquet docker compose up
```

PowerShell equivalents:

```powershell
$env:PARQUET_INPUT_PATH='/app/a.parquet'
docker compose up
```

```powershell
$env:DATASET_MODE='debug'
$env:PARQUET_INPUT_PATH='/app/a.parquet'
docker compose up
```

When `PARQUET_INPUT_PATH` is set, `app.sh` calls `prepare_corpus_from_parquet.sh`, which:

1. Uploads the parquet file into HDFS.
2. Reads it with PySpark.
3. Extracts `id`, `title`, and `text`.
4. Creates UTF-8 files named `<doc_id>_<doc_title>.txt`.
5. Uploads those documents into `/data` or `/data_100`.
6. Builds the prepared one-partition input in `/input/data` or `/input/data_100`.

## What to do next inside the master container

For the debug dataset:

1. Re-run the full index flow with `bash index.sh /input/data_100`.
2. Run searches with `bash search.sh "your query"`.

For the full dataset:

1. Start the stack with `docker compose up`.
2. Use `bash index.sh /input/data` and `bash search.sh "your query"` if you want to re-run manually.
