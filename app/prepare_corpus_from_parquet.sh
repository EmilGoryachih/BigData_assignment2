#!/bin/bash
set -euo pipefail

export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONUTF8=1

source .venv/bin/activate

LOCAL_PARQUET_PATH="${1:-}"
LOCAL_OUTPUT_DIR="${2:-generated_data}"
DOC_COUNT="${3:-1000}"
HDFS_PARQUET_PATH="${4:-/parquet/source.parquet}"
HDFS_RAW_DIR="${5:-/data}"
HDFS_PREPARED_DIR="${6:-/input/data}"

if [ -z "$LOCAL_PARQUET_PATH" ]; then
  echo "Usage: bash prepare_corpus_from_parquet.sh <local_parquet_path> [local_output_dir] [doc_count] [hdfs_parquet_path] [hdfs_raw_dir] [hdfs_prepared_dir]" >&2
  exit 1
fi

if [ ! -f "$LOCAL_PARQUET_PATH" ]; then
  echo "Local parquet file does not exist: $LOCAL_PARQUET_PATH" >&2
  exit 1
fi

echo "Preparing corpus from parquet"
echo "  Local parquet path: $LOCAL_PARQUET_PATH"
echo "  Local output dir: $LOCAL_OUTPUT_DIR"
echo "  Document count: $DOC_COUNT"
echo "  HDFS parquet path: $HDFS_PARQUET_PATH"
echo "  HDFS raw dir: $HDFS_RAW_DIR"
echo "  HDFS prepared dir: $HDFS_PREPARED_DIR"

hdfs dfs -mkdir -p "$(dirname "$HDFS_PARQUET_PATH")"
hdfs dfs -rm -f "$HDFS_PARQUET_PATH" >/dev/null 2>&1 || true
hdfs dfs -put -f "$LOCAL_PARQUET_PATH" "$HDFS_PARQUET_PATH"

spark-submit --master local[*] prepare_corpus_from_parquet.py "$HDFS_PARQUET_PATH" "$LOCAL_OUTPUT_DIR" "$DOC_COUNT"

bash prepare_data.sh "$LOCAL_OUTPUT_DIR" "$HDFS_RAW_DIR" "$HDFS_PREPARED_DIR"

echo "Parquet-based corpus preparation complete."
