#!/bin/bash
set -euo pipefail

export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONUTF8=1

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)

# Executors are not used in this local preparation step.
unset PYSPARK_PYTHON

LOCAL_INPUT_DIR="${1:-data_100}"
HDFS_RAW_DIR="${2:-/data_100}"
HDFS_PREPARED_DIR="${3:-/input/data_100}"

if [ ! -d "$LOCAL_INPUT_DIR" ]; then
  echo "Local input directory does not exist: $LOCAL_INPUT_DIR" >&2
  exit 1
fi

echo "Preparing data"
echo "  Local input dir: $LOCAL_INPUT_DIR"
echo "  HDFS raw dir: $HDFS_RAW_DIR"
echo "  HDFS prepared dir: $HDFS_PREPARED_DIR"

hdfs dfs -rm -r -f "$HDFS_RAW_DIR" >/dev/null 2>&1 || true
hdfs dfs -rm -r -f "$HDFS_PREPARED_DIR" >/dev/null 2>&1 || true

hdfs dfs -mkdir -p "$HDFS_RAW_DIR"

shopt -s nullglob
files=("$LOCAL_INPUT_DIR"/*.txt)
shopt -u nullglob

copied_count="${#files[@]}"
if [ "$copied_count" -eq 0 ]; then
  echo "No .txt files found in local input directory: $LOCAL_INPUT_DIR" >&2
  exit 1
fi

batch_size=200
for ((offset = 0; offset < copied_count; offset += batch_size)); do
  batch=("${files[@]:offset:batch_size}")
  hdfs dfs -put -f "${batch[@]}" "$HDFS_RAW_DIR"/
done

echo "Uploaded local documents to HDFS: $copied_count"

spark-submit prepare_data.py "$HDFS_RAW_DIR" "$HDFS_PREPARED_DIR"

echo "HDFS raw input:"
hdfs dfs -count "$HDFS_RAW_DIR"

echo "HDFS prepared input:"
hdfs dfs -ls "$HDFS_PREPARED_DIR"

echo "Prepared sample:"
hdfs dfs -cat "$HDFS_PREPARED_DIR"/part-* | head -n 3 || true

echo "Data preparation complete."
