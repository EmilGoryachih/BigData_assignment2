#!/bin/bash
set -euo pipefail

export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONIOENCODING=UTF-8

INDEX_BASE="${1:-/indexer}"

echo "Store the index and related data in Cassandra/ScyllaDB tables"
echo "  Index base: $INDEX_BASE"

for required_path in \
  "${INDEX_BASE}/documents" \
  "${INDEX_BASE}/postings" \
  "${INDEX_BASE}/vocabulary" \
  "${INDEX_BASE}/corpus_stats"
do
  if ! hdfs dfs -test -e "$required_path"; then
    echo "Required HDFS index path is missing: $required_path" >&2
    exit 1
  fi
done

source .venv/bin/activate
python app.py load "$INDEX_BASE"
