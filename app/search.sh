#!/bin/bash
set -euo pipefail

QUERY_TEXT="$*"

if [ -z "$QUERY_TEXT" ]; then
  echo "Usage: bash search.sh \"your query\"" >&2
  exit 1
fi

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)

# Python of the executor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
  --master yarn \
  --archives /app/.venv.tar.gz#.venv \
  --py-files /app/cassandra_utils.py,/app/text_utils.py \
  --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
  query.py "$QUERY_TEXT"
