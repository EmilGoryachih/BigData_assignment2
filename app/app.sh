#!/bin/bash
set -euo pipefail

export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONUTF8=1

# Start ssh server
service ssh restart

# Starting the services
bash start-services.sh

if [ -d ".venv" ] && [ -f ".venv.tar.gz" ]; then
  echo "Reusing existing Python environment artifacts."
  source .venv/bin/activate
else
  echo "Creating Python environment artifacts."
  rm -rf .venv .venv.tar.gz

  python3 -m venv .venv
  source .venv/bin/activate

  # Make sure wheel-based installs work when packages provide wheels.
  python -m pip install --upgrade pip setuptools wheel

  # Install project packages
  pip install -r requirements.txt

  # Package the virtual env for spark-submit --archives.
  venv-pack -o .venv.tar.gz
fi

DATASET_MODE="${DATASET_MODE:-full}"
RUN_INDEX="${RUN_INDEX:-1}"
RUN_SAMPLE_SEARCHES="${RUN_SAMPLE_SEARCHES:-1}"

if [ "$DATASET_MODE" = "full" ]; then
  LOCAL_INPUT_DIR="data"
  HDFS_RAW_DIR="/data"
  HDFS_PREPARED_DIR="/input/data"
else
  LOCAL_INPUT_DIR="data_100"
  HDFS_RAW_DIR="/data_100"
  HDFS_PREPARED_DIR="/input/data_100"
fi

echo "Bootstrap dataset mode: $DATASET_MODE"
echo "Local input dir: $LOCAL_INPUT_DIR"
echo "HDFS raw dir: $HDFS_RAW_DIR"
echo "HDFS prepared dir: $HDFS_PREPARED_DIR"

# Prepare the corpus and optionally run indexing and sample searches.
bash prepare_data.sh "$LOCAL_INPUT_DIR" "$HDFS_RAW_DIR" "$HDFS_PREPARED_DIR"

if [ "$RUN_INDEX" = "1" ]; then
  echo
  echo "Running index pipeline"
  bash index.sh "$HDFS_PREPARED_DIR"
fi

if [ "$RUN_SAMPLE_SEARCHES" = "1" ] && [ "$RUN_INDEX" = "1" ]; then
  echo
  echo "Running sample queries"
  for query_text in "history" "music" "death"; do
    echo
    echo "Search query: $query_text"
    bash search.sh "$query_text" || true
  done
fi

echo
echo "Bootstrap complete."
echo "Prepared input is ready in HDFS: $HDFS_PREPARED_DIR"
echo "Index base is ready in Cassandra keyspace: ${CASSANDRA_KEYSPACE:-simple_search_engine}"

# Keep the master container alive for follow-up manual work.
tail -f /dev/null
