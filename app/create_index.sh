#!/bin/bash
set -euo pipefail

export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONIOENCODING=UTF-8

INPUT_PATH="${1:-/input/data}"
INDEX_BASE="${2:-/indexer}"
TMP_BASE="${3:-/tmp/indexer}"
STREAMING_JAR="${HADOOP_STREAMING_JAR:-$(find "$HADOOP_HOME/share/hadoop/tools/lib" -name 'hadoop-streaming*.jar' | head -n 1)}"

echo "Create index using MapReduce pipelines"
echo "  Input path: $INPUT_PATH"
echo "  Index base: $INDEX_BASE"
echo "  Temp base: $TMP_BASE"

if [ -z "$STREAMING_JAR" ]; then
  echo "Could not locate hadoop-streaming jar." >&2
  exit 1
fi

if ! hdfs dfs -test -e "$INPUT_PATH"; then
  echo "Input path does not exist in HDFS: $INPUT_PATH" >&2
  exit 1
fi

COMMON_FILES="/app/text_utils.py#text_utils.py"
LINE_MAX_LENGTH=10000000

run_streaming_job() {
  local job_name="$1"
  local mapper_name="$2"
  local reducer_name="$3"
  local output_path="$4"

  echo
  echo "Running job: $job_name"
  echo "  Output path: $output_path"

  hdfs dfs -rm -r -f "$output_path" >/dev/null 2>&1 || true

  hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="search-engine-${job_name}" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.input.linerecordreader.line.maxlength="$LINE_MAX_LENGTH" \
    -files "/app/mapreduce/${mapper_name},/app/mapreduce/${reducer_name},${COMMON_FILES}" \
    -input "$INPUT_PATH" \
    -output "$output_path" \
    -mapper "python3 ${mapper_name}" \
    -reducer "python3 ${reducer_name}"
}

hdfs dfs -mkdir -p "$INDEX_BASE"
hdfs dfs -mkdir -p "$TMP_BASE"

run_streaming_job "documents" "mapper1.py" "reducer1.py" "${INDEX_BASE}/documents"
run_streaming_job "postings" "mapper2.py" "reducer2.py" "${INDEX_BASE}/postings"
run_streaming_job "vocabulary" "mapper3.py" "reducer3.py" "${INDEX_BASE}/vocabulary"
run_streaming_job "corpus-stats" "mapper4.py" "reducer4.py" "${INDEX_BASE}/corpus_stats"

echo
echo "Index output summary"
hdfs dfs -count "${INDEX_BASE}/documents" "${INDEX_BASE}/postings" "${INDEX_BASE}/vocabulary" "${INDEX_BASE}/corpus_stats"
