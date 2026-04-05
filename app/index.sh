#!/bin/bash
set -euo pipefail

INPUT_PATH="${1:-/input/data}"
INDEX_BASE="${2:-/indexer}"
TMP_BASE="${3:-/tmp/indexer}"

echo "Index pipeline entrypoint"
echo "  Input path: $INPUT_PATH"
echo "  Index base: $INDEX_BASE"
echo "  Temp base: $TMP_BASE"

bash create_index.sh "$INPUT_PATH" "$INDEX_BASE" "$TMP_BASE"
bash store_index.sh "$INDEX_BASE"
