#!/usr/bin/env python3
import sys


current_doc_id = None

for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue

    doc_id, rest = parts
    if doc_id == current_doc_id:
        continue

    current_doc_id = doc_id
    print(f"{doc_id}\t{rest}")
