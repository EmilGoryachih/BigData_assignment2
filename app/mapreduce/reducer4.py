#!/usr/bin/env python3
import sys


totals = {}


for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue

    name, value = parts
    totals[name] = totals.get(name, 0) + int(value)

doc_count = totals.get("doc_count", 0)
total_doc_length = totals.get("total_doc_length", 0)
avg_doc_length = (total_doc_length / doc_count) if doc_count else 0.0

print(f"doc_count\t{doc_count}")
print(f"total_doc_length\t{total_doc_length}")
print(f"avg_doc_length\t{avg_doc_length}")
