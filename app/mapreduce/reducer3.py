#!/usr/bin/env python3
import sys


current_term = None
current_df = 0


def flush():
    if current_term is not None:
        print(f"{current_term}\t{current_df}")


for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue

    term, value = parts
    if term != current_term:
        flush()
        current_term = term
        current_df = 0

    current_df += int(value)

flush()
