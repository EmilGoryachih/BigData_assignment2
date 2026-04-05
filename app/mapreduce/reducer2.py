#!/usr/bin/env python3
import sys


for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue
    print(line)
