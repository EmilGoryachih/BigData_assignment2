#!/usr/bin/env python3
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
for import_path in (SCRIPT_DIR, PARENT_DIR):
    if import_path and import_path not in sys.path:
        sys.path.insert(0, import_path)

from text_utils import analyze_text, parse_prepared_line


for raw_line in sys.stdin:
    parsed = parse_prepared_line(raw_line)
    if parsed is None:
        continue

    doc_id, title, text = parsed
    _, doc_len = analyze_text(text)
    if doc_len == 0:
        continue

    print(f"{doc_id}\t{title}\t{doc_len}")
