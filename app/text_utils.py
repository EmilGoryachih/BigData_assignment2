import re
from collections import Counter


TOKEN_PATTERN = re.compile(r"[^\W_]+(?:'[^\W_]+)*", re.UNICODE)


def normalize_whitespace(text):
    return re.sub(r"\s+", " ", text).strip()


def parse_prepared_line(line):
    parts = line.rstrip("\n").split("\t", 2)
    if len(parts) != 3:
        return None

    doc_id, title, text = (part.strip() for part in parts)
    if not doc_id or not title or not text:
        return None

    return doc_id, title, text


def tokenize(text):
    normalized = normalize_whitespace(text).lower()
    return TOKEN_PATTERN.findall(normalized)


def analyze_text(text):
    tokens = tokenize(text)
    return Counter(tokens), len(tokens)


def title_to_display(title):
    return title.replace("_", " ")
