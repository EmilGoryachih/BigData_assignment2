import argparse
import subprocess
import sys
from itertools import islice

from cassandra.concurrent import execute_concurrent_with_args

from cassandra_utils import DEFAULT_KEYSPACE, connect_session, ensure_schema, truncate_tables
from text_utils import title_to_display


def stream_hdfs_lines(base_path):
    command = ["hdfs", "dfs", "-cat", f"{base_path}/part-*"]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    try:
        for line in process.stdout:
            yield line.rstrip("\n")
    finally:
        if process.stdout is not None:
            process.stdout.close()

    stderr_output = ""
    if process.stderr is not None:
        stderr_output = process.stderr.read().strip()
        process.stderr.close()

    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"hdfs dfs -cat failed for {base_path}: {stderr_output}")


def chunked(iterable, chunk_size):
    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, chunk_size))
        if not chunk:
            break
        yield chunk


def execute_chunked(session, statement, rows, chunk_size=500, concurrency=32):
    total = 0
    for chunk in chunked(rows, chunk_size):
        results = execute_concurrent_with_args(
            session,
            statement,
            chunk,
            concurrency=concurrency,
            raise_on_first_error=False,
        )
        for success, payload in results:
            if not success:
                raise payload
        total += len(chunk)
    return total


def document_rows(index_base):
    for line in stream_hdfs_lines(f"{index_base}/documents"):
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue

        doc_id, title, doc_len = parts
        yield (int(doc_id), title, title_to_display(title), int(doc_len))


def vocabulary_rows(index_base):
    for line in stream_hdfs_lines(f"{index_base}/vocabulary"):
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue

        term, df = parts
        yield (term, int(df))


def posting_rows(index_base):
    for line in stream_hdfs_lines(f"{index_base}/postings"):
        parts = line.split("\t", 3)
        if len(parts) != 4:
            continue

        term, doc_id, tf, doc_len = parts
        yield (term, int(doc_id), int(tf), int(doc_len))


def stat_rows(index_base):
    stats = {}
    for line in stream_hdfs_lines(f"{index_base}/corpus_stats"):
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue

        name, value = parts
        stats[name] = float(value)

    doc_count = stats.get("doc_count", 0.0)
    total_doc_length = stats.get("total_doc_length", 0.0)
    avg_doc_length = total_doc_length / doc_count if doc_count else 0.0

    stats["avg_doc_length"] = avg_doc_length
    return sorted(stats.items())


def load_index(index_base):
    cluster, session = connect_session(wait_for_ready=True)
    try:
        ensure_schema(session, DEFAULT_KEYSPACE)
        truncate_tables(session)

        insert_document = session.prepare(
            """
            INSERT INTO documents (doc_id, title, display_title, doc_len)
            VALUES (?, ?, ?, ?)
            """
        )
        insert_vocabulary = session.prepare(
            "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
        )
        insert_posting = session.prepare(
            "INSERT INTO postings (term, doc_id, tf, doc_len) VALUES (?, ?, ?, ?)"
        )
        insert_stat = session.prepare(
            "INSERT INTO corpus_stats (name, value) VALUES (?, ?)"
        )

        document_count = execute_chunked(session, insert_document, document_rows(index_base))
        vocabulary_count = execute_chunked(session, insert_vocabulary, vocabulary_rows(index_base))
        posting_count = execute_chunked(session, insert_posting, posting_rows(index_base))
        stat_count = execute_chunked(session, insert_stat, stat_rows(index_base), chunk_size=50, concurrency=8)

        print(f"Loaded documents: {document_count}")
        print(f"Loaded vocabulary terms: {vocabulary_count}")
        print(f"Loaded postings: {posting_count}")
        print(f"Loaded stats: {stat_count}")
    finally:
        session.shutdown()
        cluster.shutdown()


def show_keyspaces():
    cluster, session = connect_session(wait_for_ready=True)
    try:
        rows = session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces"
        )
        for row in rows:
            print(row.keyspace_name)
    finally:
        session.shutdown()
        cluster.shutdown()


def build_parser():
    parser = argparse.ArgumentParser(description="Search engine Cassandra utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    load_parser = subparsers.add_parser("load", help="Load HDFS index data into Cassandra")
    load_parser.add_argument("index_base", nargs="?", default="/indexer")

    subparsers.add_parser("show-keyspaces", help="List Cassandra keyspaces")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "load":
        load_index(args.index_base)
        return

    if args.command == "show-keyspaces":
        show_keyspaces()
        return

    parser.print_help()
    raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise
