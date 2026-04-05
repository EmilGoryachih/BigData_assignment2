import math
import os
import sys
from collections import Counter

from pyspark.sql import SparkSession

from cassandra_utils import DEFAULT_KEYSPACE, connect_session, ensure_schema
from text_utils import title_to_display, tokenize


DEFAULT_K1 = float(os.getenv("BM25_K1", "1.5"))
DEFAULT_B = float(os.getenv("BM25_B", "0.75"))


def read_query_text():
    cli_query = " ".join(sys.argv[1:]).strip()
    if cli_query:
        return cli_query
    return sys.stdin.read().strip()


def fetch_corpus_stats(session):
    rows = session.execute("SELECT name, value FROM corpus_stats")
    stats = {row.name: float(row.value) for row in rows}
    return {
        "doc_count": int(stats.get("doc_count", 0.0)),
        "avg_doc_length": float(stats.get("avg_doc_length", 0.0)),
    }


def fetch_df_map(session, terms):
    select_df = session.prepare("SELECT df FROM vocabulary WHERE term = ?")
    result = {}
    for term in terms:
        row = session.execute(select_df, (term,)).one()
        if row is not None:
            result[term] = int(row.df)
    return result


def fetch_postings(session, query_terms):
    select_postings = session.prepare(
        "SELECT doc_id, tf, doc_len FROM postings WHERE term = ?"
    )

    posting_rows = []
    for term, query_tf in query_terms.items():
        rows = session.execute(select_postings, (term,))
        for row in rows:
            posting_rows.append((term, int(row.doc_id), int(row.tf), int(row.doc_len), int(query_tf)))

    return posting_rows


def fetch_documents(session, doc_ids):
    select_document = session.prepare(
        "SELECT title, display_title, doc_len FROM documents WHERE doc_id = ?"
    )

    documents = {}
    for doc_id in doc_ids:
        row = session.execute(select_document, (int(doc_id),)).one()
        if row is None:
            continue

        documents[int(doc_id)] = {
            "title": row.title,
            "display_title": row.display_title or title_to_display(row.title),
            "doc_len": int(row.doc_len),
        }

    return documents


def bm25_contribution(item, df_map, doc_count, avg_doc_length, k1, b):
    term, doc_id, tf, doc_len, query_tf = item
    df = df_map.get(term)
    if not df or not doc_count or not avg_doc_length or tf <= 0 or doc_len <= 0:
        return doc_id, 0.0

    idf = math.log(doc_count / df)
    normalization = k1 * ((1.0 - b) + b * (doc_len / avg_doc_length))
    score = idf * (((k1 + 1.0) * tf) / (normalization + tf))
    return doc_id, score * query_tf


def main():
    query_text = read_query_text()
    if not query_text:
        raise SystemExit("No query provided")

    query_terms = Counter(tokenize(query_text))
    if not query_terms:
        raise SystemExit("Query does not contain searchable terms")

    cluster, session = connect_session(wait_for_ready=True)
    try:
        ensure_schema(session, DEFAULT_KEYSPACE)
        session.set_keyspace(DEFAULT_KEYSPACE)

        stats = fetch_corpus_stats(session)
        if stats["doc_count"] == 0 or stats["avg_doc_length"] == 0.0:
            raise SystemExit("Index statistics are missing. Run index.sh first.")

        df_map = fetch_df_map(session, query_terms)
        if not df_map:
            print("No documents matched the query.")
            return

        postings = fetch_postings(session, query_terms)
        if not postings:
            print("No documents matched the query.")
            return

        spark = SparkSession.builder.appName("search query").getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel("WARN")

        df_map_broadcast = sc.broadcast(df_map)
        doc_count = stats["doc_count"]
        avg_doc_length = stats["avg_doc_length"]
        k1 = DEFAULT_K1
        b = DEFAULT_B

        try:
            scores_rdd = (
                sc.parallelize(postings, numSlices=max(1, min(8, len(postings))))
                .map(
                    lambda item: bm25_contribution(
                        item,
                        df_map_broadcast.value,
                        doc_count,
                        avg_doc_length,
                        k1,
                        b,
                    )
                )
                .filter(lambda item: item[1] > 0.0)
                .reduceByKey(lambda left, right: left + right)
            )

            top_results = scores_rdd.takeOrdered(10, key=lambda item: (-item[1], item[0]))
        finally:
            spark.stop()

        if not top_results:
            print("No documents matched the query.")
            return

        documents = fetch_documents(session, [doc_id for doc_id, _ in top_results])

        print(f"Top 10 results for query: {query_text}")
        print("rank\tdoc_id\ttitle\tscore")
        for rank, (doc_id, score) in enumerate(top_results, start=1):
            document = documents.get(doc_id, {})
            title = document.get("display_title") or document.get("title") or str(doc_id)
            print(f"{rank}\t{doc_id}\t{title}\t{score:.6f}")
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
