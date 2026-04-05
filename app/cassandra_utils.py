import os
import time

from cassandra.cluster import Cluster


DEFAULT_HOSTS = [
    host.strip()
    for host in os.getenv("CASSANDRA_HOSTS", "cassandra-server").split(",")
    if host.strip()
]
DEFAULT_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
DEFAULT_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "simple_search_engine")


def connect_session(wait_for_ready=True, retries=30, delay_seconds=5):
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            cluster = Cluster(DEFAULT_HOSTS, port=DEFAULT_PORT)
            session = cluster.connect()
            return cluster, session
        except Exception as exc:  # pragma: no cover - integration path
            last_error = exc
            if not wait_for_ready or attempt == retries:
                raise
            time.sleep(delay_seconds)

    raise RuntimeError(f"Could not connect to Cassandra: {last_error}")


def ensure_schema(session, keyspace=DEFAULT_KEYSPACE):
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(keyspace)

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id bigint PRIMARY KEY,
            title text,
            display_title text,
            doc_len int
        )
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            df int
        )
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS postings (
            term text,
            doc_id bigint,
            tf int,
            doc_len int,
            PRIMARY KEY (term, doc_id)
        )
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS corpus_stats (
            name text PRIMARY KEY,
            value double
        )
        """
    )


def truncate_tables(session):
    for table_name in ("postings", "vocabulary", "documents", "corpus_stats"):
        session.execute(f"TRUNCATE {table_name}")
