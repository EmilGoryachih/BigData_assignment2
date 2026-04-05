import os
import shutil
import sys

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from text_utils import normalize_whitespace


def sanitize_title(raw_title):
    sanitized = sanitize_filename(str(raw_title)).replace(" ", "_").strip("._")
    return sanitized or "untitled"


def build_filename(doc_id, title):
    return f"{doc_id}_{sanitize_title(title)}.txt"


def main():
    input_parquet = sys.argv[1] if len(sys.argv) > 1 else "/parquet/source.parquet"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "generated_data"
    doc_count = int(sys.argv[3]) if len(sys.argv) > 3 else 1000

    spark = SparkSession.builder.appName("parquet to text corpus").getOrCreate()

    df = (
        spark.read.parquet(input_parquet)
        .select("id", "title", "text")
        .filter(F.col("id").isNotNull())
        .filter(F.col("title").isNotNull())
        .filter(F.col("text").isNotNull())
        .filter(F.length(F.trim(F.col("text"))) > 0)
        .orderBy(F.col("id").asc())
        .limit(doc_count)
    )

    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    written = 0
    for row in df.toLocalIterator():
        doc_id = str(row["id"]).strip()
        title = str(row["title"]).strip()
        text = normalize_whitespace(row["text"])

        if not doc_id or not title or not text:
            continue

        filename = build_filename(doc_id, title)
        path = os.path.join(output_dir, filename)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(text)
        written += 1

    print(f"Written documents: {written}")
    print(f"Local output directory: {output_dir}")
    spark.stop()


if __name__ == "__main__":
    main()
