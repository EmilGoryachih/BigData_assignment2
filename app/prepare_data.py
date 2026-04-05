import os
import sys
from pyspark.sql import SparkSession

from text_utils import normalize_whitespace


def build_prepared_record(item):
    path, text = item
    name = os.path.basename(path)

    if not name.endswith(".txt"):
        return None

    stem = name[:-4]
    if "_" not in stem:
        return None

    doc_id, doc_title = stem.split("_", 1)
    clean_text = normalize_whitespace(text)

    if not doc_id or not doc_title or not clean_text:
        return None

    return f"{doc_id}\t{doc_title}\t{clean_text}"


def main():
    input_dir = sys.argv[1] if len(sys.argv) > 1 else "/data_100"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "/input/data_100"

    spark = SparkSession.builder.appName("data preparation").getOrCreate()
    sc = spark.sparkContext

    prepared = (
        sc.wholeTextFiles(input_dir)
        .map(build_prepared_record)
        .filter(lambda record: record is not None)
        .coalesce(1)
        .cache()
    )

    doc_count = prepared.count()
    print(f"Prepared documents: {doc_count}")
    prepared.saveAsTextFile(output_dir)
    spark.stop()


if __name__ == "__main__":
    main()
