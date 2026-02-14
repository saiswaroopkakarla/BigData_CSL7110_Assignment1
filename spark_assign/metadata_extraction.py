from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, regexp_extract, first, length, avg

spark = SparkSession.builder.appName("MetadataExtraction").getOrCreate()

# ---- Load book file ----
df = spark.read.text("/home/ssk7/wc_project/200.txt")

books_df = df.select(
    lit("200.txt").alias("file_name"),
    col("value").alias("text")
)

print("\n=== SAMPLE DATA ===")
books_df.show(5, truncate=False)

# ---- Regex metadata extraction ----
meta_df = books_df.select(
    regexp_extract("text", r"Title:\s*(.*)", 1).alias("title"),
    regexp_extract("text", r"Release Date:\s*(.*)", 1).alias("release_date"),
    regexp_extract("text", r"Language:\s*(.*)", 1).alias("language"),
    regexp_extract("text", r"Character set encoding:\s*(.*)", 1).alias("encoding")
)

meta_one = meta_df.groupBy().agg(
    first("title", True).alias("title"),
    first("release_date", True).alias("release_date"),
    first("language", True).alias("language"),
    first("encoding", True).alias("encoding")
)

print("\n=== REGEX METADATA ===")
meta_one.show(truncate=False)

# ---- Fallback title from first line ----
first_line = books_df.limit(1)

title_guess = first_line.select(
    regexp_extract("text", r"eBook of (.*)", 1).alias("title")
)

print("\n=== TITLE FALLBACK ===")
title_guess.show(truncate=False)

# ---- Analysis ----
print("\n=== AVG TITLE LENGTH ===")
title_guess.select(avg(length("title"))).show()

print("\n=== LANGUAGE COUNT ===")
meta_one.groupBy("language").count().show()

print("\n=== RELEASE DATE COUNT ===")
meta_one.groupBy("release_date").count().show()

spark.stop()

