from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, lit,
    collect_list, concat_ws
)

from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.linalg import DenseVector, SparseVector
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder.appName("TFIDFSimilarity").getOrCreate()

# -----------------------------
# Load Book File
# -----------------------------
df = spark.read.text("/home/ssk7/wc_project/200.txt")

books_df = df.select(
    lit("200.txt").alias("file_name"),
    col("value").alias("text")
)

print("\n=== RAW SAMPLE ===")
books_df.show(5, truncate=False)

# -----------------------------
# Combine all lines -> one doc
# -----------------------------
doc_df = books_df.groupBy("file_name").agg(
    concat_ws(" ", collect_list("text")).alias("text")
)

# clean text
doc_df = doc_df.select(
    col("file_name"),
    regexp_replace(lower(col("text")), "[^a-z ]", " ").alias("text")
)

print("\n=== CLEAN DOC SAMPLE ===")
doc_df.show(truncate=120)

# -----------------------------
# Tokenize
# -----------------------------
tokenizer = Tokenizer(inputCol="text", outputCol="words")
words_df = tokenizer.transform(doc_df)

# -----------------------------
# Remove Stopwords
# -----------------------------
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
filtered_df = remover.transform(words_df)

# -----------------------------
# TF
# -----------------------------
tf = HashingTF(
    inputCol="filtered",
    outputCol="rawFeatures",
    numFeatures=10000
)

tf_df = tf.transform(filtered_df)

# -----------------------------
# IDF
# -----------------------------
idf = IDF(inputCol="rawFeatures", outputCol="tfidf")
idf_model = idf.fit(tf_df)
tfidf_df = idf_model.transform(tf_df)

print("\n=== TFIDF VECTORS ===")
tfidf_df.select("file_name", "tfidf").show(truncate=False)

# -----------------------------
# Cosine Similarity
# -----------------------------
def cosine_sim(v1, v2):
    if isinstance(v1, SparseVector):
        v1 = v1.toArray()
    if isinstance(v2, SparseVector):
        v2 = v2.toArray()

    dot = float(v1.dot(v2))
    norm1 = float((v1 @ v1) ** 0.5)
    norm2 = float((v2 @ v2) ** 0.5)

    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot / (norm1 * norm2)

cosine_udf = udf(cosine_sim, DoubleType())

# self join to compute similarity
sim_df = tfidf_df.alias("a").crossJoin(tfidf_df.alias("b")) \
    .select(
        col("a.file_name").alias("book_a"),
        col("b.file_name").alias("book_b"),
        cosine_udf(col("a.tfidf"), col("b.tfidf")).alias("cosine_similarity")
    )

print("\n=== COSINE SIMILARITY ===")
sim_df.show(truncate=False)

# -----------------------------
spark.stop()

