from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AuthorNetwork").getOrCreate()

# -----------------------
# Demo similarity edges
# (replace with real similarity later if multiple books)
# -----------------------

edges_data = [
    ("book_A", "book_B", 0.82),
    ("book_A", "book_C", 0.65),
    ("book_B", "book_C", 0.71),
]

edges_df = spark.createDataFrame(
    edges_data,
    ["src", "dst", "similarity"]
)

print("\n=== SIMILARITY EDGES ===")
edges_df.show()

# -----------------------
# Build node list
# -----------------------

nodes_df = edges_df.select("src").union(
    edges_df.select("dst")
).distinct().withColumnRenamed("src", "id")

print("\n=== NODES ===")
nodes_df.show()

# -----------------------
# Simple influence score
# (sum of similarities)
# -----------------------

influence_df = edges_df.groupBy("src") \
    .sum("similarity") \
    .withColumnRenamed("sum(similarity)", "influence_score")

print("\n=== INFLUENCE SCORES ===")
influence_df.show()

spark.stop()

