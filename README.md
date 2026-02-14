# CSL7110 — Big Data Frameworks — Assignment 1

Name: Sai Swaroop  
Roll No: M25DE1023  
Course: CSL7110 — ML with Big Data

---

This assignment demonstrates end-to-end Big Data processing using:

- Apache Hadoop (HDFS + MapReduce)
- Apache Spark (DataFrames + ML features)
- Custom MapReduce implementation
- TF-IDF feature engineering
- Cosine similarity computation
- Influence network modeling

All tasks were executed on Ubuntu 22.04 (WSL2) single-node setup.

---

# Repository Structure

spark_assign/
metadata_extraction.py
tfidf_similarity.py
author_network.py

wc_project/
WordCount.java
wc.jar


---

# Hadoop Tasks Completed

## Hadoop Setup
- Installed Hadoop 3.3.6
- Configured JAVA_HOME and Hadoop environment
- Configured core-site.xml, hdfs-site.xml, yarn-site.xml
- Started HDFS and YARN services
- Verified using jps

## WordCount (Built-in)
Executed Hadoop example WordCount job on sample dataset.

## Custom WordCount (Java MapReduce)
Implemented custom Mapper and Reducer:
- punctuation removal
- lowercase normalization
- tokenization
- aggregation

Compiled and packaged into JAR and executed successfully.

## Large Dataset Experiment
- Used Project Gutenberg book (~8MB)
- Uploaded to HDFS
- Executed custom WordCount
- Measured job execution time
- Modified split size to observe mapper count changes

---

# Spark Tasks Completed

## Spark Setup
- Installed Spark 3.5.x
- Verified with spark-submit and pyspark

## Metadata Extraction
Script: `metadata_extraction.py`

- Loaded book text into Spark DataFrame
- Extracted metadata using regex
- Observed regex limitations due to inconsistent headers
- Implemented fallback title extraction
- Computed simple metadata statistics

---

## TF-IDF Feature Engineering
Script: `tfidf_similarity.py`

Pipeline:
- text cleaning
- tokenization
- stopword removal
- HashingTF
- IDF model
- TF-IDF vector generation

---

## Cosine Similarity
- Implemented cosine similarity using Spark UDF
- Compared TF-IDF vectors
- Produced similarity scores between documents

---

## Influence Network
Script: `author_network.py`

- Built similarity edge list
- Constructed node list
- Computed influence score as sum of similarity weights
- Demonstrated graph-style relationship modeling

---

# Key Concepts Demonstrated

- HDFS storage model
- MapReduce execution flow
- Split size impact on parallelism
- Replication factor tradeoffs
- Regex extraction limitations
- TF vs IDF importance
- Vector similarity measurement
- Graph influence scoring

---

# How to Run

## Hadoop WordCount

hadoop jar wc.jar WordCount /input /output


## Spark Metadata Extraction

spark-submit spark_assign/metadata_extraction.py


## Spark TF-IDF

spark-submit spark_assign/tfidf_similarity.py


## Spark Network

spark-submit spark_assign/author_network.py


---

# Notes

- All scripts executed using spark-submit
- Outputs and execution logs are included in the assignment report PDF

