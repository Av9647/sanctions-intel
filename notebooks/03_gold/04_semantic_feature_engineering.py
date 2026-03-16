# Databricks notebook source
# MAGIC %pip install langchain_text_splitters

# COMMAND ----------

# DBTITLE 1,Cell 1: Construct the Unified Narrative
from pyspark.sql import functions as F

silver_df = spark.table("default.silver_targets")

# 1. SEMANTIC MERGE
# We use concat_ws which automatically skips NULL values.
# If both exist, they are joined with a " | ". 
# If only one exists, it just takes that one.
gold_df = (silver_df
    # Combine Reason and Notes for maximum context
    .withColumn("full_narrative", 
        F.concat_ws(" | ", F.col("sanction_reason"), F.col("entity_notes")))
    
    # Filter out records that are completely empty
    .filter(F.col("full_narrative") != "")
    
    # Build the final document string
    .withColumn("doc_text", 
        F.concat(
            F.lit("Entity Name: "), F.col("entity_name"),
            F.lit(" | Type: "), F.col("entity_type"),
            F.lit(" | Jurisdiction: "), F.coalesce(F.col("primary_jurisdiction"), F.lit("Unknown")),
            F.lit(" | Risk Context: "), F.col("full_narrative")
        )
    )
    # Clean formatting
    .withColumn("doc_text", F.regexp_replace(F.col("doc_text"), r"[\n\r\t]+", " "))
    .withColumn("doc_text", F.regexp_replace(F.col("doc_text"), r"\s+", " "))
)

# COMMAND ----------

# DBTITLE 1,Cell 2: Token-Aware Splitting
import pandas as pd
from langchain_text_splitters import RecursiveCharacterTextSplitter

# Setting chunk size to 800 characters (~150-200 tokens)
# This keeps the context window efficient for 'all-MiniLM-L6-v2'
splitter = RecursiveCharacterTextSplitter(
    chunk_size=800, 
    chunk_overlap=100,
    separators=["|", ". ", " ", ""]
)

@F.pandas_udf("array<string>")
def chunk_text_udf(texts: pd.Series) -> pd.Series:
    return texts.apply(lambda x: splitter.split_text(x) if x else [])

# Explode the text into semantic chunks
gold_chunked_df = (gold_df
    .withColumn("text_chunks", chunk_text_udf(F.col("doc_text")))
    .withColumn("final_chunk", F.explode("text_chunks"))
    .select(
        "entity_id",
        "entity_name",
        F.col("final_chunk").alias("gold_text")
    )
)

display(gold_chunked_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Cell 3: Persist the Gold Corpus
(gold_chunked_df.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("default.gold_embeddings_corpus"))

print("Gold corpus ready for vectorization.")