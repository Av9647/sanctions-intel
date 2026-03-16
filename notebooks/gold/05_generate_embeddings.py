# Databricks notebook source
# MAGIC %pip install sentence-transformers

# COMMAND ----------

spark.conf.set("spark.databricks.execution.timeout", "8h") # Extends the limit to 8 hours

# COMMAND ----------

# DBTITLE 1,Cell 1: Prepare the Model (Run on Driver)
from sentence_transformers import SentenceTransformer
import pandas as pd
from typing import Iterator
import os
from pyspark.sql import functions as F

MODEL_NAME = 'all-MiniLM-L6-v2' 

def generate_embeddings_batch(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    os.environ['HF_HOME'] = '/tmp'
    os.environ['SENTENCE_TRANSFORMERS_HOME'] = '/tmp'
    model = SentenceTransformer(MODEL_NAME, cache_folder='/tmp')
    
    for pdf in iterator:
        # Generate embeddings
        embeddings = model.encode(pdf['gold_text'].tolist(), show_progress_bar=False)
        pdf['vector'] = [e.tolist() for e in embeddings]
        
        # DROPPING THE EXTRA COLUMN: 
        # We ensure only columns in our schema are returned
        yield pdf[['entity_id', 'entity_name', 'gold_text', 'vector']]

# 1. Define Output Schema
embedding_schema = "entity_id string, entity_name string, gold_text string, vector array<float>"

# 2. Prepare the data
# We select only the columns the UDF expects and repartition for better parallel performance
corpus_df = (spark.table("default.gold_embeddings_corpus")
             .select("entity_id", "entity_name", "gold_text")
             .repartition(200)) # This makes the progress bar more granular

# 3. Execute
gold_vectors_df = corpus_df.mapInPandas(generate_embeddings_batch, schema=embedding_schema)

# 4. Save
(gold_vectors_df.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("default.gold_vectors"))

print("Vectors generated.")
# Run took 5 h 33 min total duration.