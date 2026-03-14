# Databricks notebook source
# DBTITLE 1,Cell 1: Ingestion & Schema Sampling
from pyspark.sql import functions as F

# Read tiny slice (500 lines) to capture the schema
sample_df = spark.read.json("/Volumes/workspace/default/raw_data/targets.nested.json").limit(500)
target_schema = sample_df.schema

# Read full file using pre-defined schema
raw_df = spark.read.schema(target_schema).json("/Volumes/workspace/default/raw_data/targets.nested.json")

# Investigate schema to ensure 'reason' column is present
schema_json = raw_df.schema.json()

# Save it to Volume
dbutils.fs.put("/Volumes/workspace/default/raw_data/targets_schema.json", schema_json, overwrite=True)

# COMMAND ----------

# DBTITLE 1,Cell 2: Narrative Audit (The "Semantic" Look)
from pyspark.sql import functions as F

# Check how many entities have narrative text deep in the properties struct
# We use F.size()>0 to ensure the array actually contains data, rather than just being empty []
narrative_stats = raw_df.select(
    F.count("*").alias("total_records"),
    
    # Check if the 'notes' array has atleast one item
    F.sum(F.when(F.size(F.col("properties.notes"))>0, 1).otherwise(0)).alias("records_with_notes"),

    # Check if the 'sanctions' array exists which is where the 'reason' fiel lives
    F.sum(F.when(F.size(F.col("properties.sanctions"))>0, 1).otherwise(0)).alias("records_with_sanctions")
)
display(narrative_stats)

# COMMAND ----------

# DBTITLE 1,Cell 3: The Bronze Write (Persistence)
# Write nested JSON to catalog as a delta table
(raw_df.write.format("delta")
.mode("overwrite")
.option("overwriteSchema", "true")
# Specifying 'default' schema to align with Volume workspace
.saveAsTable("default.bronze_targets"))
print("bronze_targets created successfully")