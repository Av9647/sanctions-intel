# Databricks notebook source
# DBTITLE 1,Cell 1: Profiling EntitTypes & Provenance
from pyspark.sql import functions as F

bronze_df = spark.table("default.bronze_targets")

# 1. Look at types of entities we are dealing with (Person, Company, Vessel, etc.)
display(bronze_df.groupBy("schema").count().orderBy(F.desc("count")))

# 2. Inspect the provenance metadata to see where this data originates
display(bronze_df.select("id", "caption", "datasets", "first_seen").limit(10))

# COMMAND ----------

# DBTITLE 1,Cell 2: Array Complexity Check
# Understand the maximum number of sanctions or notes an entity can have
# This helps us decide whether to 'explode' (create new rows) or 'array_join' (concatenate strings)
complexity_df = bronze_df.select(
    F.max(F.size("properties.sanctions")).alias("max_sanctions_per_entity"),
    F.max(F.size("properties.notes")).alias("max_notes_per_entity")
)

display(complexity_df)