# Databricks notebook source
# DBTITLE 1,Cell 1: The Normalization & Flattening Logic
from pyspark.sql import functions as F

# Load the bronze data from the Catalog
bronze_df = spark.table("default.bronze_targets")

# Refine data: Flattening nested arrays into an analytical schema
silver_df = (bronze_df
    # 1. Filter: Ensure data quality by dropping records missing core identifiers
    .filter(F.col("id").isNotNull() & F.col("caption").isNotNull())

    # 2. Explode: Create a unique row for every sanction entry found in the nested array
    .withColumn("sanction_item", F.explode_outer("properties.sanctions"))

    # 3. Extract Reasons: Join narrative reasons; return NULL if array is empty
    .withColumn("sanction_reason", 
        F.when(F.size(F.col("sanction_item.properties.reason")) > 0, 
               F.concat_ws(" | ", F.col("sanction_item.properties.reason")))
        .otherwise(F.lit(None)))

    # 4. Extract Programs: Join sanction programs (e.g., 'SDGT', 'UKRAINE-EO')
    .withColumn("sanction_program", 
        F.when(F.size(F.col("sanction_item.properties.program")) > 0, 
               F.concat_ws(", ", F.col("sanction_item.properties.program")))
        .otherwise(F.lit(None)))

    # 5. Flatten Notes: Join all general entity notes into a single text block
    .withColumn("entity_notes", 
        F.when(F.size(F.col("properties.notes")) > 0, 
               F.concat_ws(" \n ", "properties.notes"))
        .otherwise(F.lit(None)))

    # 6. Metadata: Extract primary jurisdiction and join provenance source list
    .withColumn("primary_jurisdiction", F.col("properties.country")[0])
    .withColumn("provenance_sources", F.concat_ws(", ", "datasets"))

    # 7. Project: Select final analytical columns with clean aliases
    .select(
        F.col("id").alias("entity_id"),
        F.col("caption").alias("entity_name"),
        F.col("schema").alias("entity_type"),
        "primary_jurisdiction",
        "provenance_sources",
        "first_seen",
        "sanction_reason",
        "sanction_program",
        "entity_notes"
    )
)

# Preview: Filter for records that contain usable narrative for RAG
display(silver_df.filter(F.col("sanction_reason").isNotNull() & F.col("entity_notes").isNotNull()).limit(10))

# COMMAND ----------

# DBTITLE 1,Cell 2: Data Quality Audit
# Measure data density: Count records containing actual narrative payload
quality_audit = silver_df.select(
    F.count("*").alias("total_rows"),
    F.count("sanction_reason").alias("valid_reasons"), # count() ignores NULLs
    F.count("entity_notes").alias("valid_notes")
)

display(quality_audit)

# COMMAND ----------

# DBTITLE 1,Cell 3: Persist to Silver
# Save refined dta as a Managed Delta Table
(silver_df.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("default.silver_targets"))

print("'silver_targets' created with 1:Many sanction granularity.")