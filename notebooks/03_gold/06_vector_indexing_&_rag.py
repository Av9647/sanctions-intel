# Databricks notebook source
# MAGIC %pip install sentence-transformers faiss-cpu
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Cell 1: Building the FAISS Search Index
import faiss
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer

print("1. Loading vectors from the permanent Delta table...")
# Pulling the 1.19M rows into the driver memory
# This should take about 1-2 minutes
pdf = spark.table("default.gold_vectors").select("entity_id", "vector").toPandas()

print("2. Converting to search format...")
# Transform the lists into a high-performance NumPy matrix
embeddings = np.array(pdf['vector'].tolist()).astype('float32')

print("3. Building the FAISS Index...")
dimension = 384 
index = faiss.IndexFlatL2(dimension)
index.add(embeddings)

# 4. Define the search function
def search_entities(query_text, k=5):
    model = SentenceTransformer('all-MiniLM-L6-v2')
    query_vector = model.encode([query_text]).astype('float32')
    
    distances, indices = index.search(query_vector, k)
    
    # Use the 'pdf' we just loaded for the mapping
    results = pdf.iloc[indices[0]].copy()
    results['similarity_score'] = distances[0]
    return results

print(f"Success! Brain rebuilt with {index.ntotal} vectors.")

# COMMAND ----------

# DBTITLE 1,Cell 2: Test Search
# --- TEST SEARCH ---
test_query = "aviation industries involving drones or composites"
matches = search_entities(test_query)

# 1. Convert to Spark and remove redundant chunks of the same company
search_results_df = spark.createDataFrame(matches[["entity_id", "similarity_score"]])

# 2. Join and deduplicate
final_candidates = (search_results_df
    .join(spark.table("default.gold_vectors"), on="entity_id")
    .dropDuplicates(["entity_id"]) # Only show each company once
    .select("entity_name", "similarity_score")
    .orderBy("similarity_score")
)

print("Top Unique Industry Matches:")
display(final_candidates)

# COMMAND ----------

# DBTITLE 1,Cell 3: Check Available Endpoints
import mlflow.deployments
client = mlflow.deployments.get_deploy_client("databricks")

# Using dictionary syntax ['name'] instead of dot notation .name
endpoints = client.list_endpoints()
endpoint_names = [e["name"] for e in endpoints]

print("Available Endpoints:")
print(endpoint_names)

# COMMAND ----------

# DBTITLE 1,Cell 4: LLM Generation
# 1. Pick the best performing model from your list
FINAL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

try:
    # 2. Extract the top hit from your earlier search
    # We use .first() because we only want to analyze the #1 best match for now
    best_match_row = final_candidates.first()
    company_name = best_match_row["entity_name"]
    score = best_match_row["similarity_score"]

    # 3. Create a targeted prompt
    prompt = f"""
    Context: A vector search of 1.19 million industry records identified '{company_name}' 
    as the most relevant entity (Similarity Distance: {score:.4f}).

    User Question: "I am looking for aviation industries involving drones or composites."

    Task: Briefly explain why this company is a highly relevant match for drones or aviation composites.
    """

    # 4. Call the LLM
    response = client.predict(
        endpoint=FINAL_ENDPOINT,
        inputs={
            "messages": [
                {"role": "system", "content": "You are an expert industrial analyst. Provide concise, factual insights."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 300,
            "temperature": 0.1 # Keep it factual and consistent
        }
    )
    
    # 5. Output the result
    # Note: Using dictionary access response['choices'] to avoid AttributeError
    print(f"\n✅ ANALYSIS COMPLETE FOR: {company_name}")
    print("-" * 50)
    print(response["choices"][0]["message"]["content"])

except Exception as e:
    print(f"Error during generation: {e}")