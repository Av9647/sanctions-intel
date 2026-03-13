# SanctionsIntel - Semantic Intelligence for KYC & EDD

SanctionsIntel is a data engineering research project focused on the application of semantic analysis to global regulatory datasets. The project utilizes a Lakehouse-based refinement approach to bridge the gap between deterministic entity matching and narrative-driven risk intelligence.

## Conceptual Framework
Financial institutions rely on Know Your Customer (KYC) and Enhanced Due Diligence (EDD) to manage regulatory exposure. While traditional systems focus on deterministic name and identifier matching, regulatory disclosures (such as OFAC or EU sanctions) contain rich narrative descriptions detailing sanctioned behaviors and typologies. Conversely, internal EDD artifacts—business descriptions, analyst notes, and investigation summaries—describe complex behaviors that are difficult to compare systematically against known regulatory patterns.

SanctionsIntel structures these narratives into a semantically searchable knowledge base. By applying a Bronze–Silver–Gold (medallion) architecture, the system enables the surfacing of contextual risk signals when input narratives exhibit semantic similarity to established regulatory disclosures.

## Process
The end goal of SanctionsIntel is to build a Semantic Risk Search Engine.
Instead of just looking for a specific name (like "John Doe"), the system will be able to take a narrative description of a suspicious person and find sanctioned individuals who have done similar things.

Imagine you are a bank investigator. You receive a memo about a new potential client:

`"The subject is a high-net-worth individual involved in shipping specialized electronics to restricted regions in Eastern Europe using front companies in the UAE."`

In a traditional system, searching for that text returns zero results because no one is named "The subject."

Using SanctionsIntel system:
1. **The Input:** You paste that memo into your Databricks Dashboard.
2. **The Process (RAG):** The system "vectorizes" that text (turns the meaning into numbers).
3. **The Match:** It searches our Gold Layer and finds a 92% match with a sanctioned entity from 2024 whose reason field says: `"Exported dual-use technologies through Gulf-based intermediaries to bypass trade barriers."`
4. **The Result:** You get an alert saying: `"High Semantic Similarity to Known Sanctions Typology."`

## Data Source
The research primarily utilizes the **OpenSanctions** dataset. While the project is grounded in the **FollowTheMoney (FTM)** data model, it specifically leverages the **Nested Targets** dataset. This variant is selected for its high-fidelity inclusion of the **`reason`** and **`notes`** narrative fields, which serve as the primary corpus for semantic embedding and similarity analysis.

**`targets.nested.json`** sourced from: 
https://www.opensanctions.org/datasets/default/

## Design Principles
- **Deterministic Transformations:** Prioritizing structured ETL over black-box automation.
- **Lakehouse Refinement:** Leveraging the Medallion architecture for data provenance.
- **Explainability:** Focusing on surfacing "Why" a match is relevant through narrative context.

## Disclaimer
This repository is an engineering research exercise. It is not a production sanctions screening system and does not perform enforcement, scoring, or compliance decisioning.

## Status
- **Architecture:** Medallion (Bronze/Silver/Gold) on Databricks
- **Vector Logic:** Embedding-based similarity search
