# SanctionsIntel - Semantic Intelligence for KYC & EDD

SanctionsIntel is a data engineering research project focused on the application of semantic analysis to global regulatory datasets. The project utilizes a Lakehouse-based refinement approach to bridge the gap between deterministic entity matching and narrative-driven risk intelligence.

## Conceptual Framework
Financial institutions rely on Know Your Customer (KYC) and Enhanced Due Diligence (EDD) to manage regulatory exposure. While traditional systems focus on deterministic name and identifier matching, regulatory disclosures (such as OFAC or EU sanctions) contain rich narrative descriptions detailing sanctioned behaviors and typologies. Conversely, internal EDD artifacts—business descriptions, analyst notes, and investigation summaries—describe complex behaviors that are difficult to compare systematically against known regulatory patterns.

SanctionsIntel structures these narratives into a semantically searchable knowledge base. By applying a Bronze–Silver–Gold (medallion) architecture, the system enables the surfacing of contextual risk signals when input narratives exhibit semantic similarity to established regulatory disclosures.

## Data Source
The research primarily utilizes the **OpenSanctions** dataset, specifically the **FollowTheMoney (FTM)** data model. This provides a high-fidelity, structured representation of global sanctions, PEPs, and entities of interest, including the narrative "notes" and "reasons" required for semantic analysis.

## Design Principles
- **Deterministic Transformations:** Prioritizing structured ETL over black-box automation.
- **Lakehouse Refinement:** Leveraging the Medallion architecture for data provenance.
- **Explainability:** Focusing on surfacing "Why" a match is relevant through narrative context.

## Disclaimer
This repository is an engineering research exercise. It is not a production sanctions screening system and does not perform enforcement, scoring, or compliance decisioning.

## Status
- **Architecture:** Medallion (Bronze/Silver/Gold) on Databricks
- **Vector Logic:** Embedding-based similarity search