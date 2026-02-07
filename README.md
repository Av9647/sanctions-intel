# SanctionsIntel - Semantic Intelligence for KYC & EDD

SanctionsIntel is a data pipeline exploring how semantic analysis can be applied to global sanctions datasets using a lakehouse-based refinement approach.

In banks and financial institutions, Know Your Customer (KYC) and Enhanced Due Diligence (EDD) processes rely heavily on sanctions data to assess regulatory exposure, particularly for higher-risk entities. While traditional sanctions checks focus on deterministic name and identifier matching, regulatory disclosures often include narrative descriptions explaining sanctioned behavior and underlying typologies. At the same time, EDD reviews and investigations generate narrative artifacts such as customer business descriptions, analyst notes and case summaries that describe observed behaviors but are difficult to compare systematically against known regulatory patterns.

SanctionsIntel structures sanctions narratives into a semantically searchable knowledge base, enabling the system to surface contextual risk signals when new customer or transaction narratives exhibit similarity to known sanctioned behaviors. This approach is intended to support deeper KYC and EDD analysis by providing analysts with additional behavioral context, rather than producing automated screening decisions.

The project ingests public regulatory data, normalizes it into analytical tables using a Bronze–Silver–Gold (medallion) pattern on Databricks, and applies embedding-based similarity analysis to sanction narratives for exploratory risk intelligence.

This repository is intentionally scoped as an engineering and research exercise. It is not a production sanctions screening system and does not perform enforcement or decisioning.

## Status
Project kickoff - Feb 2026
