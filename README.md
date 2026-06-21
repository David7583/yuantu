# Yuantu (源图)
### Traceable Hybrid Retrieval Knowledge Base

*Read this in [中文](README.zh-CN.md).*

> A governed, traceable knowledge layer that runs hybrid retrieval — semantic, factual, relational, and locational — over immutable source data. Built for settings where every conclusion must be traceable to its origin, such as policy analysis and interview research.

**Status:** Research prototype, not a turnkey product. The understanding layer (four databases) is complete; the action layer is in development.

## Overview

Yuantu works with non-standard, heterogeneous, long-accumulated text — typically policy documents and interview transcripts. Material like this is large and loosely structured, and when it is handed directly to a large model for retrieval, the output tends to be neither trustworthy nor traceable. This problem points in the same direction as recent work such as Andrej Karpathy's notion of an "LLM wiki" and graph-augmented retrieval (GraphRAG).

Yuantu's design departs from approaches that compress source material into model-generated "knowledge pages." Here, the original material stays immutable and serves as the single authoritative source for the whole system; models only build indexes over it and never rewrite the source. Every piece of output the system produces explicitly separates *traceable, deterministic citations* from *model-generated inferences*.

On that basis, Yuantu is not an autonomous agent, but knowledge infrastructure for an agent ecosystem — a trustworthy knowledge layer that upper layers can call, yet that is itself strictly governed.

## Background

This project grew out of a concrete constraint in policy and regional research: a conclusion that cannot be traced back to its original source cannot be used in serious analysis. Traceability is therefore not an add-on feature but the system's first design goal, and all of the principles below are derived from it.

## Design Principles

- **Data layer as sole authority.** All facts have exactly one source: the immutable original material. Every index layer is a derived view of it and may neither alter nor replace the original.
- **Unidirectional data flow.** Information flows only from the source material toward the index layers; writing back is not allowed, so that any output can be traced along a fixed path back to its origin.
- **Institution over capability.** Within the system, a model exists as a capability that is registered, invoked, and audited — not as an autonomous decision-maker. Its output is always bounded by traceable, separable limits.
- **Citation / generation separation.** Every piece of output is explicitly labeled as either a *sourced citation* or *model-generated* content.

## Architecture

The system has three layers, with only unidirectional data flow between them.

| Layer | Responsibility | Implementation |
| --- | --- | --- |
| Data | Sole authoritative source of the original material; read-only | Raw JSON |
| Understanding | Builds multiple indexes over the source for hybrid retrieval | Four databases (see below) |
| Action (in development) | Governed capability registration and invocation | Four databases (isomorphic to the understanding layer) |

The understanding layer is made of four databases. Hybrid retrieval integrates the semantic, factual, and relational dimensions, following a "locate first, then gather evidence" logic: SQLite locates the target unit, then the three axes supply the corresponding evidence. The three axes describe the same material from mutually independent angles; SQLite carries identity, metadata, and location, and is the entry point of retrieval.

| Database | Role | Notes |
| --- | --- | --- |
| SQLite | Location & metadata | Identity, aliases, and path mapping; the entry and locating layer |
| ChromaDB | Semantic axis | Vectorized semantic similarity retrieval (BAAI/bge-m3) |
| DuckDB | Factual axis | Structured facts, tens of millions of rows |
| Neo4j | Relational axis | Entity and narrative relationship graph |

The four databases are independent and never mixed.

## How it works

At a high level, the understanding pipeline runs in stages:

1. **Admission.** Raw exports (e.g. ChatGPT conversation JSON) are admitted into the data layer as recognized assets. The system originates at the data layer; this admission step is the entry valve from raw data into the understanding layer.
2. **Structural parsing.** Eligible assets are parsed into structural text units with stable segment/sentence indices and character coordinates — pure structural parsing, with no NLP model involved.
3. **Normalization & analysis.** Units are normalized into standard text units, then analyzed for structure (statistics, adjacency, co-occurrence, hierarchy) and frequency.
4. **Registration.** Units that pass governance decisions are registered as system-level structural objects with stable IDs.
5. **Indexing.** Registered units and their relations are loaded into the three axes: relations into Neo4j, structured facts into DuckDB (synced from SQLite), and semantic vectors into ChromaDB.

Every stage writes auditable run metadata, and no stage ever rewrites the source.

## Repository structure

- `scripts/` — the pipeline code, organized by layer and stage: data admission, anchoring/ingestion, structural analysis, graph building, the vector pipeline, and database synchronization.
- `data/`, `derivation/`, `chromadb/` — destinations for processed data, derived database artifacts, and vectors. The system writes here at runtime; in this repository they are shipped as an empty skeleton.
- `config/` — connection and policy configuration. Not included in this repository (see Requirements & Setup).

## Requirements & Setup

The understanding layer runs on the Python standard library plus a small set of third-party packages:

```
pip install pyyaml neo4j duckdb chromadb sentence-transformers
```

(`sentence-transformers` is only needed for local embedding; it is not required if you use an OpenAI-compatible embedding endpoint instead.)

You will also need a running Neo4j instance (default `bolt://localhost:7687`).

Configuration and policy files — database connection, parse / noise / prominence policies, and the embedding backend — are supplied by you and are **not** included in this repository. Credentials are never stored in code: the Neo4j password is read from `--password`, the `NEO4J_PASSWORD` environment variable, or a connection config file, in that order of priority; the embedding API token is read from an environment variable whose name is given in config. Sanitized example configuration and policy files will be added.

This is a research prototype. It is not a single-command install, and reproducing the full pipeline requires the source data and configuration described above.

## Project status

This project has been under development for roughly 18 months, working on a real, long-accumulated corpus (about 2,170 records, 377 MB).

| Module | Status | Notes |
| --- | --- | --- |
| Understanding layer (four databases) | Complete (2026-03) | Semantic, factual, and relational axes closed, with SQLite as the locating layer; factual axis at tens of millions of rows; relational axis at roughly 180K nodes and 230K relationships, preserving original narrative order |
| Action layer | In development | The core framework for capability registration and governed invocation is implemented and working (including real LLM API calls); some interaction and query components remain |
| Trustworthiness metrics | Experimental | Tracks two indicators: the ratio of deterministic citations to model-generated content in output, and the "half-life" of material over time |

Yuantu is currently a research prototype, not a ready-to-use product.

## Method & notes

Implementation was carried out with the help of large language models, but every architectural choice was made by a human; tools were used as material, not as authority — consistent with how the system itself treats models. The core output of this project is architectural judgment, and the code is the implementation of those judgments.

## Contact

Questions or collaboration welcome — *1138133645@qq.com; y23666176@gmail.com*.

## License

Apache License 2.0.
