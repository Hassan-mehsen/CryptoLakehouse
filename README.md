# CryptoLakehouse

A French version 🇫🇷 of this documentation is available [here](READMOI.md)

## Table of Contents

1. [Introduction & Project Pitch](#1-introduction--project-pitch)
2. [API Constraints & Architecture Decisions](#2-api-constraints--architecture-decisions)
3. [Lakehouse Global Architecture](#3-lakehouse-global-architecture)
4. [Data Lake Structure & Role](#4-data-lake-structure--role)
5. [Data Warehouse Architecture & Modeling](#5-data-warehouse-architecture--modeling)
6. [Extraction Layer](#6-extraction-layer)
    - [6.1 Data Extraction - Architecture, Design, and Implementation](#61-data-extraction---architecture-design-and-implementation)
    - [6.2 Sequential Schemas & Code Samples - Data Engineering Extraction](#62-sequential-schemas--code-samples---data-engineering-extraction)
    - [6.3 Best Practices & Data Engineering Highlights - CryptoLakehouse Extraction](#63-best-practices--data-engineering-highlights---cryptolakehouse-extraction)
7. [Transformation Layer](#7-transformation-layer)
    - [7.1 Spark Architecture & OOP - Overview](#71-spark-architecture--oop---overview)
    - [7.2 OOP Architecture - Code Structure](#72-oop-architecture---code-structure)
    - [7.3 Execution Logic of the Spark & OOP Pipeline](#73-execution-logic-of-the-spark--oop-pipeline)
    - [7.4 Detailed Example of a Business Transformer](#74-detailed-example-of-a-business-transformer)
    - [7.5 Optimizations, Robustness & Scalability](#75-optimizations-robustness--scalability)
    - [7.6 Highlights & Best Practices](#76-highlights--best-practices)
8. [Load Layer](#8-load-layer)
    - [8.1 Architecture & Overview - Load Layer](#81-architecture--overview---load-layer)
    - [8.2 OOP Architecture - Code Structure](#82-oop-architecture---code-structure)
    - [8.3 Execution Logic of the Load Pipeline & OOP](#83-execution-logic-of-the-load-pipeline--oop)
    - [8.4 Business Pattern: Loading a Fact Table](#84-business-pattern-loading-a-fact-table)
    - [8.5 Optimizations, Robustness & Scalability](#85-optimizations-robustness--scalability)
    - [8.6 Highlights & Best Practices](#86-highlights--best-practices)
9. [Data Warehouse, SQL & Migrations - Structure & Roles](#9-data-warehouse-sql--migrations---structure--roles)
10. [Orchestration & Automation with Airflow](#10-orchestration--automation-with-airflow)

**In Progress:**

11. [Docker](#11-docker)
12. [Results Highlights + Optimizations](#12-results-highlights--optimizations)
13. [Installation & Startup Guide](#13-installation--startup-guide)

---

## 1. Introduction & Project Pitch

Welcome to **CryptoLakehouse**, the data platform designed to cover the full range of analytical, decision-making, and strategic needs for companies in the crypto sector: investment management, market trend anticipation, and advanced reporting.

**CryptoLakehouse** provides an “ML-ready” architecture: all data is historized, enriched, cleaned, and structured, making it easy to integrate machine learning models at any stage.

**CryptoLakehouse** centralizes the entire crypto data lifecycle, from automated ingestion via API to value creation for BI and ML, including decision modeling (Data Warehouse), metadata management (logs, quality, lineage), and advanced visualization (Power BI, Metabase).

Built for easy integration into enterprise architectures, the solution combines:

- an **“ML-ready” Data Lake** (Parquet and Delta), enabling exploration, machine learning model training, and large-scale transformation,
- a high-performance **Data Warehouse** (PostgreSQL, star/snowflake schema) optimized for BI and reporting,
- industrialized **orchestration** (Apache Airflow),
- full **containerization** (Docker) for rapid and reliable deployment,
- and a **metadata management layer** (process tracking, logs, monitoring, data lineage).

---
### Context & Challenge

| **Context**  | Crypto companies need advanced analytics tools to steer investment, anticipate trends, and provide relevant recommendations with machine learning. However, public APIs such as CoinMarketCap don’t provide historical data, which limits analysis depth and predictive model relevance.  |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Challenge** | Build a daily historical record, enrich and transform raw data, calculate business KPIs, and deliver a robust data architecture: **ML-ready Data Lake** for data science, modeling, and ML; a Data Warehouse optimized for BI/reporting; all supervised by a metadata layer ensuring traceability and quality. |

---
### ELT Architecture

**CryptoLakehouse** adopts a modern **ELT** architecture:

- **Extract**: automated data extraction from crypto market APIs,
- **Load**: direct loading of raw data into an **ML-ready Data Lake** (Parquet),
- **Transform**: cleaning, enrichment, KPI calculation, and large-scale analytical preparation with Spark (saving transformed data in Delta format within the data lake),
- **Final Load**: loading analysis-ready data into a Data Warehouse for BI and reporting.

The pipeline is fully orchestrated and monitored via Airflow, Docker, and metadata management.

---
### Main Objectives

- **Industrialize crypto data ingestion and transformation**: automated extraction, advanced processing, reliable and scalable storage.
- **Provide a solid analytical foundation to generate business KPIs**: volatility, return, market cap, dominance, trends, and correlations.
- **Centralize, historize, and valorize crypto data** for BI, advanced reporting, and data science/machine learning.
- **Offer an “ML-ready” platform**: extraction, cleaning, enrichment, and preparation of data for AI and predictive analytics in a dedicated Data Lake.
- **Ensure industrialization and automation** thanks to Airflow, Docker, and **object-oriented frameworks (OOP) covering all pipeline phases**, from extraction to data valorization.
- **Demonstrate complete mastery of the data project lifecycle**: extraction, transformation, load, DWH modeling, orchestration, Docker deployment, technical documentation.
- **Provide a robust analytical foundation** to power Power BI/Metabase dashboards and support data-driven decision making.

---

### Why CryptoLakehouse?

In a hyper-dynamic crypto market, having a solution that unifies **ingestion, preparation, analysis, and exploitation of data** is a major asset for any company: business steering, strategic monitoring, opportunity detection, optimized decision-making, and acceleration of data science and AI projects.

**CryptoLakehouse** embodies this modern approach:

- “Lakehouse” architecture (ML-ready Data Lake + decision-oriented Data Warehouse)
- Automated, scalable, and monitored **ELT pipeline**
- Platform ready for BI and machine learning
- Metadata management, traceability, and professional governance
- Easily extensible and maintainable

---

## 2. API Constraints & Architecture Decisions

The CryptoLakehouse pipeline is **designed to maximize the analytical value of crypto data while respecting the strict constraints imposed by public APIs** (e.g. CoinMarketCap).

#### **Key Constraints**
- **Strict monthly quota (e.g. 10,000 credits/month)**:  
    - Each API call "costs" credits, which requires **precise control over extraction frequency and data volume**.
- **Variability of endpoints**:  
    - Some endpoints are highly dynamic (prices, markets, metrics: 5x or 10x/day), while others are stable (mappings, categories, info: daily/weekly).
- **No historical data**:  
    - APIs only provide the current snapshot → **the pipeline logic is fully oriented toward “incremental archiving”** (each snapshot is stored and timestamped to guarantee historical analysis and reproducibility).
- **Weekday-only scheduling**:  
    - Collection focuses on business days (Monday-Friday) to **reduce API costs without sacrificing business value** (the market is almost stable during weekends, BI workflows are week-oriented).

#### **Structuring Decisions in the Pipeline**
- **Frequency-based orchestration**:  
    - Refresh frequency is **not defined by domain**, but by the **nature and business dynamics** of each data set.
        * **The most dynamic fact tables** (market, global metrics, sentiment, etc.) are orchestrated at high frequency (up to 10x/day) to ensure maximum freshness where the analytical impact is highest.
        * **Stable dimensions** and reference data, which are less volatile, are refreshed daily or weekly depending on business criticality.
        * This approach allows for **optimization of freshness, performance, and processing cost**.
- **Fact-first strategy**:  
    - Freshness priority goes to fact tables (market, global metrics, sentiment), which directly feed BI, reporting, and future data science models.
- **Incremental extraction**:  
    - “Full scan” only at initialization,
    - Afterwards, only new or updated data is extracted (incremental), which drastically reduces API credit consumption.
- **Logging and auditability**:  
    - All extractions are **tracked in metadata files and structured logs** (with API stats) to ensure transparency, analysis, and the ability to replay or audit each run.
- **Robustness**:  
    - Native handling of API errors (retry, exponential backoff, fallback), compatibility with Airflow/crons, and automated monitoring.
- **Strategic buffer**:  
    - The strategy always ensures a **credit margin (>35%)** to absorb any workload spikes or incidents.

#### **Concrete Benefits**

- **Maximizes freshness of key data** for BI/ML, while never exceeding the API budget.
- **Production-ready pipeline**: traceable, ready to scale (paid API tier, new endpoints, etc.).
- **Auditability-oriented, fail-soft architecture**: no extraction is lost, all incidents can be recovered without technical debt.

 **In summary:**
>
> The pipeline is orchestrated **not “by domain”**, but **according to the real dynamics of the data**, ensuring freshness where it’s critical and efficiency elsewhere.
> This data-driven strategy guarantees **performance, robustness, and cost control** at every stage.

**Bottom line:**  
> API constraints are not a blocker, but a **driver of innovation**: they guided the pipeline design, its smart frequency-based orchestration, systematic historization, and the robustness that define CryptoLakehouse.
>
>_These technical choices, dictated by API realities, ensure **industrialization**, **resilience**, and **scalability** ready for business scale-up._

---

## 3. Lakehouse Global Architecture

The architecture of **CryptoLakehouse** is built on the Lakehouse pattern:  
It combines the flexibility and scalability of a **multi-layer Data Lake** (bronze, silver, gold) with business-oriented analytical structuring, optimized for reporting and decision-making through a **Data Warehouse**.

An **ELT pipeline** feeds and transforms data within the Lakehouse, orchestrated by **Airflow** and made portable via **Docker**.  
Quality and traceability are ensured by careful **metadata management** at every stage.

![lakehouse](/docs/diagrams/lakehouse/lakhouse.png)

### **Pipeline Steps:**
1. **Ingestion**: automated extraction from APIs to the **bronze** layer (raw, historized in Parquet)
2. **Transformation & enrichment** (with Spark): move to the **silver** layer (cleaned, ML/BI-ready, with a Delta layer to guarantee ACID properties and full traceability)
3. **Load** into the **Data Warehouse**: business analysis, BI, and reporting
4. **Backup**: Data Warehouse backup to the **gold** layer

### **Key Tools:**
- **Airflow**: orchestration and automation of the entire pipeline
- **Docker**: portability and reproducible deployment across all environments
- **Metadata**: centralized management of quality, logs, and traceability at every stage

---

**This architecture ensures:**
- **Complete historization**: raw and transformed data is preserved at every stage (bronze/silver/gold, metadata management)
- **Scalable & industrialized pipeline**: automation, orchestration (**Airflow**), and containerization (**Docker**)
- **Data ready for data science, ML & BI**: cleaned, enriched, production-ready data (silver/gold, Data Warehouse)
- **Robust governance & traceability**: centralized quality, logs, and auditing via **metadata**

---

## 4. Data Lake Structure & Role

![datalake](/docs/diagrams/lakehouse/datalake.png)

The CryptoLakehouse Data Lake is structured into three layers:

- **Bronze**: raw storage of data as extracted from the APIs, in Parquet format (chosen for better resource optimization and efficiency compared to the native JSON format from APIs).  
  This layer guarantees the historization and traceability of the original data.

- **Silver**: data is cleaned, enriched, and transformed using Spark, with Delta Lake to ensure ACID properties, optimize Parquet file management, and reinforce processing traceability.  
  These datasets are ready for advanced analytics, data science, and machine learning.

- **Gold**: this layer is mainly used to store backups or snapshots of the Data Warehouse, ensuring resilience, possible restoration, and data auditing.  
  It can also host final datasets produced by data science or machine learning pipelines, such as:  
    - **predictive scores**  
    - **crypto value predictions**  
    - **market index predictions**  
    - **algorithm results (clustering, classification, etc.)**  
    - **enriched business tables ready for BI**

Each layer serves a specific purpose: preservation, preparation, or valorization of data, and careful metadata management ensures governance at every stage.

----

## 5. Data Warehouse Architecture & Modeling

**The CryptoLakehouse Data Warehouse** is designed to be **modular**, **scalable**, and **analytics-oriented**, with the goal of centralizing, historizing, and connecting all critical data from the crypto ecosystem.

**Objective**: optimize both **business performance** (fast BI queries, advanced analytics) and **technical robustness** (governance, historization, scalability).

### Architecture Approach

- **Hybrid star and snowflake model**:
    - **Fact tables** follow a **star schema** (**denormalized** for **fast joins** and straightforward BI usage).
    - **Dimensions** are **modular** and **normalized** (**snowflake schema**): split into **stable entities** (`dim_id`), **enrichments** (`dim_info`), and **evolving** entities (`dim_map`) to maximize **maintainability** and **scalability**.

- **Systematic temporal historization**:
  The **`snapshot_timestamp`** column is present in **all tables** in the Data Warehouse.
  - **Fact tables** and **evolving tables (`dim_map`)**: operate as **pure append-only** (**time key**), each entry = **new snapshot**, **no data is ever overwritten** or modified.
  - **Static tables (stable dimensions)**: **append-only** management with **filtering logic**: Spark compares the **primary keys** already present in the Data Warehouse to the new ones; only **missing keys are added** (“append”), guaranteeing **integrity** and **non-duplication**.
  This mechanism ensures **complete historization**, **traceability of changes**, and **efficient management of static dimensions**.

- **Complex relationships managed**:
    - **Many-to-many relations** (e.g. cryptos ↔ categories) are modeled with dedicated **link tables** (`link_crypto_category`), allowing for advanced sectoral analyses.

- **Thoughtful normalization**:
    - Each dimension is split by **update frequency** (static, evolving), **data type** (identity, enrichment, variation), and **anticipated scalability**.
    - **Rarely modified dimensions** are separated from evolving dimensions, minimizing **I/O costs** and **redundancy**.
    - **No field is needlessly duplicated**: “volatile” data goes into **fact tables**, “static” data into **dimensions**, **enrichments** into **extensions**.

This structure enables **optimized storage costs**, **robust BI analytics**, and is ready for the integration of **advanced analytics** (sentiment analysis, ML predictions, macro and sectoral indicators).


### Diagrams and Domain Examples

Below are the UML diagrams for the Data Warehouse domains:

#### Exchange Domain

![exchange_domain.png](/docs/diagrams/dwh/exchange_domain.png)

#### Crypto Market Domain

![crypto_domain.png](/docs/diagrams/dwh/crypto_domain.png)

#### Categories Domain

![crypto_category_domain.png](/docs/diagrams/dwh/crypto_category_domain.png)

#### Global Market Domain

![global_market_domain.png](/docs/diagrams/dwh/global_market_domain.png)

#### Sentiment Domain

![fear_greed_domain.png](/docs/diagrams/dwh/fear_greed_domain.png)


These diagrams illustrate:
- Central **fact tables** (measures, snapshots, etc.)
- Enriched and hierarchical **dimensions** (identity, info, map, categories)
- Complex links (e.g. N-N cryptos ↔ categories)
- An append-only organization for auditing, time series, and future predictions



### General Schema: Modular Star with Snowflake Components

| Aspect                     | Status | Comment                                                                                                                                |
| -------------------------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| Star schema                | Yes    | Fact tables (`fact_market_data`, `fact_exchange_assets`, etc.) point to clear dimensions (`dim_crypto_id`, `dim_exchange_id`, etc.)     |
| Modularity                 | Yes    | Each dimension is split into modules: identity, info, metrics, ensuring clarity and optimal maintainability                             |
| Partial snowflake          | Yes    | Normalized dimensions and link tables (e.g. `link_crypto_category`) reduce redundancy, factorize information, and handle N-N/hierarchies|
| Smart historization        | Yes    | `snapshot_timestamp` present everywhere                                                                                                |
| Complex relations managed  | Yes    | N-N relations (crypto ↔ category), hierarchical links, business evolution                                                               |
| Scalability                | Yes    | Model ready for tens of thousands of entities (crypto, exchange, categories, platforms, etc.)                                           |
| Rich analytical exploration| Yes    | Dashboards by coin, exchange, category, platform, sentiment, etc.                                                                       |

---
### Why a Hybrid (Star/Snowflake) Model?

This architectural choice combines the best of both worlds:

- **Analytical simplicity and speed for BI**: star schema for fact tables makes joins simple, queries fast, and the model easily usable by business analysts.
- **Robustness, scalability, and cost control**: partial snowflake normalization for some dimensions helps factorize information, avoid redundancy, separate responsibilities, and maintain the model even when adding new attributes or entities.
- **Business adaptability**: this flexibility makes it easy to adjust granularity or structure as needed for analysis or as the crypto ecosystem evolves.

**In summary:**  
The CryptoLakehouse Data Warehouse architecture combines:
- Ease of use and optimal analytical performance for BI,
- Guaranteed robustness, scalability, and flexibility,
- Full historization and traceability,
- Data governance and native openness to data science,  
all while optimizing resources and maintainability for the project.

---

## 6. Extraction Layer

### 6.1 Data Extraction - Architecture, Design, and Implementation

The extraction phase is the **entry point of the ELT pipeline**.
Its role is to **collect, historize, and ensure the reliability** of data from multiple API endpoints, guaranteeing:

- **Full traceability** of every extraction (metadata, logs)
- **Quality and integrity** of stored data: error checks, validation, **automatic fallback mechanisms** in case of extraction failures
- **Systematic historization** (every snapshot is kept, timestamped, and never overwritten)
- **Modularity and scalability** (OOP architecture, new extractors easily added)
---

 **1. Extraction Challenges & Constraints**

- **Multiple endpoints**: each endpoint (e.g., `/v1/cryptocurrency/listings/latest`, `/v1/global-metrics/quotes/latest`, etc.) has its own data formats, parameters, frequencies, and call limits.
- **API limitations**: handling throttling, lack of historical data, quota management, etc.
- **Historization & traceability**: every extraction snapshot must be kept for auditability, reproducibility, and to rebuild a reliable history.
- **Storage format**:
    - APIs return data in **JSON**.
    - **But for incremental daily historization**, storing each snapshot as JSON would be very costly in disk space and memory for large-scale analytics.
    - **Parquet format is preferred**: it compresses efficiently, enables fast selective reads, and integrates perfectly with Spark.

---

 **2. Object-Oriented Extraction Architecture**

 **Design Schema**

- **Parent class `BaseExtractor`**:
    - **Role**: Defines the shared functional base for all extractors, enforces a strict structure, and centralizes cross-cutting features.
    - **Responsibilities**:
        - Detailed logging (start, status, errors, metrics)
        - Reading/writing **metadata files** (JSON), specific to each extractor
        - API call handling (using `requests`, header management, retries…)
        - Writing data in optimized `.parquet` format, with timestamped filenames (`filename-YYYYMMDD_HHMMSS.parquet`)
        - Managing traceability (timestamp, snapshot, cross-references)
        - Requires implementation of `run()` (full extraction logic) and `parse()` (cleaning/structuring the API payload)
    - **Interface**:

        ```python
        from abc import ABC, abstractmethod

        class BaseExtractor(ABC):

            @abstractmethod
            def run(self):
                """Main extraction logic (must be implemented by subclasses)."""
                pass

            @abstractmethod
            def parse(self, data):
                """Specific parsing/cleaning logic (must be implemented by subclasses)."""
                pass

            # Other shared utilities: log(), save_metadata(), load_metadata(), save_parquet(), etc.
        ```

- **Specific extractors (`ExchangeExtractor`, `CryptoInfoExtractor`, etc.)**
    - **Role**: Each extractor inherits from `BaseExtractor` and **must implement** the `run()` and `parse()` methods.
    - **Extension**: Each extractor **can also add other methods** as needed for the endpoint (pagination, retry, business enrichment…).
    - **Class example**:
        ```python
        class CryptoInfoExtractor(BaseExtractor):
            def run(self):
                # Extraction logic for /v1/cryptocurrency/info
                ...
            def parse(self, raw_data):
                # Specific parsing for this endpoint
                ...
            # + additional methods as needed (e.g., retry management, formatting)
        ```

- **Traceability & historization**
    - Each extractor saves its `.parquet` files as:
        ```python
        filename_timestamped = f"{filename}-{self.timestamp_str}.parquet"
        ```
    - **Systematic historization and auditability**, no file is ever overwritten
    - Each extractor has its own **metadata file** containing:
        - Snapshot date
        - Source endpoint and parameters
        - Upstream/downstream snapshot refs (`extract_snapshot_ref`, `load_snapshot_ref`)
        - Extraction metrics (`num_loaded`, `num_to_fetch`, …)
        - Any status or errors

---

**3. Logging & Monitoring**

- **Each extractor** logs the entire process:
    - start of extraction
    - number of extracted rows, anomalies detected, API errors, delays
    - success or failure of each key step (API call, parsing, Parquet write, metadata save…)
- Logs are stored in a dedicated folder and can be used for monitoring or debugging (or via Airflow).

---

**4. Metadata Management & Reliability**

- **Unique metadata file per extractor**, JSONL format for appending, e.g.:
    ```jsonl
    {"snapshot_date": "2025-06-30 08:00:01", "total_count": 828, "source_endpoint": "/v1/exchange/info", "exchange_map_snapshot_ref": "2025-06-24 09:42:27"}
    ```
- This enables:
    - **Audit and traceability** of every execution
    - **Easy recovery** (in case of incident or failure)
    - **Quality control** (consistency in number of entities, tracking treated/skipped IDs…)

---

**5. Key Benefits of This Structure**

- **Industrialization** of extraction (robustness, logging, recovery on failure)
- **Extensibility**:
    - **adding a new endpoint** = create a new child extractor
    - **endpoint changes** = adapt the extractor class by adding/modifying methods
- **Traceability and governance** of data at every step
- **Automatic historization** (snapshot versioning, timestamped Parquet storage)
- **Optimized storage** (Parquet)
- **Separation of concerns**: each extractor is specific to its endpoint, shared code is factorized and tested.

---

### 6.2 Sequential Schemas & Code Samples - Data Engineering Extraction

The CryptoLakehouse extraction architecture relies on robust, industrial workflows-both traceable and optimized for big data.  
Here are three key examples illustrating the main patterns:

---

**1. Mapping Active Exchanges - `ExchangeMapExtractor`**

![ExchangeMapExtractor Sequence Diagram](docs/diagrams/extractors/ExchangeMapExtractor.png)  
*Manages snapshots, diffs, and fallback for building the exchanges map (API usage optimization, auditability, change control).*

---

 **2. Resilient Extraction of Assets by Exchange - `ExchangeAssetsExtractor`**

![ExchangeAssetsExtractor Sequence Diagram](docs/diagrams/extractors/ExchangeAssetsExtractor.png)  
*Streaming, retry/backoff, and error tolerance for extracting assets per exchange, with automatic historization and run traceability.*

---

 **3. Real-time & Scalable Extraction of Crypto Listings - `CryptoListingsLatestExtractor`**

![CryptoListingsLatestExtractor Sequence Diagram](docs/diagrams/extractors/CryptoListingsLatest.png)  
*Optimized pagination, chunk processing, and auto rate-limit management to capture reliable crypto market snapshots in real time.*

---

**Key Code Snippets**

_Main `run()` method (extraction & storage orchestration, ExchangeAssetsExtractor)_

![run() method](docs/code_snaps/code_snap_run.png)
 _Streaming method with `yield` (robust handling, ExchangeAssetsExtractor)_

![fetch_assets_per_exchange_with_recovery() method](docs/code_snaps/code_snap_yield.png)

---

**Example of Structured Extractor Log**

![Extraction log example](docs/code_snaps/code_snap_log.png)

---

> *For full documentation of all workflows and extractors (10+ sequential diagrams), see [docs/diagrams/extractors/](/docs/diagrams/extractors/).*
>
> _For full logs, see [logs/extract.log](logs/extract.log)_

---

### 6.3 Best Practices & Data Engineering Highlights - CryptoLakehouse Extraction

The CryptoLakehouse extraction layer was designed to **incorporate the best practices in data engineering**: robustness, scalability, auditability, and business optimization. Each extractor is engineered as a **production-ready component**, with a strong focus on **business value** and **technical performance**.

 - **1. Scalability & “Big Data Ready” Performance**

    - **Chunk/stream processing (`yield`)**: supports thousands of extractions continuously without ever saturating RAM.
    
    - **Pagination & bulk extraction**: all extractors leverage API capabilities (pagination, limit=5000, 100-item batches) to minimize calls and speed up ingestion.
    
    - **Low memory footprint**: immediate parsing, controlled DataFrame construction, conditional storage.
    

 - **2. Resource & API Cost Optimization**

    - **Diff/snapshot & change detection**: each extractor only processes what has actually changed (cryptos, exchanges, categories), avoiding unnecessary refetch or writes.
    
    - **Historical tracking for efficiency**: active IDs, whitelists, and “progressive enrichment” reduce quotas consumed while ensuring freshness.
    
    - **Anti-redundancy strategies**: no endpoint is called twice for the same data.

    
 -  **3. Resilience, Robustness & Error Tolerance**

    - **Retry management with exponential backoff**: all API calls are protected against network failures, timeouts, or quota limits, never blocking the global pipeline.
    
    - **Automatic fallback & self-healing**: in case of issues, extractors refresh their reference state autonomously.
    
    - **Non-blocking runs & error journaling**: every failed chunk/category is logged and gracefully skipped, process continues without crashing.


 - **4. Traceability, Auditability & Governance**

    - **Detailed business & technical logs**: every run, every attempt, every correction is traced with context (ID, timestamp, results…).
    
    - **Systematic snapshot historization**: all Parquet/JSONL files are timestamped, making it easy to track, audit, and analyze for regression or troubleshooting.
    
    - **Auditability “by design”**: ability to replay, restore state at any date, or check extraction completeness.
    

 - **5. Separation of Concerns & Design Patterns**

    - **Mother class/abstraction-oriented architecture**: each extractor inherits from `BaseExtractor` (logging, API handling, save/load metadata), ensuring code factorization and consistency.
    - **Single Responsibility Principle**: each extractor handles a single endpoint, enabling maintainability, testing, and fast extension to new data streams.
    - **Total modularity**: adding a new endpoint = new class, no refactor needed for other components.

---

**In summary:**

> The extraction layer of the CryptoLakehouse pipeline is not just functional: it **embodies the best of modern data engineering**, with real “business-ready” value, exemplary governance, and the ability to evolve and remain robust in a fast-moving and volatile crypto environment.

----

## 7. Transformation Layer

### 7.1 Spark Architecture & OOP - Overview

The transformation phase is the **analytical core of the CryptoLakehouse pipeline**. Its purpose is to convert raw data from the Data Lake (the “bronze” layer) into enriched, structured, business-ready data (the “silver” layer), while ensuring quality, traceability, and large-scale performance.

---
#### **Objectives of the Transformation Phase**

Transformation is a key stage that involves:

- **Cleaning data**: removing duplicates, filtering anomalies, correcting formats and types.
- **Enriching and structuring**: joining datasets, calculating business indicators, normalization, aggregations.
- **Calculating KPIs and advanced indicators**: volatility, returns, trends, dominance, etc.
- **Preparing data for BI, reporting, and machine learning**: optimized formats (Parquet/Delta), strict typing, datasets usable by Spark, Metabase, PowerBI, or other analytics tools.

This stage ensures that the data produced is directly **consumable by end users** (analysts, data scientists, visualization tools) while maintaining historization, traceability, and data quality.

---
#### **Why Spark?**

**Apache Spark** was chosen for transformation for several reasons:

- **Big Data scalability and performance**: Spark enables processing of millions of rows in memory, in parallel, across a cluster, without sacrificing speed even on large data volumes.
- **Native compatibility with Parquet and Delta Lake**: Spark natively reads/writes these formats, optimizing I/O performance and facilitating industrialization.
- **Rich ecosystem**: Spark provides libraries for structured data manipulation (`DataFrame`), advanced ELT, machine learning, error management, and persistence.
- **Interoperability and orchestration**: Spark integrates seamlessly with Airflow (job scheduling), Docker (containerization), and allows reproducible execution on any infrastructure.
- **Ease of use for data engineering**: its Python API (PySpark) makes writing complex transformations readable and maintainable.
- **Advanced debugging and optimization**: Spark offers **powerful observability interfaces** (Spark UI, Spark History Server), enabling **debugging, optimization, and troubleshooting** for both real-time and historical jobs. This makes it easy to **track issues, analyze performance, resume jobs, and continuously improve the pipeline**.

---
#### **Single SparkContext & Frequency-based SparkSessions**

To maximize efficiency, robustness, and code readability:

- **A single SparkContext is created for the entire pipeline**: it centralizes Spark resource management and acts as the global coordinator.
- **Dedicated SparkSessions are instantiated for each processing frequency group** (for example: `daily`, `weekly`, `5x`, `10x`), rather than for each business domain.
    - **Each frequency group** may include one or more domains, depending on the criticality or data dynamics.
    - **This design is based on business importance and data volatility**:
        - **Fact tables**, which are highly dynamic and strategic for analysis or machine learning (market, volume, price, etc.), are refreshed frequently (e.g., `10x` or `5x` sessions).
        - **Static or enrichment tables** (mappings, reference data, metadata), which change less often, are processed in less frequent sessions (`weekly`, `daily`, etc.).

This organization allows:

- **Logical isolation of processing by frequency**, making monitoring, resource optimization, and business prioritization easier.
- **Adapting refresh frequency to data volatility and criticality**: maximizing freshness for analytics, reducing processing cost for stable data.
- **Advanced Spark config customization** for each type of run: memory tuning, parallelism, Delta Lake options…

---
#### **Smart Reading & Pipeline Resilience**

Thanks to advanced orchestration:

- **Each Spark transformation step reads only the latest files or batches generated** for each table, relying on metadata and timestamped naming conventions.
    - This means **only new snapshots or recent file groups** are processed at each run, systematically avoiding re-reading or reprocessing of already processed data.
    - The pipeline is thus **smart, resource-efficient, and optimized**: it never does the same work twice, and processing costs are controlled.
- This design also makes the pipeline **very robust and resilient**:
    - In case of incident, restart, or partial batch, Spark can simply resume from the last valid snapshot, without loss of history or integrity.
    - File traceability, via logs and metadata files, ensures the **ability to replay, audit, or correct any step**.
- **In case of omission, error, or incident on a transformation step**:
    - **All anomalies are systematically tracked** in metadata files dedicated to each table (per DataFrame) and in transformation logs.
    - **Human intervention** (data engineer) can then be requested to analyze, complete, or replay the affected transformation, safely.
    - **The pipeline remains operational**: it does not crash, does not block the chain, and continues to process other tasks or tables. Production and analysis are therefore never blocked.

---
#### **Integration with Airflow & Global Orchestration**

The **transformation phase** is fully orchestrated by **Apache Airflow**, ensuring:

- **Automated runs** at defined frequencies (daily, weekly, 10x, etc.)
- **Dependency management**: transformation only starts when required extractions are complete and valid.
- **Supervision, monitoring, and alerting**: Airflow centralizes logs, raises errors, and allows DAG (pipeline) tracking, offering perfect traceability for each step.
- **Interoperability and portability**: the pipeline can be triggered both locally and in production, thanks to Docker containerization and Airflow configurations.

---
#### **UML Diagram - OOP Architecture of Transformers**

The diagram below illustrates the object-oriented architecture:  
the parent class `BaseTransformer` centralizes all shared logic (logs, Spark, metadata, utility methods),  
while each business subclass (Crypto, Exchange, GlobalMetrics, etc.) implements domain-specific transformations.

![UML_POO.png](docs/diagrams/uml/UML_POO.png)

*Each transformer inherits shared tools, ensuring modularity, robustness, and fast pipeline extension, with clear business separation.*

**In summary**:  
>The transformation phase relies on Spark for **performance and scalability**, OOP structuring for **maintainability and business scalability**, and Airflow orchestration for **automation, robustness, and production-ready governance**.  
>
>This foundation enables CryptoLakehouse to deliver reliable, BI/ML-ready, and evolutive data as the crypto market evolves, while ensuring **resilience, efficiency, and auditability**.

---

### 7.2 OOP Architecture - Code Structure

The transformation layer architecture is **fully object-oriented** and follows advanced software engineering principles (SOLID) to guarantee **modularity, clarity, maintainability, and scalability**.

```shell
transform/
├── base/
│   └── base_transformer.py      # Parent class, shared logging, Spark, metadata, shared logic
├── transformers/
│   ├── crypto_transformer.py    # Class for the Crypto domain
│   ├── exchange_transformer.py  # Class for the Exchange domain
│   └── ...                      # 1 file = 1 business domain = 1 class
├── orchestrators/
│   ├── transform_pipeline_runner.py  # Airflow entry point
│   └── transform_orchestrator.py     # Global orchestrator   
├── runners/
│   ├── run_crypto_only.py       # Local/test run scripts
│   └── ...
```

**Explanations:**

- `base/`: contains the common foundations and shared utilities (including the `BaseTransformer` class).
- `transformers/`: contains all business transformers; **1 domain = 1 class = 1 file** (e.g., `CryptoTransformer`, `ExchangeTransformer`, etc.).
- `orchestrators/`: orchestrates the execution of all or a subset of transformations, typically managed by Airflow or scripts.
- `runners/`: scripts for running a local or partial transformation, useful for tests or manual debugging.

#### **Role of the Parent Class `BaseTransformer`**

The `BaseTransformer` class is the **common foundation** for all transformers; it goes beyond simple technical factorization:

- **SparkSession injection** (SparkSession is never created inside the transformers themselves, ensuring isolation and global control).
    
- **Shared features**:
    - structured logging,
    - centralized metadata management (snapshot, tracking, audit),
    - robust write methods for Delta/Parquet formats,
    - automatic optimization (`OPTIMIZE`, `VACUUM`).
- **Advanced utility methods**, including:
    
    - **`_run_build_step`**: executes the full logic of a transformation step for a given table, from preparation to writing and metadata update, including detailed logging.
    - **`should_transform`**: checks, via metadata, if the current snapshot has already been transformed, to avoid redundant processing and optimize runs.

- **Business flexibility**: the parent class **does not require a `run()` method implementation**, but provides all the tools to orchestrate each transformer (child class) according to business needs.

**Benefits:**
- Code factorization,
- Robustness and testability,
- Fast extension (adding a new domain = new inherited class),
- Smart and secure execution (no duplicate processing, centralized logs, complete auditability).

#### **“One Business Domain = One Class” Principle**

- **Each functional domain of the Data Warehouse** (Crypto, Exchange, Sentiment, Global Market, etc.) has its own transformer class, isolated in a dedicated file.
- **Each class**:
    - reads the snapshots for its respective domain,
    - **each DataFrame (table) managed in the class has its own metadata** (transformation tracking, status, history, etc.),
    - applies the business logic (cleaning, enrichment, joins, KPI calculation, etc.),
    - writes the final table (dim/fact) in Delta format,
    - manages logs and metadata via the parent class.
- **This structure simplifies onboarding, maintenance, and versioning**:
    - Modifying/extending one domain never impacts the others,
    - Each component can be tested, deployed, and monitored independently.

#### **Applying SOLID Principles**

- **SRP (Single Responsibility Principle)**: each transformer (child class) manages only a specific business domain, with a single clear responsibility.
- **OCP (Open/Closed Principle)**: the system is open to extension (adding a new domain/transformer) without needing to modify existing code.
- **DI (Dependency Injection)**: the SparkSession (and possibly other dependencies) is injected at runtime, never hardcoded in business classes, ensuring control and isolation.
- **DRY (Don’t Repeat Yourself)**: all shared logic (logs, audit, saving, optimization, etc.) is factored into the base class.

#### **Added Value of This Structure**

- **Readability and modularity**: the file structure reflects the business logic.
- **Scalability**: adding a new domain = one file, one class, zero refactoring elsewhere. Evolving an existing domain: simply **add or adapt a method** in its class, never impacting the rest of the pipeline.
- **Easier maintenance**: each transformer/test can be isolated, debugged, and monitored.
- **Production ready**: the orchestrator allows for global or partial pipeline runs, natural integration with Airflow, or manual execution.
- **Robustness and auditability**: logs, metadata, and history are always managed, and the shared tools in the parent class guarantee non-duplication, safety, and smart processing.

**In summary:**

> The OOP architecture adopted for the transformation layer enables **rapid evolution, robust governance, and optimized performance**, all while maintaining a clear structure tailored to the demands of a large-scale data project.

---

### 7.3 Execution Logic of the Spark & OOP Pipeline

The execution of the transformation pipeline relies on a **central orchestrator** (`TransformationPipelineRunner`) that manages the sequence of Spark processing tasks for each domain, according to the chosen frequency or mode (daily, weekly, 5x, 10x).

- **A single SparkSession instance is created per run**, adapted to the processing frequency (daily, weekly, etc.).
- **This SparkSession is injected into all business transformers via the orchestrator**.
- The orchestrator handles reading the latest snapshots, controls which transformations to perform (via metadata), orchestrates calls to business logic methods, ensures writing to Delta Lake, and updates logs/metadata.
#### **Sequential Diagram of a Transformation Execution**

The diagram below summarizes the flow of a transformation step:  
from detecting the snapshot to be transformed to writing to the silver layer, with management of logs and metadata.

![schema_execution.png](docs/diagrams/execution/schema_execution.png)

Each step is tracked, optimized, and controlled to prevent any reprocessing or omissions, while ensuring auditability and recovery in case of incidents.

#### **Added Value**

> This architecture guarantees a **structured, modular, and robust execution**, where adding or modifying a transformer never impacts the rest of the pipeline.  
> The single injection of SparkSession, dynamic task sequencing, and centralization of logs/metadata ensure **performance, maintainability, and traceability** at project scale.

---

### 7.4 Detailed Example of a Business Transformer

To concretely illustrate the architecture, here is a simplified excerpt from the `CryptoTransformer` class, dedicated to the crypto-assets domain.

Each business transformer isolates the logic specific to its domain:  
- it prepares DataFrames from the latest Data Lake snapshots (bronze),
- applies cleaning, enrichment, strict typing,
- and centralizes writing, traceability, and metadata updates via the inherited `_run_build_step` method.

Example:

```python
def build_fact_crypto_market(self, last_batch: List[str]):
    """
    Builds the fact_crypto_market table from the latest batch of market data.
    - Reads and cleans market snapshots,
    - Calculates business indicators (price, volume, etc.),
    - Logs each step and writes the table in Delta format,
    - Updates the associated metadata.
    """
    batch_paths = self.find_latest_batch("crypto_listings_latest_data")
    if not self.should_transform("fact_crypto_market", batch_paths[0], force=False, daily_comparison=False):

        self.log("fact_crypto_market is up to date. Skipping transformation.", table_name="fact_crypto_market")
        return
        
    return self._run_build_step(
        table_name="fact_crypto_market",
        prepare_func=lambda: self.prepare_fact_crypto_market_df(last_batch),
        relative_path="fact_crypto_market",
        mode="overwrite"
    )

def prepare_fact_crypto_market_df(self, last_batch: List[str]) -> Optional[Tuple[DataFrame, str]]:
    """
    Prepares the market DataFrame from the latest batch.
    - Reads Parquet files,
    - Filtering, cleaning, enrichment,
    - Calculates KPIs (volatility, average price…),
    - Logs the process,
    - Returns the cleaned DataFrame and the source path for traceability.
    """
    # 1. Read raw data
    # 2. Apply strict schema, clean fields, remove outliers
    # 3. Compute business indicators
    # 4. Log the operation, return (df, source_path)
    ...
    return df_cleaned, batch_path
```
This pattern is found in all business transformers:  
the business build logic is separated from the preparation/enrichment steps.

**Workflow factorization (writing, metadata, logs) is handled by the parent class.**  
**This ensures homogeneity, robustness, and rapid extension to any new domain.**

----

### 7.5 **Optimizations, Robustness & Scalability**

| Aspect                     | Technical Detail                                                                 | Key Benefits                                                 |
|----------------------------|----------------------------------------------------------------------------------|--------------------------------------------------------------|
| **Optimization**           | Clean management of SparkSessions (one per frequency), minimized I/O (parquet, Delta, smart batching) | Performance, controlled resource usage, lower costs, clarity in Spark UI for maintenance |
| **Robustness**             | Detailed logs (business & technical), Spark UI/History monitoring, systematic `.stop()` handling, errors captured and isolated, fail-soft | Traceability, easy recovery, never-blocking pipeline         |
| **Scalability**            | Adding modules or classes without refactoring, processing massive volumes        | Fast extension, adapts to growth                             |
| **Testability / readability** | Modular code, strict OOP, independently testable classes, clear structure, structured documentation | Easier onboarding, straightforward maintenance, software quality |

---

### 7.6 **Highlights & Best Practices**

| Practice / Pattern                   | Impact / Benefit                                                                         |
|--------------------------------------|------------------------------------------------------------------------------------------|
| Adding a domain = new class          | Evolutionary model, zero technical debt, rapid business extension                        |
| No massive refactor                  | Robustness, stability, easy maintenance                                                  |
| “Frequency-based” orchestration      | Granular control, adapts to business needs, optimized performance                        |
| Business-friendly logs               | Accessible business monitoring, easy tracking, full auditability                         |
| Output data “BI/ML ready”            | Datasets ready for BI, reporting, and machine learning                                   |
| Airflow, Delta, DWH compatible       | Easy integration into enterprise workflows, “out of the box” industrialization           |


**Business Summary:**

> The OOP Spark architecture adopted here maximizes **maintainability, performance, and scalability** of the crypto pipeline, while ensuring robustness, auditability, and adaptability to all business needs (from raw data to advanced analytics).

---

## 8. Load Layer

### 8.1 Architecture & Overview - Load Layer

The **Load** phase is the final step that transfers “silver” data (transformed, enriched, analysis-ready) from the Data Lake to the **decision-making Data Warehouse** (PostgreSQL).  
Its aim is to make the data **exploitable, historized, and accessible** for BI and reporting, while ensuring robustness, integrity, and performance.

---
#### **Objectives of the Load Phase**

- **Integrate “silver” datasets into the Data Warehouse** for BI usage
- **Guarantee the quality and integrity** of loaded data (PK/FK checks, logs, auditability)
- **Enable historization and traceability** of load operations
- **Optimize performance**: batched loads, partitioning, monitoring
- **Industrialize and automate** the process via Airflow and a modular architecture

---

#### **Why Spark for the Load Phase?**

Even though the initial pipeline development was done locally, using Spark for loading into the Data Warehouse was chosen with the cloud and future scaling in mind:

- **Cloud-native scalability and performance**: Spark on a cluster can load massive volumes in parallel and perform read/write operations to PostgreSQL (or any cloud DWH) much faster than locally.
    
- **Accelerated validation and integrity management**: Key checks (PK/FK), filtering, and data processing leverage the cluster’s power instead of relying on local RAM/CPU.
    
- **Industrialization and multi-environment compatibility**: The same code runs locally for development/testing, and at scale on a Spark cluster in cloud production, with no need for rewriting.
    
- **Interoperability with the DWH**: Spark provides native connectors for PostgreSQL (and other cloud DWHs), enabling efficient, robust bulk and batch operations as well as transactional management.
    

> _This choice ensures a “future proof” pipeline, ready for the cloud and industrial-scale data processing-where a local script would quickly reach its limits in terms of volume and performance._
---
#### **Why a dedicated Load architecture?**

The choice of an OOP architecture for the Load phase is essential in order to:

- Factorize the management of SparkSession, logs, metadata, and integrity validation (PK/FK)
- Isolate the loading logic specific to each domain (Crypto, Exchange, etc.), making maintenance, scalability, and extension to new business needs easier
- Enable precise monitoring, granular incident recovery, and detailed audit of each load operation

---

#### **OOP Architecture of the Load Layer**

Each business loader (ExchangeLoader, CryptoLoader, etc.) inherits from a parent class `BaseLoader`, which provides:

- **Centralized management of SparkSession, logs, metadata**
- **Advanced integrity constraint management** (PK/FK)
- **Reading silver data ready to be loaded** (`read_delta`)
- **State validation and filtering on foreign keys** (`should_load`, `secure_fk_load`, `read_from_dw`)
- **Writing to the DWH** (`write_to_dw`), centralizing load logic (`_load_fact_or_variant_table`, `_load_stable_dim_table`)


> _See diagram below_

![UML_load.png](docs/diagrams/uml/UML_load.png)

_Each business loader inherits the factorized logic from `BaseLoader`:  
readiness checks, integrity control, logs, and fine-grained metadata management.  
This model ensures maximum robustness, rapid extension to any new domain, and easier pipeline maintenance._

---

### 8.2 OOP Architecture - Code Structure

The **Load** layer architecture is fully object-oriented and relies on advanced software engineering principles (SOLID) to guarantee **modularity, clarity, maintainability, and scalability**.

```shell
load/
├── base/
│   └── base_loader.py             # Common parent class, logging, Spark management,
|                                    FK/PK control, metadata management, shared logic
├── loaders/
│   ├── crypto_loader.py           # Class dedicated to the Crypto domain
│   ├── exchange_loader.py         # Class dedicated to the Exchange domain
│   └── ...                        # 1 file = 1 business domain = 1 class
├── orchestrators/
│   │   load_pipeline_runner.py    # Airflow entry point
│   └── load_orchestrator.py       # Global orchestrator (LoadPipelineRunner)
└── runners/
    ├── run_crypto_load.py         # Local execution scripts
    └── ...
```

**Explanation:**

- **`base/`**: contains common foundations and shared utilities (including the `BaseLoader` class):
    - SparkSession management,
    - Structured logging,
    - Centralized metadata management (read/write),
    - Integrity checks (FK/PK validation),
    - Utility methods (`read_delta`, `read_from_dw`, `write_to_dw`, `should_load`, `secure_fk_load`, etc.).
- **`loaders/`**: groups all business loaders; **1 domain = 1 class = 1 file** (e.g., `CryptoLoader`, `ExchangeLoader`…).
    - Each loader implements domain-specific methods (fact table loading, dimensions, link tables…) while inheriting all the factorized logic.
- **`orchestrators/`**: orchestrates the execution of all or a subset of loads, typically managed by Airflow.
- **`runners/`**: scripts to run a local or partial load, useful for testing or manual debugging.

#### **Role of the `BaseLoader` Parent Class**

The `BaseLoader` class is the **common foundation** for all business loaders;  
it goes beyond simple technical factorization:

- **SparkSession injection** (never directly created inside loaders).
- **Shared features**:
    - Structured logging,
    - Centralized and fine-grained metadata management (reading transformation metadata, load tracking, audit),
    - Robust methods for reading (`read_delta`), FK/PK validation (`secure_fk_load`, `read_from_dw`), and writing (`write_to_dw`).
- **Advanced utility methods**, including:
    - **`should_load`**: checks via metadata whether the data is ready to be loaded or already loaded,
    - **`_load_fact_or_variant_table()`**: centralizes append-only loading logic (tables with a temporal primary key),
    - **`_load_stable_dim_table()`**: only loads new records to a stable dimension (with composite or simple PK detection, and optional FK validation).
- **Business flexibility**: the parent class **does not require a `run()` method implementation**, but provides all the tools needed to orchestrate each loader as required by business needs.

---

**Benefits:**
- **Code factorization** (no more redundancy),
- **Robustness and testability** (each loader/test is isolated, integrated monitoring),
- **Rapid extension** (adding a domain = new inherited class),
- **Intelligent and secure execution** (FK filtering, logs, traceability, zero double-loading, full auditability).

---

#### **“1 Business Domain = 1 Class” Principle**

- **Each functional DWH domain** (Crypto, Exchange, Sentiment, Global Market…) has its own loader class, isolated in a dedicated file.
- **Each class:**
    - reads the silver datasets to load,
    - **each DataFrame (table) managed within the class has its own metadata** (transformation tracking, statuses, histories, etc.),
    - checks readiness (transformation metadata),
    - validates PK/FK by reading the existing DWH,
    - applies domain-specific business logic (append loading, integrity validation, etc.),
    - writes to the Data Warehouse,
    - manages logs and metadata via the parent class.
- **This structure makes onboarding, maintenance, and versioning easier:**
    - Modifying/extending a domain never impacts the others,
    - Each component can be tested, deployed, and monitored independently.

---

#### **Application of SOLID Principles**

- **SRP (Single Responsibility Principle)**: each loader only manages one specific business domain, a single clear responsibility.
- **OCP (Open/Closed Principle)**: the system is open to extension (adding a domain/loader) without having to modify existing code.
- **DI (Dependency Injection)**: SparkSession (and possibly other dependencies) are injected at runtime, never hardcoded inside business classes, guaranteeing control and isolation.
- **DRY (Don’t Repeat Yourself)**: all shared logic (logs, audit, controls, FK/PK handling, writing, etc.) is factorized in the base class.

---

#### **Added Value of This Structure**

- **Readability and modularity**: the folder structure reflects the business logic.
- **Scalability**: adding a new domain = one file, one class, zero refactor for the rest. **Evolving a domain = add or adapt a method**, without impacting the global pipeline.
- **Easier maintenance**: each loader/test can be isolated, debugged, and monitored.
- **Production ready**: the orchestrator allows piloting all or part of the pipeline (full/partial), natural integration with Airflow or manual execution.
- **Robustness and auditability**: logs, metadata, and history are always managed; the parent class’s shared tools guarantee no duplicate loads, security, and business compliance.

---

**In summary:**

> The OOP architecture adopted for the Load layer ensures **pipeline industrialization, rapid scalability, robust governance, and maximum business traceability**,  
> while remaining clear, modular, and perfectly suited for the requirements of an enterprise data project.

---

### 8.3 Execution Logic of the Load Pipeline & OOP

The execution of the load pipeline relies on a **central orchestrator** (`LoadPipelineRunner`) that manages the sequence of Spark loaders for each business domain, according to the defined logic and frequency (daily, weekly, 5x, 10x, etc.).

- **A single SparkSession is injected into all business loaders**, ensuring isolation, performance, and global control of the pipeline.

- The orchestrator manages reading the latest “silver” data, checks readiness via metadata, orchestrates integrity checks (PK/FK), triggers the business loading logic, ensures writing to the Data Warehouse, and updates logs/metadata.

---

#### **Sequential Diagram of a Stable Dimension Load**

The diagram below summarizes the flow of a load step:  
from checking prerequisites to writing into the Data Warehouse,  
including deduplication, key validation, and management of logs and metadata.

![schema_execution_load.png](docs/diagrams/execution/schema_execution_load.png)

_Each step is traced, optimized, and controlled to prevent any double-loading or omissions,  
while ensuring auditability, recovery, and robustness throughout the value chain._

---

> **Note:**  
> By default, loaders **read the most recent version of each Delta table** (thanks to Spark-Delta), ensuring the highest data freshness.
> 
> In case of incidents, the pipeline is **designed to explicitly target a previous version of the Delta table**: history navigation is native, allowing for **granular rollback, full audit**, or incident recovery.
> 
> **Manual intervention** may be required to correct or replay a problematic load,  
> but the pipeline remains **resilient and “fail-soft”**: it continues to load other tables, never blocking the entire analytics chain.

---

#### **Added Value**

> This architecture guarantees a **structured, modular, and robust execution**, where each business loader can be tested, monitored, replayed, or adapted independently of the rest of the pipeline.  
> The unique SparkSession injection, dynamic task sequencing, centralization of logs/metadata, and business integrity checks ensure **performance, reliability, and traceability** at project scale.

---

### 8.4 **Business Pattern: Loading a Fact Table**

The Load layer applies a factorized pattern for all fact or variant tables:  
**systematic readiness checks**, “append-only” loading mode (temporal primary key), native Delta history management, and full traceability.

#### **Business Example: `load_fact_global_market`**

Each business loader has a dedicated public method for each fact table, orchestrating the entire process:

```python
def load_fact_global_market(self, version: int = None) -> None:
    """
    Loads the fact_global_market table from the Delta silver layer into the data warehouse.

    This method assumes that the specified Delta version corresponds to a fully transformed
    snapshot that has not yet been loaded into the warehouse.

    Important:
    The version being loaded must not contain snapshot_timestamp values
    already present in the data warehouse, due to a primary key constraint.
    """
    table = "fact_global_market"
    self.log_section(title=f"Loading Started for {table}")

    if not self.should_load(table):
        self.log_section(title=f"Loading Skipped for {table}")
        return

    status = self._load_fact_or_variant_table(
        table_name=table,
        fk_presence=False,
        version=version,
        mode="append",
        notes="Load from transformed global metrics delta table",
    )

    if status:
        self.log(f"Loaded operation for {table} succeeded", table_name=table)
    else:
        self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

    self.log_section(title=f"Loading Ended for {table}")
```

> **This method orchestrates the business workflow**:  
  structured logs, idempotence check via `should_load`, call to the factorized logic `_load_fact_or_variant_table`, comprehensive reporting and audit.

#### **Focus on Factorized Methods**

The core robustness of the pipeline is built on these shared methods:

- **`should_load`**: ensures that loading only occurs if a new, valid snapshot is available (idempotence).
- **`_load_fact_or_variant_table`**: applies all “append-only” logic, Delta version management, optional FK validation, writing to the DWH, and logging.

**Code snapshot:**

![code_snap_should_load.png](docs/code_snaps/code_snap_should_load.png)

![code_snap_load_fact.png](docs/code_snaps/code_snap_load_fact.png)

> **These methods ensure:**
> 
> - **Idempotence** (never double-load, never any data loss)
> - **Full auditability** (logs, metadata)
> - **Native support for Delta versioning** (history navigation, rollback, debug)
> - **Scalability** (factorized pattern for all fact or variant tables)


**Summary:**

> Integrating these patterns into all business loaders guarantees performance, robustness, and scalability,  
> making every load flow fully traceable, testable, and production-ready.

---

### 8.5 **Optimizations, Robustness & Scalability**

| Aspect                  | Technical Detail                                                                                                                                                        | Key Benefits                                            |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
| **Optimization**        | “Live” Delta reads + historical navigation, batched loads, optimized PK/FK validation, controlled I/O (bulk write), parameterizable Spark/Postgres mode                | High performance, controlled I/O, reduced costs         |
| **Robustness**          | Detailed logs, fine-grained Airflow and business log tracking, `.stop()` handling, isolated error catching, idempotence (`should_load`), Delta versioning, easy rollback | Never-blocking pipeline, traceability, easy recovery    |
| **Scalability**         | Add domains/loaders without refactoring, multi-table, multi-batch, cloud/Spark cluster ready, “full” or “partial” mode                                                  | Fast extension, adapts to growth                        |
| **Auditability/quality**| Centralized metadata (per table), transformation-to-load mapping, rollback/versioning support, detailed business logs                                                   | Quality control, process validation, full audit         |

---

### 8.6 **Highlights & Best Practices**

| Practice / Pattern                     | Impact / Advantage                                                                  |
|----------------------------------------|-------------------------------------------------------------------------------------|
| “should_load” pattern                  | Zero duplication, safety, total idempotence                                         |
| `_load_fact_or_variant_table` pattern  | Robustness, factorization, scalability, reusable for all facts/variants             |
| Integrated Airflow orchestration       | Monitoring, centralized logs, retry, fine dependency management                     |
| Out-of-the-box Delta version support   | Rollback, debug, historical navigation, resilience, easy human intervention         |
| Adding a domain = 1 file/class         | Evolutionary model, rapid business extension, no technical debt                     |
| Automated PK/FK control                | Quality, auditability, referential integrity                                        |
| Business-friendly logs                 | Process tracking, full audit, accessible monitoring                                 |
| “DWH-ready” output data                | Data ready for BI, reporting, and data science                                      |

**Business summary:**

> The modular, factorized Load architecture enables rapid scaling, total traceability, and “production-ready” business governance.  
> The business patterns (should_load, append only, Delta navigation…) ensure the robustness, scalability, and resilience of the crypto pipeline, from raw data to decision support.

---

## 9. Data Warehouse, SQL & Migrations - Structure & Roles

The CryptoLakehouse project is organized to ensure a strict separation between:

- the ELT code (Python/Spark),
- the Data Warehouse logic (SQL, governance, BI),
- the automation of schema management and migrations.

Here’s how the technical layers are structured on the DWH side:

---

### **warehouse/** -  _Data Warehouse SQL & BI Layer_

- **Mission**:  
    Centralizes all SQL logic, administration scripts, quality governance, business analytics views, and monitoring for the PostgreSQL DWH.
- **Key Content**:
    - `admin/`: automated initialization scripts, security, role and permission management (.env driven)
    - `scripts/`:
        - DCL/: role/schema creation
        - procedures/: quality, data cleaning, automated enrichment
        - tests/: manual quality test suites (by domain)
        - views/: anomaly views, business (KPI), monitoring (freshness, latency, NULLs)
- **Why?**  
    To isolate business and BI logic from Python code, industrialize data quality, and facilitate SQL monitoring and collaboration.
- [See detailed README](warehouse/README.md)

---

### **src/db/** - _DWH Schema Management & Utilities (SQLAlchemy + backup)_

- **Mission**:  
    All Python logic for **versioned** and **automated** PostgreSQL schema management (ORM SQLAlchemy, Alembic migrations, backup scripts).
- **Key Content**:
    - `models.py`: DWH schema declaration in SQLAlchemy (single source of truth for Alembic)
    - `backup_postgres.py`: complete backup/restore utility, scriptable via Airflow
- **Why?**  
    To ensure versioning, integrity control, and automation of structural changes-without manually editing SQL.  
    _All modifications go through here before hitting production._
- [See detailed README](src/db/README.md)

---

### **alembic/** - _Versioned Schema Migration Management_

- **Mission**:  
    Centralizes the full history of PostgreSQL DWH schema changes (via Alembic): migration scripts, upgrades/downgrades, collaborative versioning.
- **Key Content**:
    - `env.py`: Alembic configuration (links .env + SQLAlchemy)
    - `versions/`: all migration scripts, auto-generated or custom
    - Full migration history and basic commands (upgrade, downgrade, autogenerate…)

- **Why?**  
    To ensure traceability, reproducibility, collaboration, and easy rollback across the DWH infrastructure.
    
- [See detailed README](alembic/README.md)

---

> **In summary:**
>
> - Each layer (SQL, ORM, migration, tests, BI, monitoring) is isolated in its own dedicated folder, documented, and maintainable.
> - The local documentation in each folder is **sufficient and up-to-date**: just link to it from the main doc.
>
> - This organization **guarantees professional governance, easier onboarding, and full end-to-end industrialization**.

---

## 10. Orchestration & Automation with Airflow

### 10.1 **Orchestration Architecture - Airflow Global Overview**

The **Airflow** platform orchestrates **the entire ELT pipeline**, from automated data extraction to post-load quality checks. This orchestration ensures:

- **Full automation** of the data lifecycle,
- **Modularity and intelligent orchestration**:  
  the pipeline is modular by business domain (each component handles a specific scope),  
  and Airflow orchestrates execution in “frequency-based blocks” using runner scripts (`transform_pipeline_runner.py`, `load_pipeline_runner.py`), triggering all required modules for each run based on the frequency.
- **Monitoring**, **traceability**, and **incident recovery**.

#### **Global Airflow Orchestration Diagram**

The architecture of the main pipeline DAGs is illustrated below:

![airflow_pipline.png](docs/diagrams/airflow/airflow_pipline.png)

#### **Diagram Explanation**

1. **EXTRACT TaskGroup - PythonOperator**
    
    - **Role:** launches data extraction (API) by calling the `run()` method of each business extractor (one per endpoint, e.g., `/v1/cryptocurrency/listings/latest`).
        
    - **Task serialization:** each extraction is encapsulated in a _TaskGroup_, with tasks sequenced to respect quotas/limitations of public APIs.
        
    - **Frequency-based automation:** DAGs trigger extractions according to the defined frequency (daily, weekly, 5x, 10x, etc.).
        
2. **TRANSFORM Task - BashOperator (Spark)**
    
    - **Role:** triggers the Spark job via `spark-submit` to execute the transformation phase in distributed batch mode.
        
    - **Cloud-ready automation:** the task runs the runner script (`transform_pipeline_runner.py`), passing the business frequency as an argument (e.g., daily, weekly, 5x, 10x, etc.). This frequency targets only relevant transformations.
        
    - **Delta Lake storage:** all transformed data is written to the _silver_ layer of the Data Lake, in Delta format, ready for the next phase.
        
    - **Logic:**  
        This “frequency-based” system provides flexible and efficient orchestration:
        
        - A single Python script (`transform_pipeline_runner.py`) manages all frequencies, creating a dedicated SparkSession per run (with explicit naming for monitoring).
            
        - The frequency argument dynamically orchestrates the required processing (`run_daily_tasks`, `run_weekly_tasks`, etc.), factoring business logic for maximum robustness.
            
        - This pattern favors scalability, observability (Spark UI), and pipeline industrialization, both locally and on cloud/distributed environments.
            
3. **LOAD Task - BashOperator (Spark + Postgres JARs)**
    
    - **Role:** triggers the Spark job for the “Load” phase via the runner script (`load_pipeline_runner.py`), passing the business frequency as an argument.
        
    - **Native connection to the Data Warehouse:** with Postgres drivers (`.jars`), Spark writes directly to the Data Warehouse (PostgreSQL) in batch mode, managing integrity constraints (PK/FK) and partitioning as needed.
        
    - **Delta reading and validation:** Spark reads the latest version of the Delta _silver_ tables (or a specific version for rollback), deduplicates, validates keys, and only inserts new compliant records.
        
    - **Logic:**
        
        - The Python script `load_pipeline_runner.py` centralizes all load logic for each frequency.
            
        - A dedicated SparkSession is created per run (named by frequency, e.g., `CryptoETL_Load_<frequency>`), making monitoring easier.
            
        - Based on the frequency (`daily`, `weekly`, `5x`, `10x`, `all`), the appropriate method is called (`run_daily_tasks()`, etc.), dynamically orchestrating the loads of all necessary tables.
            
        - The integration of Postgres drivers enables transactional, compliant, and efficient loading.
            
        - The process ensures idempotence (metadata, integrity), traceability, and the ability to rollback or selectively rerun in case of incident.
            
4. **POST-LOAD/SQL TaskGroup - SQLExecuteQueryOperator**
    
    - **Role:** after each load, runs SQL procedures to guarantee data quality, fill certain NULL fields, etc.
        
    - **Quality control:** these post-load tasks are grouped and managed independently, allowing modularity and targeted reruns if needed.
        
    - **Example:** `CALL fill_nulls_spot_volume_and_wallet_weight();`
---

#### **Key Features of Airflow Orchestration**

- **Frequency-based control:** each DAG manages all tables/domains for a given frequency (e.g., “DAG daily”, “DAG weekly”, etc.).
    
- **Modular execution:** possibility to replay an isolated step (extract only, transform only, etc.).
    
- **Integrated monitoring and alerting** (Airflow UI, logs, notifications).
    
- **Complementary DAGs:**
    
    - **Init DAG:** performs the initial full load of the Data Warehouse, extracting all data. This DAG is triggered manually from the Airflow UI; it initializes the DWH to allow for incremental frequency-based operation afterward.
        
    - **Backup DAG:** complete DWH (Postgres) backup with dedicated scheduling.
        
- **Robustness & recovery:** in case of incident, the modularity of tasks enables granular recovery and full traceability.
    

---

### 10.2 **Operational Summary**

> **Airflow orchestrates the entire ELT pipeline**, ensuring industrialization, governance, and scalability ready for production.
> 
> The combination of TaskGroups, specialized operators, and frequency/domain partitioning enables:
> 
> - enforcement of API constraints,
>     
> - maximization of Spark performance on large volumes,
>     
> - and guaranteed business quality and data traceability at every step.
>     

---

**Note:**  
Thanks to this design, adding a new domain, a new frequency, or a new quality check can be simply integrated by extending the script logic or adding a TaskGroup/task to the Airflow DAG, without ever breaking existing functionality.


**In summary:**  
> The Airflow architecture of this project enables robust, configurable, and extensible orchestration, naturally adapting to evolving business needs and production technical constraints.
>
> **This architecture and design are reproducible in any cloud or on-premises environment, thanks to containerization (Docker) and configuration management via Airflow.**

---

## 11. Docker
## 12. Results Highlights + Optimizations
## 13. Installation & Startup Guide


