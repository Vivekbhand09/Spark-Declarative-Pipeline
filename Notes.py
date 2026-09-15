# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Declarative Pipelines (SDP) — Complete Revision Notes
# MAGIC ### (with my own pipeline code explained line-by-line, interview-style)
# MAGIC
# MAGIC > Sources used: [Spark Declarative Pipelines Programming Guide (Apache Spark 4.2 official docs)](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html) + [Azure Databricks — Lakeflow / Declarative Pipelines docs](https://learn.microsoft.com/en-us/azure/databricks/) + my own practice code (`first_dag.py`, `second_dag.py`, `append_flow.py`, `auto_cdc.py`, `expectation.py`, `parameters.py`).
# MAGIC >
# MAGIC > This file is meant to be the **only thing I need to revise SDP** before an interview or before writing pipeline code again.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. What is Apache Spark?
# MAGIC
# MAGIC **Apache Spark** is an open-source, distributed data processing engine used for large-scale batch and streaming analytics, SQL, machine learning, and graph processing.
# MAGIC
# MAGIC Key facts:
# MAGIC - It processes data **in-memory** (much faster than old Hadoop MapReduce, which writes to disk between every step).
# MAGIC - It works on a **cluster** — a driver program splits work into tasks that run in parallel across many worker/executor nodes.
# MAGIC - It has a **unified engine**: the same engine powers SQL (Spark SQL), streaming (Structured Streaming), ML (MLlib), graphs (GraphX), and now declarative pipelines (SDP).
# MAGIC - Core abstraction: the **RDD** (Resilient Distributed Dataset) historically, but almost all modern code uses the higher-level **DataFrame/Dataset API** built on Spark SQL's Catalyst optimizer.
# MAGIC - Spark is **lazy** — transformations (`select`, `filter`, `join`, `groupBy`) just build a logical plan; nothing runs until an **action** (`show()`, `count()`, `write()`) is called.
# MAGIC
# MAGIC **Interview one-liner:** *"Apache Spark is a distributed, in-memory, lazy-evaluated compute engine that unifies batch, streaming, SQL, and ML workloads on a cluster."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. What is PySpark?
# MAGIC
# MAGIC **PySpark** is the **Python API for Apache Spark**. It lets you write Spark applications (DataFrame transformations, SQL, streaming jobs) in Python instead of Scala/Java, while the actual execution still happens on the JVM-based Spark engine (Python calls are translated via Py4J / Spark Connect into JVM operations, so you get Python's ease of use without losing Spark's distributed performance).
# MAGIC
# MAGIC Key pieces of PySpark:
# MAGIC - `pyspark.sql` → DataFrame API, `SparkSession`, functions (`col`, `when`, `to_date`, etc.)
# MAGIC - `pyspark.sql.streaming` → Structured Streaming (`readStream`, `writeStream`)
# MAGIC - `pyspark.ml` → MLlib
# MAGIC - **`pyspark.pipelines`** → the module that gives us **Spark Declarative Pipelines (SDP)** — this is the module all my pipeline files import as `dp`.
# MAGIC
# MAGIC **Interview one-liner:** *"PySpark is Spark's Python front-end — you write DataFrame/SQL/streaming code in Python, and it's executed by the same distributed Spark engine used by Scala/Java."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. What is SDP (Spark Declarative Pipelines)?
# MAGIC
# MAGIC **Spark Declarative Pipelines (SDP)** is a **declarative framework built into Apache Spark** (available from **Spark 4.1**, via `pyspark.pipelines`) for building reliable, maintainable, testable ETL/ELT pipelines — for both **batch and streaming** data.
# MAGIC
# MAGIC > Official definition (Apache Spark docs): *"SDP is a declarative framework for building reliable, maintainable, and testable data pipelines on Apache Spark. SDP simplifies ETL development by allowing you to focus on the transformations you want to apply to your data, rather than the mechanics of pipeline execution."*
# MAGIC
# MAGIC Instead of writing imperative code that says "read this → transform it this way → write it there → checkpoint it → handle failures," in SDP you simply **declare**:
# MAGIC - What tables/views should exist, and
# MAGIC - What data should be in them (a SQL query or a DataFrame-returning Python function)
# MAGIC
# MAGIC SDP figures out **everything else automatically**: the dependency graph between your tables, execution order, parallelism, whether a table needs batch or incremental (streaming) refresh, checkpointing, and error handling.
# MAGIC
# MAGIC **Origin:** SDP is the **open-sourced core of Databricks' Delta Live Tables (DLT)**. Databricks donated this engine to Apache Spark. On Databricks itself, this same technology (plus extra managed-only features) is now branded **Lakeflow Declarative Pipelines** (previously called DLT). So:
# MAGIC - **Apache Spark SDP** (`pyspark.pipelines`) = the free, open-source core — this is exactly the API I used in all six of my practice files.
# MAGIC - **Databricks Lakeflow Pipelines** = SDP + extra production features: **AUTO CDC**, a **queryable event log**, **continuous mode**, and (until a very recent change) **expectations** were Databricks-only. (Note: standalone materialized views/streaming tables in Databricks SQL now also support the `CONSTRAINT ... EXPECT` clause.)
# MAGIC
# MAGIC Common use cases (from the official docs):
# MAGIC - Data ingestion from cloud storage (S3, ADLS Gen2, GCS)
# MAGIC - Data ingestion from message buses (Kafka, Kinesis, Pub/Sub, Event Hubs)
# MAGIC - Incremental batch and streaming transformations
# MAGIC
# MAGIC **Interview one-liner:** *"SDP is Spark's built-in declarative ETL framework — you declare tables/views and their logic, and SDP auto-generates the dependency graph, decides batch vs incremental execution, and manages checkpointing/orchestration/error-handling for you."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Comparison with Traditional Spark Programming
# MAGIC
# MAGIC This is one of the most commonly asked interview questions: *"Why not just write plain PySpark jobs?"*
# MAGIC
# MAGIC Traditional (procedural) Spark programming and SDP (declarative) sit on two different paradigms:
# MAGIC
# MAGIC | Aspect | Traditional / Procedural Spark | SDP (Declarative) |
# MAGIC |---|---|---|
# MAGIC | **What you write** | Step-by-step imperative code: read → transform → write, explicitly | Only the **desired end state**: "this table should contain this query's result" |
# MAGIC | **Orchestration** | You manually chain jobs/notebooks (often with Airflow, cron, or Databricks Jobs) and manage the order yourself | SDP **automatically** builds a dependency graph (DAG) from your table/view references and runs things in the correct order, in parallel where possible |
# MAGIC | **Incremental logic** | You hand-write checkpoints, watermarks, `MERGE INTO` logic for CDC, etc. | Built-in: streaming tables auto-track checkpoints; `AUTO CDC` (Databricks) auto-handles upserts/deletes/out-of-order events |
# MAGIC | **Error handling** | Custom retry/rollback code | Automatic retries and atomic updates — failures roll back cleanly |
# MAGIC | **Data quality** | You write manual `filter()`/`assert` logic scattered through the code | Declarative **Expectations** (`@dp.expect...`) attached directly to the dataset definition |
# MAGIC | **Testing** | Run against real/mock data | `spark-pipelines dry-run` validates the whole graph (syntax, analysis, cyclic dependency checks) without touching any data |
# MAGIC | **Mental model** | "Steps to execute" | "Tables that should exist" |
# MAGIC | **Control** | Full, fine-grained control over execution | Less manual control — traded for simplicity and reliability |
# MAGIC
# MAGIC From the official Databricks "Procedural vs Declarative" comparison:
# MAGIC - Procedural = you specify **how** (explicit steps, loops, conditionals, fine-grained tuning). Good for: complex custom business logic, low-level performance tuning, legacy imperative scripts.
# MAGIC - Declarative = you specify **what** (desired result), and the system picks the best execution plan. Good for: SQL-style transformations, managed pipelines, scalable workloads that benefit from automatic optimization.
# MAGIC
# MAGIC **Interview one-liner:** *"Traditional Spark makes you own orchestration, checkpointing, and error handling yourself. SDP flips this — you declare the tables you want, and the framework owns orchestration, incremental state, and recovery."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Why SDP? (Motivation / Problem it Solves)
# MAGIC
# MAGIC Traditional Spark ETL pipelines (a chain of scripts/notebooks) run into these recurring problems, and SDP solves each one:
# MAGIC
# MAGIC | Problem with traditional pipelines | How SDP fixes it |
# MAGIC |---|---|
# MAGIC | You manually manage job ordering/DAGs (Airflow, cron, custom scripts) | SDP auto-derives the DAG purely from which tables/views reference each other |
# MAGIC | You manually write incremental logic (watermarks, checkpoints, merge logic) | Streaming tables handle incremental state and checkpointing automatically |
# MAGIC | Hand-rolled retry/rollback/error handling | Built-in atomic updates and automatic recovery |
# MAGIC | No built-in data quality gate — bad data silently flows downstream | Expectations (`expect` / `expect_or_drop` / `expect_or_fail`) validate every row and let you warn, drop, or fail |
# MAGIC | Testing end-to-end pipelines is hard | `spark-pipelines dry-run` validates the whole graph without reading/writing real data |
# MAGIC | Hard to know pipeline health/lineage | Dependency graph + (on Databricks) a queryable event log |
# MAGIC
# MAGIC **In short:** SDP lets engineers focus on **what** the data should look like (the business logic) instead of **how** to execute, sequence, and babysit the pipeline.
# MAGIC
# MAGIC **Interview one-liner:** *"SDP exists to remove the boilerplate of orchestration, incremental-state management, and data-quality enforcement from ETL — so engineers write business logic, not plumbing."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. Key Components: Flows
# MAGIC
# MAGIC A **Flow** is the **foundational unit of data processing** in SDP — it's the same concept whether you're doing batch or streaming.
# MAGIC
# MAGIC > Official definition: *"A flow reads data from a source, applies user-defined processing logic, and writes the result into a target dataset."*
# MAGIC
# MAGIC Example from the official docs:
# MAGIC ```sql
# MAGIC CREATE STREAMING TABLE target_table AS
# MAGIC SELECT * FROM STREAM source_table
# MAGIC ```
# MAGIC This single statement creates the table `target_table` **and** implicitly creates a **flow** that reads new rows from `source_table` and writes them into `target_table`.
# MAGIC
# MAGIC A flow can be:
# MAGIC - **Batch flow** — a full/complete recompute (used by materialized views)
# MAGIC - **Streaming flow** — an incremental read that only processes new data (used by streaming tables)
# MAGIC
# MAGIC ### 👉 Flows in my code
# MAGIC Every single `@dp.table` / `@dp.materialized_view` function I wrote **is** a flow definition:
# MAGIC ```python
# MAGIC @dp.table
# MAGIC def basic_st() -> DataFrame:
# MAGIC     return spark.readStream.table("samples.nyctaxi.trips")
# MAGIC ```
# MAGIC The function body (`read → transform → return`) **is** the flow — SDP takes the returned DataFrame's plan and wires it into a flow that writes into the target table.
# MAGIC
# MAGIC I also explicitly created **named flows** using `@dp.append_flow` (see `append_flow.py`) and `dp.create_auto_cdc_flow` (see `auto_cdc.py`) — both are just special types of flows that write into a target dataset.
# MAGIC
# MAGIC **Interview one-liner:** *"A flow = read + transform + write. Every dataset (table/view) in SDP is populated by one or more flows."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7. Key Components: Datasets
# MAGIC
# MAGIC A **Dataset** is a **queryable object produced by one or more flows**. Flows inside the pipeline can also read from datasets produced elsewhere in the same pipeline (this is how dependency chains form).
# MAGIC
# MAGIC There are **three kinds of datasets**:
# MAGIC
# MAGIC | Dataset type | Description | How it's refreshed |
# MAGIC |---|---|---|
# MAGIC | **Streaming Table** | A table + one or more **streaming flows** writing into it | **Incrementally** — processes only new/changed data as it arrives; each row is processed **exactly once** |
# MAGIC | **Materialized View** | A view that's pre-computed into a physical table; always has **exactly one batch flow** writing to it | Recomputed (fully or incrementally, SDP/Databricks chooses the cheaper option) whenever the pipeline runs |
# MAGIC | **Temporary View** | Scoped only to a single pipeline execution | Not persisted; used to encapsulate reusable/intermediate logic other flows depend on |
# MAGIC
# MAGIC **When to use which** (a very common interview question):
# MAGIC - Use a **materialized view** when your query does **joins/aggregations**, or the source can be **updated/deleted** (not just appended) — a materialized view is always kept *correct*, recomputing when dimensions change.
# MAGIC - Use a **streaming table** when the source **only grows** (append-only) and you want **high-throughput, low-latency, exactly-once** ingestion (e.g., Kafka topics, files landing in cloud storage). Streaming tables are the recommended choice for **ingestion**.
# MAGIC - Use a **temporary view** for logic you want to reuse across multiple downstream tables but that itself shouldn't be materialized as a physical table.
# MAGIC
# MAGIC ### 👉 Datasets in my code
# MAGIC - `first_dag.py` → `src_sales`, `enr_sales`, `cur_sales` are all **materialized views** (`@dp.materialized_view`) — batch, chained one after another.
# MAGIC - `second_dag.py` → `src_sales_stream`, `enr_sales_stream`, `cur_sales_stream` are all **streaming tables** (`@dp.table` + `spark.readStream`) — the exact same business logic as `first_dag.py`, but done incrementally.
# MAGIC - `auto_cdc.py` → `products_scd2` and `products_scd1` are **streaming tables** created explicitly via `dp.create_streaming_table(...)` (empty shells), then populated by an **AUTO CDC flow**.
# MAGIC - `products_source` (in `auto_cdc.py`) is a **temporary view** (`@dp.temporary_view`) — it exists only to feed the CDC flow and isn't materialized as a queryable table outside the pipeline.
# MAGIC
# MAGIC **Interview one-liner:** *"A dataset is what a flow writes into. Streaming tables = incremental, exactly-once, append-friendly. Materialized views = always-correct batch recompute, good for joins/aggregations. Temporary views = pipeline-scoped intermediate logic."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 8. Key Components: Pipelines
# MAGIC
# MAGIC A **Pipeline** is the **primary unit of development and execution** in SDP.
# MAGIC
# MAGIC > Official definition: *"A pipeline can contain one or more flows, streaming tables, and materialized views. While your pipeline runs, it analyzes the dependencies of your defined objects and orchestrates their order of execution and parallelization automatically."*
# MAGIC
# MAGIC A **Pipeline Project** is the set of source files (`.py` and/or `.sql`) that define a pipeline's datasets/flows, plus a **spec file** (conventionally `spark-pipeline.yml`):
# MAGIC
# MAGIC ```yaml
# MAGIC name: my_pipeline
# MAGIC libraries:
# MAGIC   - glob:
# MAGIC       include: transformations/**
# MAGIC storage: file:///absolute/path/to/storage/dir
# MAGIC catalog: my_catalog
# MAGIC database: my_db
# MAGIC configuration:
# MAGIC   spark.sql.shuffle.partitions: "1000"
# MAGIC ```
# MAGIC
# MAGIC Spec fields:
# MAGIC - **name** (required) — pipeline project name
# MAGIC - **libraries** (required) — paths/glob patterns to the `.py`/`.sql` transformation files (this is exactly why all my practice files live inside a folder called **`transformations`** — that's the SDP convention referenced by `glob: include: transformations/**`)
# MAGIC - **storage** (required) — directory to store streaming checkpoints
# MAGIC - **catalog / database** (optional) — default output location for tables (this is where `sdp_catalog.source.*` / `sdp_catalog.target.*` in my code comes from — the catalog is configured once at the pipeline level, and I reference fully qualified names, e.g. `sdp_catalog.source.sales`, `sdp_catalog.target.src_sales`)
# MAGIC - **configuration** (optional) — Spark config key-value pairs, also used to pass **pipeline parameters** into Python code (see Section 15)
# MAGIC
# MAGIC ### The `spark-pipelines` CLI
# MAGIC Built on top of `spark-submit`:
# MAGIC
# MAGIC | Command | Purpose |
# MAGIC |---|---|
# MAGIC | `spark-pipelines init --name my_pipeline` | Scaffold a new pipeline project |
# MAGIC | `spark-pipelines run` | Run the pipeline (default = incremental update) |
# MAGIC | `spark-pipelines run --full-refresh-all` | Full recompute of every dataset from scratch |
# MAGIC | `spark-pipelines run --full-refresh orders,customers` | Full recompute of specific datasets only |
# MAGIC | `spark-pipelines run --refresh orders` | Incremental update of specific datasets |
# MAGIC | `spark-pipelines dry-run` | Validate the pipeline (syntax/analysis/cyclic-dependency checks) **without reading or writing any data** |
# MAGIC
# MAGIC ```bash
# MAGIC spark-pipelines run
# MAGIC spark-pipelines run --spec /path/to/my-pipeline.yaml
# MAGIC spark-pipelines run --full-refresh-all
# MAGIC spark-pipelines run --conf spark.sql.shuffle.partitions=200 --driver-memory 4g
# MAGIC spark-pipelines run --remote sc://my-cluster:15002
# MAGIC ```
# MAGIC
# MAGIC Refresh rules:
# MAGIC - No refresh flag → default incremental update
# MAGIC - `--full-refresh-all` **cannot** be combined with `--full-refresh` or `--refresh`
# MAGIC - `--full-refresh` and `--refresh` **can** be combined (different behavior for different datasets in the same run)
# MAGIC
# MAGIC **Interview one-liner:** *"A pipeline is the deployable unit: a collection of dataset/flow definitions plus a spec file, executed and orchestrated together — I run mine with `spark-pipelines run`, and validate changes safely first with `spark-pipelines dry-run`."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 9. Process Batch Tables — `first_dag.py` explained
# MAGIC
# MAGIC **Concept:** A **batch table** in SDP = a **materialized view**. It's populated by exactly **one batch flow** — a plain `spark.read` (not `readStream`). Every time the pipeline runs, SDP recomputes the view (fully or incrementally under the hood, whichever is cheaper) so that it's always correct relative to its source.
# MAGIC
# MAGIC ### My code: `first_dag.py`
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC # Materialized View — reads a batch source table and adds a formatted date column
# MAGIC @dp.materialized_view(name="src_sales")
# MAGIC def src_sales():
# MAGIC     df = spark.read.table("sdp_catalog.source.sales")
# MAGIC     df = df.withColumn("sale_date", to_date(col("date"), "MM-dd-yyyy"))
# MAGIC     return df
# MAGIC
# MAGIC # Materialized View — chained on top of src_sales, applies a markup to revenue
# MAGIC @dp.materialized_view(name="enr_sales")
# MAGIC def enr_sales():
# MAGIC     df = spark.read.table("sdp_catalog.target.src_sales")
# MAGIC     df = df.withColumn("revenue", col("revenue") * 1.5)
# MAGIC     return df
# MAGIC
# MAGIC # Materialized View — chained on top of enr_sales, aggregates revenue by date
# MAGIC @dp.materialized_view(name="cur_sales")
# MAGIC def cur_sales():
# MAGIC     df = spark.read.table("sdp_catalog.target.enr_sales")
# MAGIC     df = df.groupBy("date").agg(sum("revenue").alias("total_sales"))
# MAGIC     return df
# MAGIC ```
# MAGIC
# MAGIC **What's actually happening — a classic Bronze → Silver → Gold (medallion) pattern:**
# MAGIC 1. **`src_sales`** (Bronze-ish): reads the raw source table `sdp_catalog.source.sales` with a plain batch `spark.read.table(...)`, and parses the string `date` column (format `MM-dd-yyyy`) into a proper `DATE` type using `to_date()`. This is the "landing"/cleanup step.
# MAGIC 2. **`enr_sales`** (Silver — "enriched"): reads `src_sales` (note it's read from `sdp_catalog.target.src_sales` — the **target** schema, because `src_sales` is now a table *produced by this pipeline*, sitting in the pipeline's output/target location) and applies a business transformation — multiplying `revenue` by `1.5`.
# MAGIC 3. **`cur_sales`** (Gold — "curated"): reads `enr_sales` and aggregates it — `groupBy("date")` + `sum("revenue")` — to produce a final reporting-ready table of total sales per day.
# MAGIC
# MAGIC **Key things to notice (and say in an interview):**
# MAGIC - Because every function is decorated with `@dp.materialized_view`, **SDP automatically figures out the dependency chain** `src_sales → enr_sales → cur_sales` just from the fact that `enr_sales`'s code reads the table named `src_sales`, and `cur_sales`'s code reads `enr_sales`. **I never had to write any orchestration code** — no Airflow DAG, no manual ordering.
# MAGIC - `spark.read.table(...)` (not `readStream`) is what makes these **batch** flows → hence these are **materialized views**, not streaming tables.
# MAGIC - This is exactly the "Querying Tables Defined in a Pipeline" pattern from the official docs — referencing another pipeline table is done the same way as referencing any Spark table.
# MAGIC - Materialized views are ideal here because the transformations involve a `groupBy`/aggregation (`cur_sales`), and materialized views are guaranteed to stay *correct* even if upstream data changes (Databricks' engine reprocesses only the necessary changed data where possible — this is called **incremental refresh for materialized views**).
# MAGIC
# MAGIC **Interview Q&A:**
# MAGIC - *Q: Why use a materialized view instead of a streaming table for `cur_sales`?*
# MAGIC  A: Because it involves an aggregation (`groupBy`/`sum`), and materialized views guarantee the result stays correct even when historical rows are updated/deleted — a streaming table processes each row only once and wouldn't automatically "fix" an aggregate if an old row changed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 10. Process Data Incrementally
# MAGIC
# MAGIC **Concept:** "Processing data incrementally" means only touching **new/changed data** on each run, instead of reprocessing the entire table from scratch every time. This is the single biggest efficiency win SDP gives you over hand-written batch jobs.
# MAGIC
# MAGIC SDP achieves incremental processing in two different ways depending on dataset type:
# MAGIC
# MAGIC 1. **Streaming tables** — incremental by construction. Every micro-batch, only new rows from the streaming source (Kafka offsets, new files via Auto Loader/cloudFiles, new rows appended to an upstream streaming table) are read, transformed, and appended. State (offsets/checkpoints) is fully managed by SDP — I never write manual checkpoint paths or watermark logic.
# MAGIC 2. **Materialized views** — even though they're conceptually "batch", the underlying engine tries to compute an **incremental refresh** rather than a full recompute whenever it's cheaper: it tracks changes in upstream data and only reprocesses what's changed, using Delta Lake features like **row tracking** and **deletion vectors**. If a fully incremental refresh isn't possible for a given query shape, it automatically falls back to a full recompute — I don't have to decide this myself.
# MAGIC
# MAGIC **Rule of thumb for choosing (a very common interview question):**
# MAGIC - If the source is **continuously/incrementally growing** and each record should be processed **exactly once**, or you need **high throughput + low latency** → use a **streaming table**.
# MAGIC - If the query does **joins/aggregations** where the source can also be **updated or deleted** (not just appended) → use a **materialized view** (it recomputes correctly when dimensions change; streaming tables don't recompute existing output when a joined dimension changes — "fast but wrong" if used for that case).
# MAGIC
# MAGIC ### 👉 In my code
# MAGIC - `first_dag.py` shows the **materialized view** style — incremental refresh happens transparently underneath `spark.read.table(...)` calls.
# MAGIC - `second_dag.py` shows the **streaming table** style — incremental processing is explicit via `spark.readStream.table(...)`.
# MAGIC - Both pipelines implement the **exact same business logic** (parse date → apply markup → aggregate revenue), which is a great way to see side-by-side how the *same* transformation looks in batch vs. incremental/streaming mode.
# MAGIC
# MAGIC **Interview one-liner:** *"Incremental processing means SDP only touches new/changed data per run. Streaming tables do this natively via streaming reads + checkpoints; materialized views do it via smart incremental-refresh under the hood, falling back to full recompute only when necessary."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 11. Process Streaming Tables — `second_dag.py` explained
# MAGIC
# MAGIC **Concept:** A **streaming table** is created using the `@dp.table` decorator on a function that performs a **streaming read** (`spark.readStream...`). Each row from the source is processed **exactly once**.
# MAGIC
# MAGIC ### My code: `second_dag.py`
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC # Streaming Table — reads sales as a stream and parses the date
# MAGIC @dp.table(name="src_sales_stream")
# MAGIC def src_sales():
# MAGIC     df = spark.readStream.table("sdp_catalog.source.sales")
# MAGIC     df = df.withColumn("sale_date", to_date(col("date"), "MM-dd-yyyy"))
# MAGIC     return df
# MAGIC
# MAGIC # Streaming Table — chained, applies markup to revenue
# MAGIC @dp.table(name="enr_sales_stream")
# MAGIC def enr_sales():
# MAGIC     df = spark.readStream.table("sdp_catalog.target.src_sales_stream")
# MAGIC     df = df.withColumn("revenue", col("revenue") * 1.5)
# MAGIC     return df
# MAGIC
# MAGIC # Streaming Table — chained, aggregates revenue by date
# MAGIC @dp.table(name="cur_sales_stream")
# MAGIC def cur_sales():
# MAGIC     df = spark.readStream.table("sdp_catalog.target.enr_sales_stream")
# MAGIC     df = df.groupBy("date").agg(sum("revenue").alias("total_sales"))
# MAGIC     return df
# MAGIC ```
# MAGIC
# MAGIC **What's happening:**
# MAGIC - This is **the exact same 3-stage pipeline as `first_dag.py`** (Bronze/Silver/Gold), but every stage now uses `@dp.table` + `spark.readStream.table(...)` instead of `@dp.materialized_view` + `spark.read.table(...)`.
# MAGIC - `src_sales_stream` continuously reads new rows landing in `sdp_catalog.source.sales`, converts the string date to a proper date.
# MAGIC - `enr_sales_stream` streams from `src_sales_stream` and applies the revenue markup — a **stateless** streaming transformation (each row processed independently, no cross-row state needed).
# MAGIC - `cur_sales_stream` streams from `enr_sales_stream` and does a **stateful** streaming aggregation (`groupBy("date")` + `sum`) — this requires Spark Structured Streaming to maintain running aggregate state per group, normally something you'd manage with watermarks in raw Structured Streaming, but SDP + the streaming table abstraction manages this for you.
# MAGIC
# MAGIC **Important subtlety worth mentioning in an interview:** streaming aggregations without a watermark can, in raw Structured Streaming, grow state indefinitely (since the "date" groups never officially "close"). In production pipelines, you'd typically add a **watermark** (`withWatermark`) on an event-time column before the `groupBy` to bound state size — this is one of the few things SDP does *not* fully automate away; you still need to reason about **stateful stream processing** correctly.
# MAGIC
# MAGIC **Interview Q&A:**
# MAGIC - *Q: What's the practical difference between running this as `first_dag.py` (materialized views) vs `second_dag.py` (streaming tables)?*
# MAGIC  A: `first_dag.py` recomputes from all available batch data each run (or incrementally-refreshes when possible) — good when I want "always correct" snapshots, e.g., a nightly report. `second_dag.py` processes only newly arrived rows continuously/on each trigger, giving lower latency and higher throughput — good for near-real-time dashboards — but requires care with stateful operations like the final aggregation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 12. Append Flow — `append_flow.py` explained
# MAGIC
# MAGIC **Concept:** Normally one dataset is written to by **one** flow. But SDP lets you attach **multiple flows to a single target table** using `@dp.append_flow` — each flow independently appends its own data into the same target, without interfering with each other. This is the **"fan-in" pattern**.
# MAGIC
# MAGIC > Official docs pattern:
# MAGIC > ```python
# MAGIC > dp.create_streaming_table("customers_us")
# MAGIC >
# MAGIC > @dp.append_flow(target="customers_us")
# MAGIC > def append_customers_us_west():
# MAGIC >     return spark.readStream.table("customers_us_west")
# MAGIC >
# MAGIC > @dp.append_flow(target="customers_us")
# MAGIC > def append_customers_us_east():
# MAGIC >     return spark.readStream.table("customers_us_east")
# MAGIC > ```
# MAGIC
# MAGIC ### My code: `append_flow.py`
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC # Creating an empty streaming table (just the target shell, no flow yet)
# MAGIC dp.create_streaming_table("total_sales")
# MAGIC
# MAGIC # Appending north sales into "total_sales"
# MAGIC @dp.append_flow(target="total_sales")
# MAGIC def north_sales():
# MAGIC     df = spark.readStream.table("sdp_catalog.source.north_sales")
# MAGIC     return df
# MAGIC
# MAGIC # Appending south sales into "total_sales"
# MAGIC @dp.append_flow(target="total_sales")
# MAGIC def south_sales():
# MAGIC     df = spark.readStream.table("sdp_catalog.source.south_sales")
# MAGIC     return df
# MAGIC ```
# MAGIC
# MAGIC **What's happening, step by step:**
# MAGIC 1. **`dp.create_streaming_table("total_sales")`** — first I explicitly create an **empty streaming table** called `total_sales`. This just declares the target dataset shell; at this point it has no flow writing into it.
# MAGIC 2. **`north_sales`** — an append flow that streams from `sdp_catalog.source.north_sales` and appends every new row directly into `total_sales`.
# MAGIC 3. **`south_sales`** — a second, independent append flow that streams from `sdp_catalog.source.south_sales` and **also** appends into the same `total_sales` table.
# MAGIC
# MAGIC The result: `total_sales` continuously receives rows from **two independent regional sources**, merged (unioned) into one combined streaming table — without me writing a manual `UNION`/`union()` operation myself. SDP runs both flows in parallel and manages them as two separate, independently-checkpointed streams writing to the same sink.
# MAGIC
# MAGIC **Why use append flows instead of just `union()`-ing two DataFrames in one function?**
# MAGIC - Each source can independently fail/retry/checkpoint without affecting the other.
# MAGIC - New sources can be added later just by adding another `@dp.append_flow(target="total_sales")` function — no need to touch existing flows.
# MAGIC - This is the recommended SDP pattern anytime multiple independent streaming sources need to land in one combined table (e.g., combining regional sales feeds, multiple IoT device topics, per-store data feeds, etc.)
# MAGIC
# MAGIC **Interview one-liner:** *"Append flow lets multiple independent streaming sources fan-in to one target table — each flow is created with `@dp.append_flow(target=...)`, and I first shell out the empty target with `dp.create_streaming_table(...)`."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 13. Auto CDC — `auto_cdc.py` explained
# MAGIC
# MAGIC > ⚠️ **Important distinction (say this in interviews — it shows depth):** `AUTO CDC` (`create_auto_cdc_flow` / `AUTO CDC INTO`) is a **Databricks Lakeflow-only** feature. It is **not** part of open-source Apache Spark Declarative Pipelines. If you're running plain OSS Spark SDP, you don't get `create_auto_cdc_flow` — you'd have to implement CDC logic yourself (e.g., manual `MERGE INTO`). On Databricks, it's available out of the box as one of the "production features" Lakeflow adds on top of SDP.
# MAGIC
# MAGIC **Concept — why AUTO CDC exists:** Implementing Change Data Capture (CDC) with hand-written `MERGE INTO` SQL requires you to solve, yourself: out-of-order event handling, deduplication, partial updates, and schema evolution. `AUTO CDC` handles **all of this declaratively** — you just describe the shape of the change feed and the target table, and Databricks handles ordering, dedup, and schema evolution.
# MAGIC
# MAGIC AUTO CDC supports:
# MAGIC - **SCD Type 1** — "overwrite": the target table always reflects only the **latest** value for each key (no history kept).
# MAGIC - **SCD Type 2** — "history": the target table keeps **every version** of a row, with `__START_AT` / `__END_AT` columns marking the validity window of each version.
# MAGIC - *(newer, Beta)* **Bitemporal** — SCD Type 2 extended across two time dimensions (business time + system time).
# MAGIC
# MAGIC ### My code: `auto_cdc.py`
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC # Empty target streaming tables — one for each SCD style
# MAGIC dp.create_streaming_table("products_scd2")
# MAGIC dp.create_streaming_table("products_scd1")
# MAGIC
# MAGIC # A temporary view that streams the raw CDC feed — used as the CDC "source"
# MAGIC @dp.temporary_view
# MAGIC def products_source():
# MAGIC     df = spark.readStream.table("sdp_catalog.source.products")
# MAGIC     return df
# MAGIC
# MAGIC # SCD Type 2 — keeps full history of every change per product_id
# MAGIC dp.create_auto_cdc_flow(
# MAGIC   target = "products_scd2",
# MAGIC   source = "products_source",
# MAGIC   keys = ["product_id"],
# MAGIC   sequence_by = col("updated_at"),
# MAGIC   except_column_list = ["updated_at"],
# MAGIC   stored_as_scd_type = "2"
# MAGIC )
# MAGIC
# MAGIC # SCD Type 1 — keeps only the latest state per product_id (overwrite)
# MAGIC dp.create_auto_cdc_flow(
# MAGIC   target = "products_scd1",
# MAGIC   source = "products_source",
# MAGIC   keys = ["product_id"],
# MAGIC   sequence_by = col("updated_at"),
# MAGIC   except_column_list = ["updated_at"],
# MAGIC   stored_as_scd_type = "1"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **What's happening, piece by piece:**
# MAGIC 1. Two **empty streaming table shells** are created up front: `products_scd2` and `products_scd1` — these will be populated purely by the AUTO CDC flows below.
# MAGIC 2. `products_source` is a **temporary view** wrapping a streaming read of the raw CDC feed `sdp_catalog.source.products` — this decouples "where the CDC data comes from" from "how it's applied," and lets me reuse the same source for **both** SCD flows without reading the source table twice in application code.
# MAGIC 3. **`dp.create_auto_cdc_flow(...)`** — the core function. Key parameters used here:
# MAGIC    - `target` — the streaming table to apply changes into (must already exist, hence step 1).
# MAGIC    - `source` — the CDC feed to read from (here, the `products_source` temp view).
# MAGIC    - `keys = ["product_id"]` — the column(s) that uniquely identify a row; AUTO CDC uses this to decide whether an incoming record is an **insert** (new key) or an **update** (matching key).
# MAGIC    - `sequence_by = col("updated_at")` — tells AUTO CDC how to **order** events for the same key, so out-of-order CDC events are still applied correctly (the record with the **latest** `updated_at` always wins, no matter what order it physically arrives in).
# MAGIC    - `except_column_list = ["updated_at"]` — excludes the `updated_at` column itself from being written into the final target table (since it was only needed for sequencing, not as business data — though for SCD Type 2 you'd typically still want it, or accept that `__START_AT`/`__END_AT` take its place).
# MAGIC    - `stored_as_scd_type` — `"2"` for the first flow → full history retained; `"1"` for the second → only latest state retained (an UPSERT/overwrite).
# MAGIC 4. Because **both** flows read from the same `products_source` temporary view but write to **different** targets with **different** `stored_as_scd_type`, this pipeline produces **two parallel representations of the same underlying change feed** — one for "current state only" reporting (`products_scd1`), and one for "full audit history" use cases like point-in-time analysis (`products_scd2`).
# MAGIC
# MAGIC **Interview Q&A:**
# MAGIC - *Q: Why exclude `updated_at` via `except_column_list`?*
# MAGIC  A: Because it's a control/sequencing column used purely to order CDC events correctly, not meant to be a normal business attribute duplicated into the final table (its role is already captured via `__START_AT`/`__END_AT` for the SCD-2 table).
# MAGIC - *Q: What happens if a change event for the same `product_id` arrives out of order (e.g., an older update arrives after a newer one)?*
# MAGIC  A: AUTO CDC compares `sequence_by` values and simply discards/ignores events that are older than the current state for that key — the target always reflects the record with the highest sequence value seen so far for a given key.
# MAGIC - *Q: Difference between SCD1 and SCD2 target table shape?*
# MAGIC  A: SCD1 target has the same columns as the source (minus excluded ones) — one row per key, always the latest values. SCD2 target has **extra system columns** `__START_AT` and `__END_AT` and can have **multiple rows per key**, one per historical version.
# MAGIC
# MAGIC **Interview one-liner:** *"AUTO CDC (Databricks-only) replaces hand-written MERGE INTO CDC logic — I give it keys + a sequence column + SCD type, and it declaratively handles upserts, out-of-order events, and (for SCD2) history tracking."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 14. Expectations (Data Quality) — `expectation.py` explained
# MAGIC
# MAGIC > ⚠️ Same important note as AUTO CDC: `@dp.expect`, `@dp.expect_or_drop`, `@dp.expect_or_fail`, and the `_all` variants are **Databricks Lakeflow** features layered on top of SDP (not present in vanilla open-source Spark SDP as of today, though the underlying `pyspark.pipelines` module is shared).
# MAGIC
# MAGIC **Concept:** An **expectation** is a data-quality rule attached directly to a dataset definition. It's a **SQL boolean expression** evaluated against every row flowing through the query. You choose an **action** for rows that fail:
# MAGIC
# MAGIC | Action | Python decorator | SQL syntax | Result |
# MAGIC |---|---|---|---|
# MAGIC | **Warn** (default) | `@dp.expect(name, condition)` | `EXPECT (...)` | Invalid rows are still **written to the target**; only metrics are logged |
# MAGIC | **Drop** | `@dp.expect_or_drop(name, condition)` | `EXPECT (...) ON VIOLATION DROP ROW` | Invalid rows are **silently dropped** before reaching the target; drop-count is logged |
# MAGIC | **Fail** | `@dp.expect_or_fail(name, condition)` | `EXPECT (...) ON VIOLATION FAIL UPDATE` | The **entire pipeline update fails** and rolls back atomically; requires manual intervention before rerunning |
# MAGIC
# MAGIC For **multiple rules at once**, Python offers dictionary-based group decorators: `@dp.expect_all(rules_dict)`, `@dp.expect_all_or_drop(rules_dict)`, `@dp.expect_all_or_fail(rules_dict)` — each dict maps `rule_name → SQL boolean condition string`.
# MAGIC
# MAGIC Important constraint: the constraint clause must be **valid SQL** and **cannot** contain custom Python functions, external service calls, or subqueries referencing other tables.
# MAGIC
# MAGIC ### My code: `expectation.py`
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC rules = {
# MAGIC     "rule1": "product_id IS NOT NULL",
# MAGIC     "rule2": "updated_at IS NOT NULL"
# MAGIC }
# MAGIC
# MAGIC @dp.table(name="products_table")
# MAGIC @dp.expect_all_or_fail(rules)
# MAGIC def products_table():
# MAGIC     df = spark.read.table("sdp_catalog.source.products")
# MAGIC     return df
# MAGIC ```
# MAGIC
# MAGIC **What's happening:**
# MAGIC 1. `rules` is a dictionary defining **two named data-quality constraints**:
# MAGIC    - `rule1` → `product_id IS NOT NULL` (every product must have a non-null primary key)
# MAGIC    - `rule2` → `updated_at IS NOT NULL` (every product must have a non-null last-updated timestamp — this matters a lot given `updated_at` is also used as the `sequence_by` column in the CDC pipeline above; a null here would break CDC ordering!)
# MAGIC 2. `@dp.expect_all_or_fail(rules)` applies **both rules together** with the strictest action: **fail the whole update** the moment *any* row violates *either* rule. This is the harshest of the three actions — appropriate here because `product_id` and `updated_at` are both critical/structural columns that downstream CDC logic depends on.
# MAGIC 3. **Decorator order matters and is applied bottom-up**: Python evaluates decorators from the one closest to the function upward. So `@dp.expect_all_or_fail(rules)` wraps the raw `products_table` function *first* (attaching the validation logic to it), and **then** `@dp.table(name="products_table")` wraps *that* result, registering it as a pipeline dataset. Conceptually: *"define the table, and gate it with these expectations before it's considered a valid table definition."*
# MAGIC 4. Inside the function, it's a plain batch read (`spark.read.table(...)`) of the raw `sdp_catalog.source.products` table — so `products_table` is a **materialized view-like batch table** that acts as a **validated Bronze/landing layer**, guaranteeing that anything downstream of it (like the `products_source` temp view feeding AUTO CDC) can trust that `product_id` and `updated_at` are never null.
# MAGIC
# MAGIC **Interview Q&A:**
# MAGIC - *Q: Why `expect_all_or_fail` here instead of `expect_all_or_drop`?*
# MAGIC  A: Because both `product_id` and `updated_at` are structural columns other flows (like the AUTO CDC keys/sequence column) depend on — silently dropping bad rows could mask an upstream data problem, whereas failing the update forces someone to investigate and fix the root cause before any wrong data flows further.
# MAGIC - *Q: What's the difference between `expect_all` and `expect_all_or_fail`?*
# MAGIC  A: `expect_all` just tracks metrics and **keeps** invalid rows in the output (warn-only, for both rules); `expect_all_or_fail` additionally **stops the entire update** if any rule fails for any row.
# MAGIC - *Q: Can expectations reference other tables?*
# MAGIC  A: No — the constraint must be a plain SQL boolean expression on the row itself; no subqueries referencing other tables, no custom Python functions, no external calls.
# MAGIC - *Q: Where do I see the results of expectations?*
# MAGIC  A: Via the pipeline's **Data quality** tab in the Databricks UI for a given dataset, or by querying the pipeline's **event log** directly for pass/fail metrics per constraint. Note: for `fail`-type expectations, since the update itself fails, no metrics are recorded — you instead get a structured error message identifying which record violated which named constraint.
# MAGIC
# MAGIC **Interview one-liner:** *"Expectations are named SQL boolean constraints on a dataset — warn keeps everything and logs metrics, drop removes bad rows quietly, fail aborts the whole update atomically. I used `expect_all_or_fail` because my rules protect structural columns (`product_id`, `updated_at`) that downstream CDC logic depends on."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 15. Pipeline Parameters — `parameters.py` explained
# MAGIC
# MAGIC **Concept:** Pipeline parameters let the **same pipeline source code** be reused across different environments (dev/staging/prod) or process different inputs on different runs, without editing the code itself — e.g., different catalog names, different file paths, or (as in my case) a dynamically-sized list of tables to generate.
# MAGIC
# MAGIC Two ways parameters are surfaced in Databricks Lakeflow pipelines:
# MAGIC 1. **Pipeline Parameters (Beta, SQL-only)** — defined as key–value pairs on the pipeline; referenced in **SQL** source code with `${parameter_name}` syntax. **Not usable directly from Python source code.**
# MAGIC 2. **Configuration field (works for Python too)** — a map of Spark configuration properties set on the pipeline; read at runtime in **Python** using `spark.conf.get("key")`. This is the general mechanism, and it's what I used.
# MAGIC
# MAGIC Parameter precedence (highest to lowest) when the same key is set in multiple places:
# MAGIC 1. Job run parameters (overrides for a single job run)
# MAGIC 2. Job parameters (defaults on the parent job)
# MAGIC 3. Pipeline task parameters (set on the pipeline task inside a job)
# MAGIC 4. Pipeline parameters (defaults defined in the pipeline settings)
# MAGIC
# MAGIC ### My code: `parameters.py`
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC import ast
# MAGIC
# MAGIC list_var = spark.conf.get("tables_list")
# MAGIC list_var_list = ast.literal_eval(list_var)
# MAGIC
# MAGIC for i in list_var_list:
# MAGIC     @dp.table(name=f"table_{i}")
# MAGIC     def table():
# MAGIC         df = spark.readStream.table("sdp_catalog.source.sales")
# MAGIC         return df
# MAGIC ```
# MAGIC
# MAGIC **What's happening, step by step:**
# MAGIC 1. **`spark.conf.get("tables_list")`** — reads a pipeline **Configuration** value named `tables_list` at runtime. Since configuration values are always injected as **strings**, if I configured, say, `tables_list = "['a','b','c']"` in the pipeline's Configuration settings (or via `--conf tables_list=...`), this line reads it back as the literal string `"['a','b','c']"`.
# MAGIC 2. **`ast.literal_eval(list_var)`** — safely parses that string into an actual Python list (`['a', 'b', 'c']`), since `spark.conf.get()` can only ever return strings, never native Python objects.
# MAGIC 3. **`for i in list_var_list: @dp.table(name=f"table_{i}") ...`** — this is the **"Creating Tables in a For Loop"** pattern straight from the official docs: I dynamically generate **one streaming table per entry** in the parameterized list, each named `table_<i>` (e.g. `table_a`, `table_b`, `table_c`), all pulling from the same source `sdp_catalog.source.sales`.
# MAGIC
# MAGIC **Why this matters / what I'm demonstrating:**
# MAGIC - This lets the **number and names of output tables** be controlled entirely from pipeline configuration — no code change needed to add/remove a table, just update the `tables_list` config value.
# MAGIC - This is exactly the parameterization use case the docs call out: *"reusing the same transformation logic to process from multiple data sources"* and *"separating variables from code."*
# MAGIC
# MAGIC **A subtle but important interview-worthy gotcha (Python closures in loops):** In the official `region_list` example, each generated function uses a **default argument trick** — `def regional_customer_orders(region_filter=region):` — specifically to capture the *current* value of the loop variable at each iteration. This avoids the classic Python "late-binding closure" bug, where all functions created in a loop would otherwise end up sharing the **same final value** of the loop variable by the time they're actually called (since Python closures capture variables by reference, not by value, unless you force it with a default argument). In my `parameters.py`, this isn't a *functional* bug only because the loop variable `i` is used **only inside the `name=f"table_{i}"` decorator argument** (which is evaluated immediately, at decoration time, so each table correctly gets its own unique name) — but the function **body** itself doesn't reference `i` at all, so all generated tables happen to pull identical logic from the same fixed source. If I *did* need per-`i` logic inside the function body (e.g., filtering by `i`), I would need the same `default-argument capture` trick as the official example to avoid every table silently using the same (last) value of `i`.
# MAGIC - Also worth noting: **the list of values driving a `for` loop that defines datasets must only ever grow (be additive) across runs** — removing or reordering values can break the identity of already-existing tables in the pipeline's dependency graph.
# MAGIC
# MAGIC **Interview Q&A:**
# MAGIC - *Q: Why `ast.literal_eval` instead of `eval`?*
# MAGIC  A: `literal_eval` only parses Python literals (lists, dicts, strings, numbers) safely — it can't execute arbitrary code, unlike `eval`, which would be a security risk since the config value is external input.
# MAGIC - *Q: Could I have used SQL-style `${tables_list}` parameters instead?*
# MAGIC  A: No — SQL-style pipeline parameters are **SQL-only**; since this file is Python, the correct mechanism is the pipeline's **Configuration** field read via `spark.conf.get(...)`.
# MAGIC - *Q: How would I override `tables_list` for a single run without changing the saved pipeline default?*
# MAGIC  A: Pass a parameter/configuration override from the pipeline UI ("Run with different settings"), from a Pipeline task's Parameters field inside a Databricks Job, or via the API's Start-update request.
# MAGIC
# MAGIC **Interview one-liner:** *"Parameters let the same pipeline code run differently per environment. In Python, I read them via `spark.conf.get()` from the pipeline's Configuration map; in SQL you'd reference them directly with `${param}` syntax. I used this to dynamically fan-out a variable number of tables from one parameterized list."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 16. Pipeline Orchestrating in a Job
# MAGIC
# MAGIC **Concept:** A pipeline **handles its own internal orchestration** — the dependency graph between its flows/datasets is resolved automatically. But a pipeline is usually just **one step** in a larger workflow: you might need it to run **on a schedule**, run **after** some other task, trigger a **downstream report** after it finishes, or coordinate **multiple pipelines** together with conditional/branching logic. For all of that, you wrap the pipeline in a **Job** (Databricks: **Lakeflow Jobs**) rather than building that logic into the pipeline itself.
# MAGIC
# MAGIC > Official guidance: *"A pipeline automatically resolves the dependencies between its datasets, so it handles simple, in-pipeline orchestration on its own. For orchestration that a pipeline isn't built for — such as conditional execution, branching on task outcomes, retries, or coordinating a pipeline with other types of work — use a dedicated workflow orchestrator instead of building the logic into the pipeline."*
# MAGIC
# MAGIC ### Ways to orchestrate a pipeline inside a bigger workflow:
# MAGIC
# MAGIC 1. **Lakeflow Jobs (the native/recommended way on Databricks)**
# MAGIC    - Add a **Pipeline task** to a Databricks Job.
# MAGIC    - The job can chain the pipeline task with other task types (notebooks, SQL, Python scripts) and set explicit **task dependencies** (run this pipeline, then run a downstream reporting notebook, etc.).
# MAGIC    - Jobs support scheduling (cron-like triggers), retries at the *task* level, and conditional branching between tasks — things a pipeline alone doesn't provide.
# MAGIC
# MAGIC 2. **Apache Airflow**
# MAGIC    - Use the `DatabricksSubmitRunOperator` from the `apache-airflow-providers-databricks` package to trigger a pipeline update from an Airflow DAG:
# MAGIC    ```python
# MAGIC    from airflow import DAG
# MAGIC    from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
# MAGIC    from airflow.utils.dates import days_ago
# MAGIC
# MAGIC    with DAG('ldp', start_date=days_ago(2), schedule_interval="@once") as dag:
# MAGIC        opr_run_now = DatabricksSubmitRunOperator(
# MAGIC            task_id='run_now',
# MAGIC            databricks_conn_id='CONNECTION_ID',
# MAGIC            pipeline_task={"pipeline_id": "8279d543-063c-4d63-9926-dae38e35ce8b"}
# MAGIC        )
# MAGIC    ```
# MAGIC    - Requires Airflow ≥ 2.1.0 and the Databricks provider package ≥ 2.1.0.
# MAGIC
# MAGIC 3. **Azure Data Factory (ADF)**
# MAGIC    - Trigger a pipeline update via a **Web activity** calling the REST API: `POST https://<databricks-instance>/api/2.0/pipelines/<pipeline-id>/updates`, with an `Authorization: Bearer <token>` header, and an optional JSON body (e.g. `{"full_refresh": "true"}` to force a full recompute).
# MAGIC    - Since this API call is **asynchronous** (it returns immediately after *starting* the update, not after it *completes*), if a downstream ADF task depends on the pipeline finishing, you need to poll for completion — typically with an **Until activity** containing a **Wait activity** + a follow-up Web activity that checks the update's `state` field.
# MAGIC    - ⚠️ **Retry-multiplication gotcha:** both the pipeline itself and the ADF activity calling it can have their own retry counts — these **multiply**. E.g., pipeline default retries = 5, ADF retry = 3 → up to **15** total retries on failure. Databricks recommends limiting retries on one side (commonly via the pipeline's `pipelines.numUpdateRetryAttempts` setting) to avoid excessive retry storms.
# MAGIC
# MAGIC ### Preparing a pipeline for orchestration
# MAGIC - Design each pipeline around a **distinct, independently schedulable unit of work** — if you have one large pipeline doing unrelated things, split it into smaller pipelines (moving tables between pipelines) so a job/orchestrator can coordinate them as **separate tasks** with proper control flow between them.
# MAGIC - This matters directly for **Expectations with `fail` actions**: in a *triggered* pipeline run, one flow failing does **not** automatically fail sibling parallel flows — but in a *continuous* pipeline, a failure **does** stop the failing flow and everything depending on it. If you need finer-grained control over what happens across pipeline boundaries when a validation fails (e.g., stop a whole downstream chain), split validation and downstream work into **separate pipelines** and coordinate them via job-level control flow.
# MAGIC
# MAGIC **Interview one-liner:** *"A pipeline auto-orchestrates its own internal flow dependencies, but for scheduling, cross-pipeline coordination, retries-with-branching, or chaining a pipeline with non-pipeline work, I wrap it in a Job (Lakeflow Jobs' Pipeline task, or externally via Airflow's `DatabricksSubmitRunOperator` / ADF's REST Web activity)."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 17. Full Concept Map — How My 6 Files Fit Together
# MAGIC
# MAGIC ```
# MAGIC sdp_catalog.source.sales
# MAGIC         │
# MAGIC         ├── first_dag.py  (BATCH / materialized views)
# MAGIC         │     src_sales(mv) → enr_sales(mv) → cur_sales(mv)
# MAGIC         │
# MAGIC         └── second_dag.py (STREAMING / streaming tables)
# MAGIC               src_sales_stream(st) → enr_sales_stream(st) → cur_sales_stream(st)
# MAGIC
# MAGIC sdp_catalog.source.north_sales ─┐
# MAGIC                                   ├── append_flow.py → total_sales (fan-in streaming table)
# MAGIC sdp_catalog.source.south_sales ─┘
# MAGIC
# MAGIC sdp_catalog.source.products
# MAGIC         │
# MAGIC         ▼
# MAGIC expectation.py
# MAGIC   products_table (@dp.table + @dp.expect_all_or_fail)  ── validated Bronze layer
# MAGIC         │
# MAGIC         ▼  (conceptually feeds into)
# MAGIC auto_cdc.py
# MAGIC   products_source (temp view, streaming read)
# MAGIC         ├── create_auto_cdc_flow(SCD Type 2) → products_scd2  (full history)
# MAGIC         └── create_auto_cdc_flow(SCD Type 1) → products_scd1  (latest state only)
# MAGIC
# MAGIC parameters.py
# MAGIC   spark.conf.get("tables_list") → ast.literal_eval → for-loop
# MAGIC         → table_a, table_b, table_c ... (dynamically generated streaming tables,
# MAGIC            all from sdp_catalog.source.sales, count controlled by pipeline Configuration)
# MAGIC
# MAGIC Orchestration layer (outside all of the above):
# MAGIC   Lakeflow Jobs (Pipeline task) / Airflow (DatabricksSubmitRunOperator) / ADF (REST Web activity)
# MAGIC         → schedules, chains, and coordinates the pipeline(s) above with the rest of the workflow
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 18. Rapid-Fire Interview Q&A Cheat Sheet
# MAGIC
# MAGIC **Q: What's the difference between SDP and Databricks Lakeflow pipelines?**
# MAGIC A: SDP is the open-source core (Apache Spark 4.1+, `pyspark.pipelines`) offering flows, datasets (streaming tables/materialized views/temp views), pipelines, sinks, append flows, and automatic orchestration. Lakeflow pipelines run this same engine on Databricks Runtime and add production-only features: **AUTO CDC**, a **queryable event log**, **continuous mode**, and **update flows/foreachBatch sinks**. (Data-quality expectations were Lakeflow-only but are now also available for standalone materialized views/streaming tables in Databricks SQL.)
# MAGIC
# MAGIC **Q: What are the three types of datasets in SDP?**
# MAGIC A: Streaming table (incremental, exactly-once, append-friendly), materialized view (batch, always-correct, good for joins/aggregations), temporary view (pipeline-scoped, not persisted).
# MAGIC
# MAGIC **Q: What is a flow?**
# MAGIC A: The foundational unit — reads from a source, transforms, writes to a target. Every `@dp.table`/`@dp.materialized_view` function body defines a flow implicitly; `@dp.append_flow` and `create_auto_cdc_flow` create flows explicitly and by name.
# MAGIC
# MAGIC **Q: How does SDP know the order to run my tables in?**
# MAGIC A: It statically analyzes which tables/views each flow's code reads from (e.g., `spark.table("src_sales")` inside `enr_sales`'s definition) and builds a dependency DAG automatically — no manual orchestration needed.
# MAGIC
# MAGIC **Q: How do you test a pipeline before running it for real?**
# MAGIC A: `spark-pipelines dry-run` — validates syntax, analysis (e.g. selecting a non-existent column/table), and graph structure (e.g. cyclic dependencies) without reading or writing any actual data.
# MAGIC
# MAGIC **Q: What operations should you NEVER use inside a dataset-defining function?**
# MAGIC A: `collect()`, `count()`, `pivot()`, `toPandas()`, `save()`, `saveAsTable()`, `start()`, `toTable()` — the function must be side-effect-free and simply **return** a DataFrame; SDP re-evaluates this code multiple times during planning.
# MAGIC
# MAGIC **Q: What are the 3 actions an expectation can take?**
# MAGIC A: Warn (`expect` — keep bad rows, just log metrics), Drop (`expect_or_drop` — silently remove bad rows), Fail (`expect_or_fail` — abort and roll back the whole update atomically).
# MAGIC
# MAGIC **Q: What's the difference between SCD Type 1 and SCD Type 2 in AUTO CDC?**
# MAGIC A: Type 1 overwrites — target always shows only the latest value per key. Type 2 preserves full history — every version of a row is kept, with `__START_AT`/`__END_AT` marking each version's validity window.
# MAGIC
# MAGIC **Q: How does AUTO CDC handle out-of-order events?**
# MAGIC A: Via the `sequence_by` column — it always applies the event with the highest sequence value for a given key, regardless of the physical arrival order.
# MAGIC
# MAGIC **Q: How do you parameterize a pipeline?**
# MAGIC A: In SQL, via `${parameter_name}` pipeline parameters (Beta). In Python, via the pipeline's Configuration map, read at runtime with `spark.conf.get("key")` — values are always strings, so parse them (e.g., `ast.literal_eval`) if you need a richer type like a list.
# MAGIC
# MAGIC **Q: Why not just build orchestration logic (scheduling, branching, retries across tasks) into the pipeline itself?**
# MAGIC A: Because a pipeline is designed to resolve *its own* internal dataset dependencies only — for scheduling, cross-task branching, conditional execution, or coordinating multiple pipelines/non-pipeline work together, use a dedicated orchestrator: Lakeflow Jobs' Pipeline task, Airflow's `DatabricksSubmitRunOperator`, or an external caller like Azure Data Factory hitting the pipeline's REST API.
# MAGIC
# MAGIC **Q: Materialized view vs. streaming table — when do you pick which?**
# MAGIC A: Streaming table: append-only/continuously-growing source, need exactly-once + low latency + high throughput (e.g., ingestion from Kafka/cloud files). Materialized view: source involves joins/aggregations or can be updated/deleted, and you need the result to always be *correct* (recomputes when dimensions change) rather than fast-but-possibly-stale.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 19. One-Page Summary (Final Revision Pass)
# MAGIC
# MAGIC - **Apache Spark** = distributed, in-memory, lazily-evaluated engine for batch/streaming/SQL/ML.
# MAGIC - **PySpark** = Python API on top of Spark; `pyspark.pipelines` is the sub-module that gives us SDP.
# MAGIC - **SDP** = declarative ETL framework built into Spark 4.1+; declare tables, SDP handles orchestration/incrementality/errors.
# MAGIC - **vs traditional Spark**: procedural = you own every step; declarative (SDP) = you own only the "what," system owns the "how."
# MAGIC - **Why SDP**: removes manual DAG-building, manual checkpoint/watermark code, manual retry/rollback, manual data-quality checks, and hard-to-test pipelines.
# MAGIC - **Flow** = read + transform + write (the atomic unit).
# MAGIC - **Dataset** = streaming table (incremental) | materialized view (batch, always correct) | temporary view (pipeline-scoped).
# MAGIC - **Pipeline** = the deployable unit; a set of flows/datasets + spec file (`spark-pipeline.yml`), run via `spark-pipelines run`/`dry-run`.
# MAGIC - **Batch tables** (`first_dag.py`) = `@dp.materialized_view` + `spark.read` — Bronze→Silver→Gold via plain batch reads.
# MAGIC - **Incremental processing** = streaming tables (explicit, streaming reads) or materialized views (implicit, smart incremental refresh).
# MAGIC - **Streaming tables** (`second_dag.py`) = `@dp.table` + `spark.readStream` — same logic as batch, but continuous/exactly-once.
# MAGIC - **Append flow** (`append_flow.py`) = multiple independent flows fan-in to one target via `@dp.append_flow(target=...)`, after shelling it with `dp.create_streaming_table(...)`.
# MAGIC - **AUTO CDC** (`auto_cdc.py`, Databricks-only) = `create_auto_cdc_flow(keys=..., sequence_by=..., stored_as_scd_type=...)` — declarative upsert/history handling instead of manual `MERGE INTO`.
# MAGIC - **Expectations** (`expectation.py`, Databricks-only) = `@dp.expect` / `_or_drop` / `_or_fail` (+ `_all` variants for dicts of rules) — declarative data-quality gates: warn, drop, or fail.
# MAGIC - **Parameters** (`parameters.py`) = SQL uses `${param}`; Python uses `spark.conf.get("key")` from the pipeline's Configuration map — enables environment/data-driven reuse of the same code (including dynamic for-loop table generation).
# MAGIC - **Orchestrating in a job** = pipelines self-orchestrate internally only; use Lakeflow Jobs / Airflow (`DatabricksSubmitRunOperator`) / ADF (REST Web activity + polling) for scheduling, retries, branching, and multi-pipeline coordination.