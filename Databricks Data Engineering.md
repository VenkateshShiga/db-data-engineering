#### Stack - SQL, Python, Apache Spark & Databricks

### Detailed Learning Roadmap

**1. Phase 1: Python for Data Engineering:** Weeks 1–3 | Target: 10–12 hrs/week.

- **Core Syntax & Structures:** Master control flows, functions, list comprehensions, and data structures (`lists`, `dicts`, `sets`, `tuples`).
    
- **Data File Handling:** Practice reading and manipulating JSON, CSV, and Parquet files using Python’s native libraries and `pandas`.
    
- **Modular Code:** Learn Object-Oriented Programming (OOP) basics, exception handling, and virtual environment management (`venv` or `conda`).
    
- **Bridging DBA to Code:** Practice translating complex SQL queries into Python logic.

**2. Phase 2: Apache Spark & PySpark Fundamentals:** Weeks 4–6 | Target: 10–12 hrs/week.

- **Spark Architecture:** Understand Driver, Worker Nodes, Executors, RDDs, DataFrames, and how distributed memory works.
    
- **DataFrame Operations:** Master PySpark syntax for filtering, aggregations, joining, windowing, and handling null values.
    
- **Execution Mechanics:** Learn Lazy Evaluation, Transformations vs. Actions, Narrow vs. Wide dependencies, Partitioning, and Shuffling.
    
- **Join Optimizations:** Map your DBA knowledge of join types to PySpark optimizations like Broadcast Hash Joins.

**3. Phase 3: Databricks Core, Delta Lake & Medallion Architecture:** Weeks 7–9 | Target: 10–12 hrs/week.

- **Delta Lake Mechanics:** Understand transaction logs (`_delta_log`), ACID properties, time travel (`VERSION AS OF`), schema enforcement, and schema evolution.
    
- **Incremental Ingestion:** Learn Auto Loader (`cloudFiles`) to ingest batch and real-time streams from cloud storage into Delta tables.
    
- **Medallion Architecture:** Build multi-stage pipelines: **Bronze** (raw landing) → **Silver** (cleaned/filtered) → **Gold** (business-level aggregations).
    
- **Performance Tuning:** Master Delta table optimization commands (`OPTIMIZE`, `Z-ORDER`, Liquid Clustering, and `VACUUM`).

**4. Phase 4: Governance, Orchestration & Spark UI:** Weeks 10–12 | Target: 10–12 hrs/week.

- **Data Governance:** Use **Unity Catalog** to set up metastores, grant fine-grained access control, configure row/column masking, and trace data lineage.
    
- **Pipeline Orchestration:** Build declarative pipelines with **Lakeflow Jobs / Workflows** and trigger tasks via schedules or file arrival events.
    
- **Troubleshooting:** Learn to read the **Spark UI** to diagnose bottleneck issues like data skew, disk spills, and memory allocation issues.
    
- **CI/CD:** Learn Git integration, Databricks Repos, and Declarative Automation Bundles (DABs) for deployment.

**5. Phase 5: Portfolio Project & Certification:** Weeks 13–14 | Target: 10–12 hrs/week.

- **End-to-End Capstone:** Build an automated Medallion pipeline that ingests continuous source data, cleanses it, enforces Unity Catalog permissions, and outputs gold analytical tables.
    
- **Certification:** Sit for the **Databricks Certified Data Engineer Associate** exam to validate your skills.