---
tags:
  - databricks
  - data-engineering
  - notebook
  - cheatsheet
date: 2026-09-16
---
# 🚀 Databricks Notebook Magic Commands & Fundamentals

### Notebook Essentials & Behavior

Databricks Notebooks serve as an **Integrated Development Environment (IDE)** directly inside the browser.

- **Auto-Save & Version Control:** Notebooks automatically save changes in real time. Full modification history is tracked time-wise under the *Last Edited* menu.
- **Default Storage Location:** Newly created, unrenamed notebooks are saved under the `drafts` folder. Renamed notebooks reside in `/Workspace/Users/<user-email>/`.
- **Multi-Language Support:** A single notebook can run cells in Python, SQL, R, Scala, and Markdown simultaneously using **Magic Commands**.
---
## Databricks Magic Commands Reference

**Magic commands** are special built-in commands in Databricks notebooks that let you change the behavior of an entire cell, switch programming languages, or interact directly with the underlying operating system and file system. They always begin with a percent sign (`%`).

## 🧙‍♂️ Databricks Magic Commands

Magic commands allow you to override the default notebook execution language on a **per-cell basis**.

| Magic Command | Target Language / Purpose | Primary Use Case                                                |
| :------------ | :------------------------ | :-------------------------------------------------------------- |
| `%python`     | Python / PySpark          | Data manipulation, ETL logic, PySpark DataFrames                |
| `%sql`        | SQL                       | Querying Delta tables, database objects, aggregated queries     |
| `%md`         | Markdown                  | Technical documentation, flow explanations, notes               |
| `%fs`         | DBFS / Volume File System | Directory navigation, file listing, copying, removing           |
| `%run`        | Notebook Modularization   | Importing functions, variables, and scope from another notebook |
| `%skip`       | Execution Control         | Skipping cell execution during run-all tasks                    |
| `%scala`      | Scala                     | High-performance legacy Spark code (less common now)            |
| `%r`          | R Language                | Analytics & statistical modeling                                |

---
## 1. 📁 File System Magics (`%fs`)
Allows you to interact with the **Databricks File System (DBFS)** without needing to write Python or Scala code.

> [!warning] Unsupported Command
> Note that `%fs cat` is **not** a valid command. Use `%fs head` instead.

### Supported Operations
- **`%fs ls <path>`** :: Lists files and directories in a path.
- **`%fs head <path>`** :: Displays the first few thousand bytes of a file.
- **`%fs cp <source> <target>`** :: Copies a file or directory.
- **`%fs mv <source> <target>`** :: Moves or renames a file or directory.
- **`%fs rm <path>`** :: Removes a file. Add `-r` after `rm` to recursively delete a directory.
- **`%fs mkdirs <path>`** :: Creates a new directory path.
- **`%fs mounts`** :: Lists all currently mounted object storage buckets.

Used to inspect, list, and interact with mounted file systems, DBFS, and Volumes.

Bash
```
# List available datasets provided by Databricks
%fs ls dbfs:/databricks-datasets/

# Examine specific sample data directory
%fs ls dbfs:/databricks-datasets/bikeSharing/
```

> [!TIP] Default Sample Datasets Databricks comes pre-loaded with sample datasets at `dbfs:/databricks-datasets/` containing sample data (retail, bike sharing, credit card, government data) for practice without external cloud setup.

---

## 2. Language Magics
Overrides the default language for that specific cell, turning Databricks into a polyglot environment.
### Supported Operations
- **`%python`** :: Executes Python code.
- **`%scala`** :: Executes Scala code.
- **`%sql`** :: Executes Spark SQL queries.
- **`%r`** :: Executes R code.
##### 🟢 Language Switchers (`%sql`, `%python`, `%md`)
Overriding cell language without changing the overall notebook default settings.

```sql
%sql
SELECT current_timestamp() AS execution_time, current_date() AS today;
```
---
## 3. Workflow Control & Package Magics
Used to control how notebooks run sequentially or to manage libraries dynamically inside your active session.

> [!important] The `%skip` Command
> Add `%skip` at the very beginning of a cell to bypass it during bulk execution. This allows you to test or share notebooks safely without deleting or commenting out heavy blocks of code.
### Supported Operations
- **`%skip`** :: Prevents the cell from running when using "Run All", "Run All Above", or "Run All Below".
- **`%run <notebook-path>`** :: Runs another notebook from inside your current notebook (useful for importing shared helper functions).
- **`%pip install <package>`** :: Installs notebook-scoped Python libraries.
- **`%uv pip install <package>`** :: Installs notebook-scoped packages at much faster speeds using the `uv` package manager wrapper.
---
## 4. Operating System / Shell Magics (`%sh`)
Executes standard **Linux shell commands** directly on the driver node of your Databricks cluster. 

> [!tip] Viewing Full Files
> If you need to use `cat` to view a full file, you must use `%sh` combined with the absolute local mount path prefix (`/dbfs/`).
### Supported Operations
- **`%sh cat /dbfs/FileStore/your_file.txt`** :: Views the entire content of a file.
- **`%sh pip install <package>`** :: Installs a Python package onto the cluster's driver node.
- **`%sh wget <url>`** :: Downloads files from the internet directly to the driver.
- **`%sh ls -la`** :: Performs a detailed list of the local Linux directory.
- **`%sh pwd`** :: Prints the current working directory of the cluster's operating system.

---
## 5. Auxiliary & Utility Magics
Used for documentation, cell output sizing, or rendering custom visuals.
### Supported Operations
- **`%md`** :: Renders the cell contents as standard Markdown (for text, titles, lists, and images).
- **`%set_cell_max_output_size_in_mb <size>`** :: Sets a threshold (1-20 MB) to limit text output spam for all subsequent cells.
- **`%tensorboard --logdir <logs>`** :: Displays the TensorBoard UI inline (available on ML runtimes).