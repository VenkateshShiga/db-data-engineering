
1. *Architecture of Databricks*
2. *Databricks setup on AZURE*
3. *Data Lakehouse*
4. *unity Catalog*
5. *Data Engineering*
	1. *Notebooks*
	2. *ETL with DLT - Delta Live Tables*
	3. *Jobs/Workflows*
	4. *Autoloader*
6. *Data Analyst*
	1. *SQL Warehouse / Queries*
	2. *Dashboards*
7. *CICD with Databricks*
8. *Setup GIT/DevOps*
9. *Databricks CLI & API*
10. *Serverless Offerings & Benefits*
11. *Cost Analysis & Optimizations*

# High Level Architecture 

> [!info] Reference - https://docs.databricks.com/aws/en/getting-started/high-level-architecture

### What is Databricks 

Databricks is a unified, cloud based Data & AI platform which provides tools for data analysis, data engineering, data science & AI, data warehousing
### Why Databricks & Lakehouse

![[Pasted image 20260820175853.png]]

### High Level architecture of Databricks

##### Databricks Objects - 

At the account level, you manage:

- Identity and access: Users, groups, service principals, SCIM provisioning, and SSO configuration.
    
- Workspace management: Create, update, and delete workspaces across multiple regions.
    
- Unity Catalog metastore management: Create and attach metastore to workspaces.
    
- Usage management: Billing, compliance, and policies.

An account can contain multiple workspaces and Unity Catalog metastores.

- **Workspaces** are the collaboration environment where users run compute workloads such as ingestion, interactive exploration, scheduled jobs, and ML training.
    
- **Unity Catalog metastores** are the central governance system for data assets such as tables and ML models. You organize data in a metastore under a three-level namespace:

`<catalog-name>.<schema-name>.<object-name>`

>[!IMPORTANT] Metastores are attached to workspaces, `one metastore can be attached to multiple workspaces`

![[Pasted image 20260822171946.png]]

##### Workspace architecture

Databricks operates out of a **control plane** and a **compute plane**.

- The **control plane** includes the backend services that Databricks manages in your Databricks account. The control plane is located in the Databricks account, not your cloud account. The web application is in the control plane.
    
- The **compute plane** is where your data is processed. There are two types of compute planes depending on the compute that you are using.
    
    - For serverless compute, the serverless compute resources run in a **_serverless compute plane_ in your Databricks account**.
    - For classic Databricks compute, the compute resources are in your **AWS account in what is called the _classic compute plane_**.

**Classic Compute Plane architecture**
![[Pasted image 20260822184712.png]]
**Serverless Compute Plane architecture**
![[Pasted image 20260822184719.png]]
## Workspace storage

Workspace storage contains two categories of data: 

- workspace `file system data`
- workspace `system data`

Both are separate from your own data objects (such as Unity Catalog tables and volumes).

### Workspace file system data

The workspace file system stores the assets that users create and manage through the Databricks UI. These include:

- Notebooks
- SQL queries and dashboards
- Alerts
- Repos (folders attached to Git repositories)
- Libraries (`.whl`, `.jar`)
- Python files, YAML configuration files, and other small files
### Workspace system data

Every Databricks workspace also stores system data generated internally by Databricks features. This data is too large to store in memory or databases, or needs to persist beyond the lifetime of a single compute resource. Examples of workspace system data include:

- SQL query results and cached query results
- Job run results
- Notebook revisions
- SQL query plans used for observability
- Cluster logs

### Serverless workspaces

Serverless workspaces use default storage, which is a `fully managed storage location` for internal workspace system data and Unity Catalog data assets. 

Serverless workspaces also `support the ability to connect to your cloud storage locations for your own catalogs, tables, and other data assets`.

### Classic workspaces

>[!WARNING] Do not delete or modify the workspace storage in your cloud account. A Databricks workspace depends on both its control plane databases and its workspace storage for correct operation. If workspace storage is deleted, the workspace cannot be recovered.

In classic workspaces, workspace system data is distinct from [What is DBFS?](https://docs.databricks.com/aws/en/dbfs/). Although both may reside in the same cloud storage bucket in classic workspaces, they serve different purposes. DBFS root is a user-accessible file system, while workspace system data is used internally by Databricks features.