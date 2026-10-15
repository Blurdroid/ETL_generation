# Dynamic ETL Studio — Architecture, Features & Debugging Guide

**Project**: Dynamic ETL Studio  
**Author**: Ziad Alashry  
**Last Updated**: October 2026  
**Environment**: Python 3.10+, Streamlit, Informatica PowerCenter  

---

## 1. Executive Summary & Architecture Overview

Dynamic ETL Studio is an enterprise-grade automated data engineering accelerator designed to bridge requirement specifications (RSDs), schema lineage catalogs, and Informatica PowerCenter production assets. It converts banking source specifications into valid, deployable PowerCenter XML workflows without manual mapping errors or precision truncations.

### Architectural Data Flow Layers

The platform models modern banking data warehouse architectures according to Data Vault 2.0 and Dimensional Modeling principles:

1. **Source Systems (SRC)**: Core transaction engines including T24 (Core Banking), CARD400 (Card Management), CR2 (Channel/ATM), and IVR (Interactive Voice Response).
2. **Staging (STG)**: Raw 1:1 copies of source tables with zero business transformations applied. Data lands exactly as structured at the source.
3. **Raw Data Vault (DV)**: The historical system of record utilizing Hubs (business keys), Links (unit of work / relationships), and Satellites (descriptive context over time) with full auditability.
4. **Business Vault (BV)**: Extends the Raw Data Vault with derived business rules, computed calculations, soft business logic, and consolidated master records.
5. **Information Delivery (ID)**: Curated publication datasets that stage clean, conformed data ready for enterprise consumers.
6. **Information Mart (iMart)**: Dimensional star/snowflake schemas (Fact and Dimension tables) optimized for BI reporting, executive dashboards, and analytics (e.g., Tableau, PowerBI, OFSAA).

---

## 2. Core Modules & Feature Breakdown

### Feature 1: Interactive Dashboard UI & Orchestrator (`app.py`)

* **Role**: Primary user interface built on Streamlit providing a visual pipeline builder, metadata visualizer, and deployment hub.
* **Key Capabilities**:
  * **Interactive S2T Lineage Matrix**: Interactive filtering by source table, status (`Mapped`, `Formula`, `Dropped`), and text search.
  * **Topological Lineage Visualization**: Renders Graphviz dependency graphs and exports publication-ready PNG diagrams using native Segoe UI typography.
  * **Direct Informatica Deployment**: Integrated `pmrep` deployment bridge supporting domain authentication, folder targeting, and live import execution logs.
  * **Evidence & Trust Auditing**: 0% hallucination verification displaying hop-by-hop XML `<CONNECTOR>` proofs.

### Feature 2: Requirements Gap Analysis Engine (`gap_analysis.py`)

* **Role**: Ingests functional requirements documents (`.docx`) and compares them against enterprise lineage catalogs.
* **Key Capabilities**:
  * Parses structured tables inside Word RSDs (`python-docx`).
  * Cross-references source table/column catalogs against `table_lineage.csv`, `column_lineage.csv`, and `unresolved.csv`.
  * Identifies unmapped source fields, missing business rules, and orphaned target ports.
  * Produces an automated gap matrix highlighting required transformations before ETL code generation.

### Feature 3: Dynamic ETL Generator & Schema Self-Healing (`etl_generator.py`)

* **Role**: Programmatic pipeline synthesizer translating high-level mapping intent into validated ETL specifications.
* **Key Capabilities**:
  * **AST Rule Generation**: Translates natural language or business formulas (e.g., hash keys, string concatenation, filters) into deterministic expression trees.
  * **Precision & Datatype Healing**: Resolves length, scale, and type conflicts (e.g., string to timestamp, integer overflow prevention) automatically before XML emission.
  * **Schema Normalization**: Synchronizes port definitions between upstream source definitions and downstream target definitions.

### Feature 4: Informatica PowerCenter XML Generator (`powercenter_xml_generator.py` & `powercenter_agent.py`)

* **Role**: Low-level XML DOM serialization engine generating 100% compliant Informatica PowerCenter repository XML files.
* **Key Capabilities**:
  * Emits standard `<POWERMART>`, `<REPOSITORY>`, and `<FOLDER>` XML hierarchies.
  * Constructs full mapping transformations:
    * **Source Qualifier (SQ)**: SQL queries, extraction filters, and port mappings.
    * **Expression (EXP)**: Input, output, and variable ports containing verbatim Informatica expressions (`IIF`, `TO_DATE`, `CONCAT`, `MD5`).
    * **Joiner (JNR)**: Multi-source join conditions with normal, master, or detail semantics.
    * **Router (RTR)**: Conditional routing groups for active and rejected records.
    * **Union (UNN)**: Merging heterogeneous data streams.
    * **Sequence Generator (SEQ)**: Deterministic generation of surrogate keys (`NEXTVAL`).
  * Generates runtime `<CONFIG>`, `<SESSION>`, and `<WORKFLOW>` objects linked via `<TASKINSTANCE>` and `<WORKFLOWLINK>` elements.

---

## 3. Workflow Explanation: Lineage Categories Deep-Dive

In the **Explain Workflow** tab, each column path is classified into one of three distinct categories to provide complete transparency for data governance and debugging:

### 1. Active Mapping (`Mapped (Active)`)
* **What it means**: Columns that originate from an identified upstream source table and flow uninterrupted through the pipeline into a target table column.
* **Technical Resolution**: The DAG traversal engine starts from the target port, walks backward through all `<CONNECTOR>` hops across transformations, and successfully reaches a `<SOURCEFIELD>`.
* **Sub-types**:
  * **Direct Pass-through**: Value flows 1:1 without alteration (e.g., `SRC_IVR.PHONE_NUM` → `SAT_IVR.PHONE_NUM`).
  * **Renamed / Type-Cast**: Value passes through an Expression or Source Qualifier where the port name or data type is standardized.

### 2. Derived / Formula (`Derived / Formulas`)
* **What it means**: Target table columns populated via calculated logic, mathematical operations, lookup transformations, or system-generated sequences.
* **Technical Resolution**: Detected in Expression transformations via `<TRANSFORMFIELD EXPRESSION="...">`.
* **Sub-types**:
  * **Derived**: An expression that combines or modifies upstream source columns (e.g., `FIRST_NAME || ' ' || LAST_NAME`, `MD5(SRC_SYS_ID || '|' || CUST_ID)`).
  * **Formula**: Standalone expressions without source dependencies, such as constants or environment variables (e.g., `'IVR'` for Source System Name, `SYSDATE` for Load Timestamp).
  * **Sequence / Auto-Gen**: Surrogate primary keys generated using Sequence Generators (`NEXTVAL` / `CURRVAL`).

### 3. Filtered / Dropped (`Filtered / Dropped`)
* **What it means**: Columns defined in the source schema that are **intentionally not propagated** to the target table.
* **Technical Resolution**: The engine scans all fields in the `<SOURCE>` definition. Any source field that has no downstream connector chain terminating in a `<TARGET>` column is cataloged as `Dropped`.
* **Why it matters**: Confirms that missing columns were deliberately filtered or excluded by architectural design rather than dropped accidentally.

---

## 4. Debugging & Troubleshooting Guide

| Issue / Symptom | Probable Cause | Diagnostic & Resolution Steps |
| :--- | :--- | :--- |
| **PowerCenter XML Import Fails with "Invalid connector link"** | Port mismatch or missing port between upstream transformation and downstream target. | 1. Open the generated XML in a text editor.<br>2. Search for `<CONNECTOR>` tags referencing the error port.<br>3. Verify that `FROMFIELD` exists in `FROMINSTANCE` and `TOFIELD` exists in `TOINSTANCE`. |
| **Numeric Overflow / Precision Truncation** | Source precision exceeds target column length (e.g., `DECIMAL(18,2)` mapped to `DECIMAL(10,2)`). | Check `etl_generator.py` auto-healing rules. Ensure target DDL matches the maximum length specified in the source catalog. |
| **`pmrep` Deployment Connection Timeout** | PowerCenter domain name, gateway host, or port configuration is unreachable. | 1. Verify VPN or network reachability to the Informatica host.<br>2. Run `pmrep connect -r <repo> -d <domain> -n <user> -X <password>` manually from command line to verify credentials. |
| **Graphviz Rendering Failure in Streamlit UI** | Graphviz binaries not on system `PATH` or dot syntax parsing exception. | The application automatically falls back to PIL/Pillow matrix rendering. If native graphs are required, verify `dot -V` in command prompt. |
| **Unmapped Columns in S2T Matrix** | Target column name does not match source column name and no custom rule was supplied. | Use the Business Rules text area in `app.py` to specify explicit alias rules (e.g., `MAP SRC_ID -> TARGET_ID`). |

---

## 5. Automated Health & Verification Task

To maintain documentation accuracy and code reliability over time, a recurring background automation can be scheduled using the Antigravity cron engine:

* **Recommended Cadence**: Daily at 9:00 AM (`CRON_TZ=Africa/Cairo 0 9 * * *`).
* **Automated Actions**:
  1. Parse all modified Python modules (`app.py`, `gap_analysis.py`, `etl_generator.py`, `powercenter_xml_generator.py`).
  2. Verify that all public functions, classes, and transformation handlers contain valid docstrings and type annotations.
  3. Execute automated pipeline generation on sample DDLs and assert XML validity using `xml.etree.ElementTree`.
  4. Generate a daily diagnostic status log.

---
*Generated by Antigravity Pairing Assistant for Dynamic ETL Studio.*
