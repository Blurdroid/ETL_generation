# Dynamic ETL Studio & Informatica PowerCenter Accelerator

**Author**: Ziad Alashry  
**Platform**: Python 3.10+, Streamlit, Informatica PowerCenter  

---

## Overview

**Dynamic ETL Studio** is an enterprise data engineering accelerator designed for high-complexity financial data warehouses. It bridges requirement specifications (RSDs), enterprise lineage catalogs (200+ mappings, 100+ sessions), and Informatica PowerCenter production assets.

It allows you to:
1. **Explain & Trace Complex Workflows**: Parse multi-mapping, multi-session enterprise workflows (e.g. OFSAA AFU) with end-to-end column-level lineage and zero hallucinations.
2. **Build Enterprise-Grade ETL Pipelines**: Generate turnkey, production-grade Informatica PowerCenter XML pipelines with multi-source branches, Joiners (Master/Detail ports), Routers (multi-group conditional routing), Unions (Custom Transformation templates), Sorters, Sequences, and Expressions.
3. **100% Guaranteed PowerCenter Import**: Automated pre-flight validation against Informatica DTD (`powrmart.dtd`) and fixed-precision datatype rules ensures XML files import into PowerCenter Designer and Workflow Manager without errors.
4. **Automated Requirements Gap Analysis**: Ingests Word RSDs and cross-references source and target catalogs to uncover unmapped columns, dropped fields, and formula rules.

---

## Key Modules

- `app.py`: Streamlit multi-page interface (Workflow Explainer, ETL Studio, Knowledge Hub, Gap Analysis).
- `powercenter_xml_generator.py`: XML generator producing 100% compliant `powrmart.dtd` PowerCenter packages with complete DOCTYPE, Sources, Targets, Transformations, Instances, Connectors, Sessions, and Workflows.
- `etl_generator.py`: AI-driven pipeline synthesis with self-healing schema normalization and enterprise preset loaders.
- `gap_analysis.py`: RSD Word document table extraction and multi-catalogue reconciliation.
- `powercenter_agent.py`: PowerCenter deployment automation and Ollama model resolution.

---

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/Blurdroid/ETL_generation.git
cd ETL_generation
```

### 2. Install Dependencies
```bash
pip install -r requirement.txt
```

### 3. Run the Application
```bash
python -m streamlit run app.py
```

---

## Enterprise Complexity: OFSAA AFU Topology

Dynamic ETL Studio natively generates and parses enterprise banking workflows matching the complexity of **OFSAA AFU**:
- **6 Diverse Sources**: Deposits, Consumer Loans, AFU Daily Transactions, Product Map, Trans Map, Customer Demographics.
- **5 Sorters**: High-performance key sorting for sorted joiner input.
- **5 Joiners**: Master/Detail outer joins (`INPUT/OUTPUT/MASTER` ports).
- **Conditional Router**: Split routing into Credit Card, Overdraft, Loan, and Default streams.
- **Union Transformation**: C-Language Custom Transformation (`pmuniontrans`) unifying multi-branch data flows.
- **Sequence Generator**: Surrogate key generation (`NEXTVAL` / `CURRVAL`).
- **Turn-key Execution Engine**: Automated `CONFIG`, `SESSION`, and `WORKFLOW` definitions.
