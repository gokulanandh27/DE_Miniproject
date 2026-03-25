# Automobile Service Center — Data Engineering Project
## Architecture & Pipeline Documentation

---

## 1. Project Overview

This project implements a **Medallion Architecture** (Bronze → Silver → Gold) for an automobile service center chain. The pipeline ingests raw data from Azure Blob Storage via Fivetran, lands it into a Bronze layer as-is, then cleans, transforms, and validates it into a Silver layer ready for Gold-layer analytics.

**Platform**: Databricks (AWS) with Unity Catalog  
**Compute**: Serverless Interactive Cluster  
**Storage Format**: Delta Lake  
**Catalog**: `workspace`  
**Ingestion Tool**: Fivetran (Azure Blob Storage connector)  

---

## 2. Architecture Diagram

```
┌─────────────────────┐
│   Azure Blob Storage │
│   (CSV source files) │
└──────────┬──────────┘
           │ Fivetran Connector
           ▼
┌─────────────────────────────────────┐
│  RAW LAYER (Fivetran-managed)       │
│  Schema: workspace.azure_blob_storage│
│  Tables: customer_survey, estimate,  │
│  invoice, ns_budget, order, store,   │
│  metadata                            │
│  (+ _file, _line, _modified,         │
│    _fivetran_synced metadata cols)    │
└──────────┬──────────────────────────┘
           │ Bronze Pipeline Notebook
           ▼
┌─────────────────────────────────────┐
│  BRONZE LAYER (Raw Copy)             │
│  Schema: workspace.bronze            │
│  Tables: customer_survey, estimate,  │
│  invoice, ns_budget, order, store    │
│  Format: Delta (overwrite mode)      │
│  No transformations applied          │
└──────────┬──────────────────────────┘
           │ Silver Pipeline Notebook
           ▼
┌─────────────────────────────────────┐
│  SILVER LAYER (Cleaned & Validated)  │
│  Schema: workspace.silver            │
│  Tables: customer_survey, estimate,  │
│  invoice, ns_budget, order, store    │
│  Format: Delta (overwrite + schema)  │
│  Transformations: type casting,      │
│  metadata removal, null imputation,  │
│  deduplication, referential fixes    │
└──────────┬──────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│  GOLD LAYER (Analytics-Ready)        │
│  Schema: workspace.gold (planned)    │
│  Aggregated business metrics,        │
│  joined fact/dimension tables        │
└─────────────────────────────────────┘
```

---

## 3. Data Source — Fivetran Ingestion

### 3.1 How Fivetran Works in This Project

Fivetran connects to **Azure Blob Storage** and syncs CSV files into Databricks Unity Catalog as managed tables. It automatically:
- Detects file schema and creates tables
- Adds metadata columns to every table:
  - `_file` (STRING): Source file name
  - `_line` (BIGINT): Line number in the source file
  - `_modified` (TIMESTAMP): File modification timestamp
  - `_fivetran_synced` (TIMESTAMP): When Fivetran last synced the row

### 3.2 Metadata Table

Fivetran syncs a `metadata.csv` that acts as a registry of all source tables:

| _file        | _line | table_name      | schema |
|-------------|-------|-----------------|--------|
| metadata.csv | 0     | customer_survey | raw    |
| metadata.csv | 1     | estimate        | raw    |
| metadata.csv | 2     | invoice         | raw    |
| metadata.csv | 3     | ns_budget       | raw    |
| metadata.csv | 4     | order           | raw    |
| metadata.csv | 5     | store           | raw    |

### 3.3 Source Tables (workspace.azure_blob_storage)

All 6 source tables land here with their original columns plus the 4 Fivetran metadata columns.

---

## 4. Bronze Layer

### 4.1 Purpose
The Bronze layer is a **raw, exact copy** of the Fivetran-managed tables in Delta format. No transformations are applied. This serves as a reliable, versioned snapshot that the Silver layer reads from.

### 4.2 Pipeline Logic
- **Notebook**: `Bronze Pipeline Setup`
- **Location**: `/DE_Miniproject_Automobile/01_bronze/01_raw/02_bronze_pipeline/`
- **Strategy**: Reads the metadata table, iterates over each `table_name`, reads from `workspace.azure_blob_storage.<table>`, and writes to `workspace.bronze.<table>` using Delta format with overwrite mode.

### 4.3 Bronze Table Inventory

| Table | Row Count | Columns (including metadata) |
|-------|-----------|-----|
| store | 50 | 12 (4 metadata + 8 business) |
| order | 170,273 | 23 (4 metadata + 19 business) |
| customer_survey | 124,032 | 14 (4 metadata + 10 business) |
| estimate | 425,103 | 13 (4 metadata + 9 business) |
| invoice | 186,039 | 10 (4 metadata + 6 business) |
| ns_budget | 1,212 | 8 (4 metadata + 4 business) |

---

## 5. Silver Layer

### 5.1 Purpose
The Silver layer applies **cleaning, type casting, deduplication, null handling, and data quality fixes** to make the data analytics-ready.

### 5.2 Pipeline Details
- **Notebook**: `silver pipeline setup`
- **Location**: `/DE_Miniproject_Automobile/02_silver/01_transformation/01_silver_notebook/`
- **Common Operations** (applied to all tables):
  1. Column name normalization (lowercase, spaces → underscores)
  2. Drop Fivetran metadata columns (`_file`, `_line`, `_modified`, `_fivetran_synced`)
  3. Type casting to appropriate data types
  4. Deduplication via `dropDuplicates()`
  5. Write to `workspace.silver.<table>` with `overwriteSchema=true`

### 5.3 Silver Table Schemas

#### 5.3.1 silver.store (50 rows, 8 columns)
| Column | Type | Notes |
|--------|------|-------|
| store_id | INT | Primary key, cast from BIGINT |
| store_name | STRING | Service center name |
| city | STRING | City location |
| state | STRING | US state abbreviation |
| manager_id | STRING | Foreign key to manager |
| manager_name | STRING | Backfilled via manager_id lookup |
| opened_year | INT | Year store opened, cast from BIGINT |
| store_type | STRING | OWNED or FRANCHISE |

**Special cleaning**: Store 11 had null `manager_name` but valid `manager_id` (MGR012). Backfilled with "Samuel Andrade" by looking up other stores with the same manager_id.

#### 5.3.2 silver.order (170,273 rows, 19 columns)
| Column | Type | Notes |
|--------|------|-------|
| order_id | STRING | Primary key (e.g., RO000000014) |
| store_id | INT | FK to store, cast from BIGINT |
| technician_id | STRING | Nullable (3.93% null — unassigned) |
| technician_name | STRING | Technician full name |
| service_type | STRING | PAINT, MECHANICAL, INSPECTION, BODY |
| vehicle_no | STRING | License plate number |
| vehicle_make | STRING | Honda, BMW, Toyota, Ford, Audi, Hyundai, Kia |
| vehicle_model | STRING | 0.1% null |
| vehicle_in_datetime | TIMESTAMP | When vehicle arrived |
| vehicle_out_datetime | TIMESTAMP | 27% null (OPEN/IN_PROGRESS orders) |
| planned_work_start_datetime | TIMESTAMP | Scheduled start |
| actual_work_start_datetime | TIMESTAMP | 10% null (OPEN orders) |
| planned_completion_datetime | TIMESTAMP | Scheduled completion |
| actual_completion_datetime | TIMESTAMP | 19% null (OPEN + some IN_PROGRESS) |
| promised_delivery_datetime | TIMESTAMP | 3% null |
| actual_delivery_datetime | TIMESTAMP | 27% null (OPEN/IN_PROGRESS orders) |
| order_status | STRING | COMPLETED (73%), IN_PROGRESS (17%), OPEN (10%) |
| customer_name | STRING | Customer full name |
| customer_phone | STRING | Phone number (various formats) |

**Null pattern**: Timestamp nulls align with order status — OPEN orders have no actuals, IN_PROGRESS have partial actuals. This is structurally expected.

#### 5.3.3 silver.customer_survey (124,032 rows, 10 columns)
| Column | Type | Notes |
|--------|------|-------|
| survey_id | STRING | Primary key (e.g., SRV000000015) |
| order_id | STRING | FK to order |
| survey_sent_date | TIMESTAMP | When survey was sent |
| survey_response_date | TIMESTAMP | 40% null (non-respondents) |
| responded_flag | BOOLEAN | Fixed: 0 nulls (was 3,700) |
| delivered_on_time_rating | INT | 0-1 scale, imputed for non-respondents |
| work_quality_rating | INT | 1-10 scale, imputed for non-respondents |
| cleanliness_rating | INT | 1-10 scale, imputed for non-respondents |
| communication_rating | INT | 1-10 scale, imputed for non-respondents |
| overall_satisfaction_rating | INT | 1-10 scale, imputed for non-respondents |

**Special cleaning**:
1. **responded_flag fix**: 3,700 null flags → set to `true` where `survey_response_date` exists, `false` otherwise
2. **Rating imputation**: Null ratings (non-respondents) filled with **store-level averages** (joined via order → store_id). Fallback to **global averages** for stores with zero responses. The `responded_flag` is preserved so downstream can distinguish real vs imputed.

#### 5.3.4 silver.estimate (425,103 rows, 9 columns)
| Column | Type | Notes |
|--------|------|-------|
| estimate_id | STRING | Primary key (e.g., EST000000025) |
| order_id | STRING | FK to order |
| version_no | INT | Estimate revision number, cast from BIGINT |
| estimate_amount | DOUBLE | 3% null, range: 3,000 - 35,893 |
| created_at | TIMESTAMP | Estimate creation time |
| created_by | STRING | Creator name |
| estimate_type | STRING | MIXED (33%), LABOUR (33%), PARTS (33%) |
| estimator_id | STRING | FK to estimator |
| estimator_name | STRING | Estimator full name |

#### 5.3.5 silver.invoice (186,039 rows, 6 columns)
| Column | Type | Notes |
|--------|------|-------|
| invoice_id | STRING | Primary key (e.g., INV000000012) |
| order_id | STRING | FK to order |
| invoice_date | TIMESTAMP | Invoice creation date |
| invoice_amount | DOUBLE | Range: 1,500 - 35,893, cast from BIGINT |
| payment_mode | STRING | CARD, NETBANKING, CASH, UPI (5% null) |
| currency | STRING | 100% INR |

#### 5.3.6 silver.ns_budget (1,212 rows, 4 columns)
| Column | Type | Notes |
|--------|------|-------|
| ns_store_id | INT | FK to store, cast from BIGINT |
| month | STRING | Format: YYYY-MM (e.g., 2024-01) |
| budget_amount | DOUBLE | Range: 800,000 - 2,550,000, cast from BIGINT |
| approved_by | STRING | Approver name |

---

## 6. Data Quality Audit Results

### 6.1 Row Count Validation (Bronze = Silver)
| Table | Bronze | Silver | Status |
|-------|--------|--------|--------|
| store | 50 | 50 | ✓ MATCH |
| order | 170,273 | 170,273 | ✓ MATCH |
| customer_survey | 124,032 | 124,032 | ✓ MATCH |
| estimate | 425,103 | 425,103 | ✓ MATCH |
| invoice | 186,039 | 186,039 | ✓ MATCH |
| ns_budget | 1,212 | 1,212 | ✓ MATCH |

### 6.2 Referential Integrity
| Check | Result |
|-------|--------|
| order.store_id → store.store_id | ✓ 0 orphans |
| customer_survey.order_id → order.order_id | ✓ 0 orphans |
| estimate.order_id → order.order_id | ✓ 0 orphans |
| invoice.order_id → order.order_id | ✓ 0 orphans |
| ns_budget.ns_store_id → store.store_id | ✓ 0 orphans |

### 6.3 Primary Key Uniqueness
| Table | Primary Key | Result |
|-------|-------------|--------|
| store | store_id | ✓ Unique |
| order | order_id | ✓ Unique |
| customer_survey | survey_id | ✓ Unique |
| estimate | estimate_id | ✓ Unique |
| invoice | invoice_id | ✓ Unique |

### 6.4 Must-Fix Issues (Applied)
| Issue | Table | Fix Applied |
|-------|-------|-------------|
| Null manager_name (store 11) | store | Backfilled "Samuel Andrade" via MGR012 lookup |
| 3,700 null responded_flag | customer_survey | Set true/false based on survey_response_date |
| Null rating columns | customer_survey | Imputed with store-level averages, global fallback |

### 6.5 Known Remaining Issues (Low Severity)
| Issue | Table | Count | % | Notes |
|-------|-------|-------|---|-------|
| Null payment_mode | invoice | 9,239 | 4.97% | Could fill with "UNKNOWN" |
| Null estimate_amount | estimate | 12,787 | 3.01% | Could fill with median per type |
| Null vehicle_model | order | 168 | 0.1% | Minor |
| Illogical timestamp | order | 1 | <0.01% | RO000025110: delivery before vehicle-in |
| Null technician_id | order | 6,684 | 3.93% | Unassigned orders |

---

## 7. Entity Relationship Model

```
┌─────────────┐     1:N     ┌──────────────┐     1:N     ┌────────────────┐
│   store      │◄───────────│    order      │◄───────────│ customer_survey │
│             │             │              │             │                │
│ store_id PK │             │ order_id PK  │             │ survey_id PK   │
│ store_name  │             │ store_id FK  │             │ order_id FK    │
│ city        │             │ service_type │             │ ratings (5)    │
│ state       │             │ order_status │             │ responded_flag │
│ manager_*   │             │ timestamps(8)│             └────────────────┘
│ opened_year │             │ customer_*   │
│ store_type  │             │ vehicle_*    │     1:N     ┌──────────────┐
└─────────────┘             │ technician_* │◄───────────│   estimate    │
      ▲                     └──────────────┘             │              │
      │ 1:N                        ▲                     │ estimate_id  │
┌─────────────┐                    │ 1:N                 │ order_id FK  │
│  ns_budget   │             ┌──────────────┐             │ amount       │
│             │             │   invoice     │             │ type         │
│ ns_store_id │             │              │             └──────────────┘
│ month       │             │ invoice_id PK│
│ budget_amt  │             │ order_id FK  │
│ approved_by │             │ amount       │
└─────────────┘             │ payment_mode │
                            └──────────────┘
```

---

## 8. Key Design Decisions

| Decision | Rationale |
|----------|----------|
| Read from bronze tables instead of DBFS CSV paths | Serverless compute disables public DBFS root. Bronze Delta tables provide reliable, versioned access. |
| overwriteSchema = true | Silver tables may have different schemas from prior runs (e.g., after dropping metadata columns). |
| Store-level avg for rating imputation | Stores have consistent service patterns. More granular than global avg, more stable than order-level. |
| Preserve responded_flag | Allows downstream to filter real vs imputed ratings. |
| Metadata-driven bronze ingestion | Adding a new source table only requires a metadata.csv update — no code changes. |
| dropDuplicates() on all tables | Defensive measure — source data may have duplicates from Fivetran re-syncs. |

---

## 9. File & Notebook Locations

| Asset | Path |
|-------|------|
| Bronze Pipeline | `/DE_Miniproject_Automobile/01_bronze/01_raw/02_bronze_pipeline/Bronze Pipeline Setup` |
| Silver Pipeline | `/DE_Miniproject_Automobile/02_silver/01_transformation/01_silver_notebook/silver pipeline setup` |
| This Documentation | `/DE_Miniproject_Automobile/docs/01_Architecture_and_Pipeline_Overview.md` |
| Code Reference | `/DE_Miniproject_Automobile/docs/02_Code_Reference_and_Explanation.md` |

---

## 10. How to Run the Pipeline

1. **Ensure Fivetran sync is complete** — check `workspace.azure_blob_storage.metadata` for latest `_fivetran_synced` timestamp
2. **Run Bronze Pipeline** — execute all cells in `Bronze Pipeline Setup` notebook (creates `workspace.bronze.*` tables)
3. **Run Silver Pipeline** — execute all cells in `silver pipeline setup` notebook sequentially (Cell 1 → Cell 7)
4. **Verify** — check row counts, null patterns, and referential integrity as documented in Section 6

---

*Document generated: March 25, 2026*  
*Project: DE_Miniproject_Automobile*