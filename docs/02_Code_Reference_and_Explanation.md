# Automobile Service Center — Code Reference & Explanation
## Detailed Code Documentation for Bronze & Silver Pipelines

---

## Table of Contents

1. [Bronze Pipeline — Bronze Pipeline Setup Notebook](#1-bronze-pipeline)
2. [Silver Pipeline — silver pipeline setup Notebook](#2-silver-pipeline)
   - [Cell 1: Common Cleaning Functions](#cell-1-common-cleaning-functions)
   - [Cell 2: Silver — Store](#cell-2-silver--store)
   - [Cell 3: Silver — Order](#cell-3-silver--order)
   - [Cell 4: Silver — Customer Survey](#cell-4-silver--customer-survey)
   - [Cell 5: Silver — Estimate](#cell-5-silver--estimate)
   - [Cell 6: Silver — Invoice](#cell-6-silver--invoice)
   - [Cell 7: Silver — NS Budget](#cell-7-silver--ns-budget)
3. [Frequently Asked Questions](#3-frequently-asked-questions)

---

## 1. Bronze Pipeline

**Notebook**: `Bronze Pipeline Setup`  
**Path**: `/DE_Miniproject_Automobile/01_bronze/01_raw/02_bronze_pipeline/`  
**Purpose**: Copy raw Fivetran tables from `workspace.azure_blob_storage` to `workspace.bronze` as Delta tables.

### Cell 1: Create Bronze Schema

```sql
%sql
CREATE SCHEMA IF NOT EXISTS bronze;
```

**Explanation**:
- `CREATE SCHEMA IF NOT EXISTS bronze` — Creates the `bronze` schema inside the current catalog (`workspace`) if it doesn't already exist.
- `IF NOT EXISTS` — Prevents errors on re-runs. This is an **idempotent** operation.
- The `%sql` magic command tells Databricks to interpret this cell as SQL even though the notebook default language may be Python.

---

### Cell 2: Read Metadata Table

```python
metadata_df = spark.table("workspace.azure_blob_storage.metadata")
display(metadata_df)
```

**Line-by-line**:
- `spark.table("workspace.azure_blob_storage.metadata")` — Reads the `metadata` table from the Fivetran-managed schema. This table is a registry of all source CSV files. `spark.table()` returns a DataFrame by reading a Unity Catalog table.
- `display(metadata_df)` — Renders the DataFrame as an interactive table in the notebook. Shows 6 rows: one per source table (customer_survey, estimate, invoice, ns_budget, order, store).

**Output columns**: `_file`, `_line`, `_modified`, `_fivetran_synced`, `table_name`, `schema`

---

### Cell 3: Metadata-Driven Bronze Ingestion

```python
catalog = "workspace"
source_schema = "azure_blob_storage"
bronze_schema = "bronze"

metadata_df = spark.table(f"{catalog}.{source_schema}.metadata")
display(metadata_df)

for row in metadata_df.collect():
    table_name = row["table_name"]
    source_table = f"{catalog}.{source_schema}.{table_name}"
    print(f"Reading source table: {source_table}")
    
    df = spark.table(source_table)
    bronze_table = f"{catalog}.{bronze_schema}.{table_name}"
    df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
    
    print(f"Bronze table created: {bronze_table}")

print("Bronze ingestion completed successfully!")
```

**Line-by-line**:

| Line | Code | Explanation |
|------|------|-------------|
| 1-3 | `catalog = "workspace"` ... | Define constants for the 3-level namespace: catalog, source schema, target schema. |
| 5 | `spark.table(f"{catalog}.{source_schema}.metadata")` | Re-read the metadata table using fully qualified name (`workspace.azure_blob_storage.metadata`). |
| 6 | `display(metadata_df)` | Show the metadata to verify what tables will be processed. |
| 8 | `for row in metadata_df.collect():` | `collect()` brings all rows to the driver as a list of Row objects. Since metadata has only 6 rows, this is safe. **Caution**: Never use `collect()` on large DataFrames. |
| 9 | `table_name = row["table_name"]` | Extract the `table_name` field from each metadata row (e.g., "customer_survey", "store"). |
| 10-11 | `source_table = ...` | Build the fully qualified source table name (e.g., `workspace.azure_blob_storage.customer_survey`). |
| 13 | `df = spark.table(source_table)` | Read the entire source table into a DataFrame. No filters or transformations. |
| 14-15 | `bronze_table = ...` | Build the target table name (e.g., `workspace.bronze.customer_survey`). |
| 16 | `df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)` | Write the DataFrame as a managed Delta table. `mode("overwrite")` replaces all existing data. `format("delta")` ensures Delta Lake format for ACID transactions, time travel, and schema enforcement. |

**Key concept — Metadata-driven ingestion**: Instead of hardcoding table names, this loop dynamically reads from a metadata registry. Adding a new source table only requires updating `metadata.csv` — no code changes needed.

---

## 2. Silver Pipeline

**Notebook**: `silver pipeline setup`  
**Path**: `/DE_Miniproject_Automobile/02_silver/01_transformation/01_silver_notebook/`  
**Purpose**: Clean, transform, validate, and write analytics-ready tables to `workspace.silver`.

---

### Cell 1: Common Cleaning Functions

```python
from pyspark.sql import functions as F
from pyspark.sql import types as T

def normalize_columns(df):
    """Standardizes column names."""
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))
    return df

def cast_timestamp(df, col):
    return df.withColumn(col, F.to_timestamp(F.col(col)))

def cast_float(df, col):
    return df.withColumn(col, F.col(col).cast("double"))

def cast_int(df, col):
    return df.withColumn(col, F.col(col).cast("int"))
```

**Line-by-line**:

| Line | Code | Explanation |
|------|------|-------------|
| 1 | `from pyspark.sql import functions as F` | Import PySpark SQL functions module. Convention: alias as `F`. Provides `col()`, `when()`, `avg()`, `coalesce()`, `round()`, `lit()`, etc. |
| 2 | `from pyspark.sql import types as T` | Import PySpark data types (StringType, IntegerType, etc.). Used for explicit schema definitions. |
| 4-7 | `def normalize_columns(df):` | **Column normalization function**. Iterates over all column names, applies: `strip()` (remove leading/trailing whitespace), `lower()` (convert to lowercase), `replace(" ", "_")` (spaces to underscores). Returns a DataFrame with cleaned column names. Example: "Store ID" → "store_id". |
| 9-10 | `def cast_timestamp(df, col):` | Helper to cast a column to TIMESTAMP type using `F.to_timestamp()`. Handles various date/time string formats automatically. |
| 12-13 | `def cast_float(df, col):` | Helper to cast a column to DOUBLE (64-bit floating point). Used for monetary amounts. |
| 15-16 | `def cast_int(df, col):` | Helper to cast a column to INT (32-bit integer). Used for IDs and counts. |

**Why define utility functions?** DRY principle (Don't Repeat Yourself). Each silver table needs the same operations, so centralizing them avoids code duplication and ensures consistency.

---

### Cell 2: Silver — Store

```python
df_store = spark.table("workspace.bronze.store")
df_store = normalize_columns(df_store)

df_store = (
    df_store
    .drop("_file", "_line", "_modified", "_fivetran_synced")
    .withColumn("store_id", F.col("store_id").cast("int"))
    .withColumn("opened_year", F.col("opened_year").cast("int"))
    .dropDuplicates()
)

# Backfill null manager_name using manager_id
manager_lookup = (
    df_store.filter(F.col("manager_name").isNotNull())
    .select("manager_id", F.col("manager_name").alias("manager_name_lookup")).distinct()
)
df_store = (
    df_store
    .join(manager_lookup, on="manager_id", how="left")
    .withColumn("manager_name", F.coalesce(F.col("manager_name"), F.col("manager_name_lookup")))
    .drop("manager_name_lookup")
)

df_store.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("workspace.silver.store")
```

**Line-by-line**:

| Line | Code | Explanation |
|------|------|-------------|
| 1 | `spark.table("workspace.bronze.store")` | Read the bronze store table. Originally tried reading from `/mnt/raw/store.csv` but DBFS is disabled on serverless compute, so we read from the bronze Delta table instead. |
| 2 | `normalize_columns(df_store)` | Apply column name standardization. Bronze columns are already lowercase but this ensures consistency. |
| 6 | `.drop("_file", "_line", "_modified", "_fivetran_synced")` | Remove Fivetran metadata columns that are not needed for analytics. These track ingestion lineage, not business data. |
| 7 | `.withColumn("store_id", F.col("store_id").cast("int"))` | Cast `store_id` from BIGINT to INT. Store IDs are small numbers (1-50), INT is sufficient and more efficient. |
| 8 | `.withColumn("opened_year", F.col("opened_year").cast("int"))` | Cast `opened_year` from BIGINT to INT. Year values (2015-2024) fit comfortably in INT. |
| 9 | `.dropDuplicates()` | Remove any duplicate rows. Defensive measure against Fivetran re-syncs that may create duplicates. |
| 13-16 | `manager_lookup = ...` | **Manager name backfill logic**: Create a lookup DataFrame from stores that DO have a manager_name. Filter out nulls, select `manager_id` and rename `manager_name` to `manager_name_lookup` to avoid column ambiguity during join. `.distinct()` ensures one name per manager_id. |
| 17-21 | `df_store.join(manager_lookup, ...)` | Left join the store table with the lookup on `manager_id`. `F.coalesce()` takes the first non-null value: if original `manager_name` exists, keep it; otherwise use the lookup value. Finally drop the temporary `manager_name_lookup` column. |
| 24 | `.write.mode("overwrite").option("overwriteSchema", "true")...` | Write to silver. `overwriteSchema=true` is critical: it forces Delta to replace the table schema (needed when we've dropped columns from a previous wider schema). |

**Data quality fix**: Store 11 had `manager_id` = MGR012 but null `manager_name`. Three other stores (2, 37, 38) with the same MGR012 had "Samuel Andrade". The lookup join fills this gap automatically.

---

### Cell 3: Silver — Order

```python
df_order = spark.table("workspace.bronze.order")
df_order = normalize_columns(df_order)

df_order = (
    df_order
    .drop("_file", "_line", "_modified", "_fivetran_synced")
    .withColumn("store_id", F.col("store_id").cast("int"))
    .dropDuplicates()
)

df_order.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("workspace.silver.order")
```

**Line-by-line**:

| Line | Code | Explanation |
|------|------|-------------|
| 1 | `spark.table("workspace.bronze.order")` | Read the order table (170,273 rows, 23 columns in bronze). |
| 2 | `normalize_columns(df_order)` | Standardize column names. |
| 6 | `.drop(...)` | Remove 4 Fivetran metadata columns. |
| 7 | `.withColumn("store_id", F.col("store_id").cast("int"))` | Cast `store_id` to INT to match the store table's primary key type. Ensures consistent join types downstream. |
| 8 | `.dropDuplicates()` | Remove any duplicate rows. |
| 11 | `.write...saveAsTable("workspace.silver.order")` | Write 170,273 rows with 19 columns to silver. |

**Note on timestamp columns**: The 8 timestamp columns (vehicle_in/out, planned/actual work start/completion, promised/actual delivery) are already TIMESTAMP type from bronze. No casting needed. Null timestamps are structurally expected for OPEN and IN_PROGRESS orders.

---

### Cell 4: Silver — Customer Survey

This is the most complex transformation cell. It handles:
1. Type casting for 5 rating columns
2. Fixing null `responded_flag`
3. Imputing null ratings using store-level averages

```python
df_survey = spark.table("workspace.bronze.customer_survey")
df_survey = normalize_columns(df_survey)

df_survey = (
    df_survey
    .drop("_file", "_line", "_modified", "_fivetran_synced")
    .withColumn("delivered_on_time_rating", F.col("delivered_on_time_rating").cast("int"))
    .withColumn("work_quality_rating", F.col("work_quality_rating").cast("int"))
    .withColumn("cleanliness_rating", F.col("cleanliness_rating").cast("int"))
    .withColumn("communication_rating", F.col("communication_rating").cast("int"))
    .withColumn("overall_satisfaction_rating", F.col("overall_satisfaction_rating").cast("int"))
    .dropDuplicates()
)
```

**Part 1 — Basic cleaning**: Drop metadata, cast all 5 rating columns from DOUBLE to INT (ratings are whole numbers on 0-10 scale), and remove duplicates.

```python
# Fix responded_flag nulls: true if response_date exists, false otherwise
df_survey = df_survey.withColumn(
    "responded_flag",
    F.when(F.col("responded_flag").isNull() & F.col("survey_response_date").isNotNull(), F.lit(True))
    .when(F.col("responded_flag").isNull() & F.col("survey_response_date").isNull(), F.lit(False))
    .otherwise(F.col("responded_flag"))
)
```

**Part 2 — Fix responded_flag**: 
- `F.when(...condition..., value)` — PySpark's equivalent of SQL CASE WHEN.
- **Rule 1**: If flag is null AND response_date exists → customer responded → set `True`
- **Rule 2**: If flag is null AND response_date is null → customer didn't respond → set `False`
- **Rule 3**: `.otherwise()` → keep original value if flag was already set
- `F.lit(True)` / `F.lit(False)` — Creates a literal (constant) column value.
- This fixed 3,700 null flags: 2,175 set to True, 1,525 set to False.

```python
rating_cols = [
    "delivered_on_time_rating", "work_quality_rating",
    "cleanliness_rating", "communication_rating", "overall_satisfaction_rating"
]

# Join with order table to get store_id
df_order = spark.table("workspace.silver.order").select("order_id", "store_id")
df_survey = df_survey.join(df_order, on="order_id", how="left")
```

**Part 3 — Get store_id**: The survey table doesn't have `store_id` directly. We need it to compute store-level averages. So we join with the order table (which has `store_id`) using `order_id` as the join key. `how="left"` ensures we keep all survey rows even if an order match isn't found.

```python
# Compute store-level average ratings (from responded surveys only)
store_avg = (
    df_survey.filter(F.col("responded_flag") == True)
    .groupBy("store_id")
    .agg(*[F.round(F.avg(c)).cast("int").alias(f"{c}_avg") for c in rating_cols])
)
```

**Part 4 — Store-level averages**:
- `filter(F.col("responded_flag") == True)` — Only use actual responses for computing averages (exclude non-respondents and imputed values).
- `groupBy("store_id")` — Group by store to get per-store averages.
- `F.avg(c)` — Compute the mean of each rating column.
- `F.round(...)` — Round to nearest integer.
- `.cast("int")` — Convert to INT (round returns DOUBLE).
- `.alias(f"{c}_avg")` — Name the column, e.g., `work_quality_rating_avg`.
- The `*[... for c in rating_cols]` unpacks a list comprehension into separate arguments to `.agg()`.

```python
# Compute global averages as fallback
global_avg = (
    df_survey.filter(F.col("responded_flag") == True)
    .agg(*[F.round(F.avg(c)).cast("int").alias(f"{c}_global") for c in rating_cols])
)
```

**Part 5 — Global fallback**: Same logic but without `groupBy` — computes a single row of global averages across all stores. Used when a store has zero responses (no store-level average available).

```python
# Join store averages and fill nulls
df_survey = df_survey.join(store_avg, on="store_id", how="left")
df_survey = df_survey.crossJoin(global_avg)

for c in rating_cols:
    df_survey = df_survey.withColumn(
        c,
        F.coalesce(F.col(c), F.col(f"{c}_avg"), F.col(f"{c}_global"))
    )
```

**Part 6 — Apply imputation**:
- `join(store_avg, on="store_id", how="left")` — Attach store-level averages to each row.
- `crossJoin(global_avg)` — Attach global averages to every row (since `global_avg` is a single row, this is safe).
- `F.coalesce(original, store_avg, global_avg)` — **Priority-based null filling**:
  1. First try the original rating value
  2. If null, use the store-level average
  3. If still null (store had no responses), use the global average

```python
# Drop helper columns and store_id (not in original schema)
cols_to_drop = [f"{c}_avg" for c in rating_cols] + [f"{c}_global" for c in rating_cols] + ["store_id"]
df_survey = df_survey.drop(*cols_to_drop)

df_survey.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("workspace.silver.customer_survey")
```

**Part 7 — Cleanup and write**:
- Build a list of all temporary columns: 5 `_avg` + 5 `_global` + `store_id` = 11 columns to drop.
- `df_survey.drop(*cols_to_drop)` — The `*` unpacks the list as positional arguments.
- Write to silver with schema overwrite.

---

### Cell 5: Silver — Estimate

```python
df_estimate = spark.table("workspace.bronze.estimate")
df_estimate = normalize_columns(df_estimate)

df_estimate = (
    df_estimate
    .drop("_file", "_line", "_modified", "_fivetran_synced")
    .withColumn("version_no", F.col("version_no").cast("int"))
    .dropDuplicates()
)

df_estimate.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("workspace.silver.estimate")
```

**Line-by-line**:

| Line | Code | Explanation |
|------|------|-------------|
| 1-2 | Read + normalize | Standard pattern for all silver cells. |
| 6 | `.drop(...)` | Remove Fivetran metadata. |
| 7 | `.withColumn("version_no", ...)` | Cast `version_no` from BIGINT to INT. Estimate versions are small numbers (1-4). |
| 8 | `.dropDuplicates()` | Remove duplicates. |
| 11 | `.write...` | Write 425,103 rows with 9 columns to silver. |

**Note**: `estimate_amount` (DOUBLE) has 3% nulls (12,787 rows). These are left as-is because imputing monetary amounts requires business context. They could be filled with median per `estimate_type` in a gold-layer transformation.

---

### Cell 6: Silver — Invoice

```python
df_invoice = spark.table("workspace.bronze.invoice")
df_invoice = normalize_columns(df_invoice)

df_invoice = (
    df_invoice
    .drop("_file", "_line", "_modified", "_fivetran_synced")
    .withColumn("invoice_amount", F.col("invoice_amount").cast("double"))
    .dropDuplicates()
)

df_invoice.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("workspace.silver.invoice")
```

**Line-by-line**:

| Line | Code | Explanation |
|------|------|-------------|
| 7 | `.withColumn("invoice_amount", ... .cast("double"))` | Cast `invoice_amount` from BIGINT to DOUBLE. Although currently whole numbers (1,500 - 35,893), DOUBLE allows for future decimal amounts and is standard for monetary values. |

**Note**: `payment_mode` has 5% nulls (9,239 rows). Four valid values: CARD, NETBANKING, CASH, UPI. Nulls could be filled with "UNKNOWN" in the gold layer.

---

### Cell 7: Silver — NS Budget

```python
df_budget = spark.table("workspace.bronze.ns_budget")
df_budget = normalize_columns(df_budget)

df_budget = (
    df_budget
    .drop("_file", "_line", "_modified", "_fivetran_synced")
    .withColumn("ns_store_id", F.col("ns_store_id").cast("int"))
    .withColumn("budget_amount", F.col("budget_amount").cast("double"))
    .dropDuplicates()
)

df_budget.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("workspace.silver.ns_budget")
```

**Line-by-line**:

| Line | Code | Explanation |
|------|------|-------------|
| 7 | `.withColumn("ns_store_id", ... .cast("int"))` | Cast from BIGINT to INT. This is a foreign key to `store.store_id`. Matching types ensures efficient joins. |
| 8 | `.withColumn("budget_amount", ... .cast("double"))` | Cast from BIGINT to DOUBLE. Budget amounts range 800K–2.55M. DOUBLE provides flexibility for decimal budgets. |

**Note**: `month` is stored as STRING in "YYYY-MM" format (e.g., "2024-01"). It could be cast to DATE in the gold layer, but STRING is acceptable for grouping and filtering.

---

## 3. Frequently Asked Questions

### Q1: Why read from bronze tables instead of CSV files directly?
**A**: The project runs on Databricks **serverless compute**, which disables public DBFS root (`/mnt/` paths). The original code used `spark.read.csv("/mnt/raw/store.csv")` which threw `DBFS_DISABLED` error. Reading from bronze Delta tables is the correct approach on serverless.

### Q2: Why use `overwriteSchema = true`?
**A**: When writing with `mode("overwrite")` to an existing Delta table, Spark preserves the old table schema by default. If the silver table previously had 12 columns (with metadata) and we now write 8 columns, the old 4 columns remain as nulls. `overwriteSchema=true` forces Delta to replace the schema entirely.

### Q3: Why use `F.coalesce()` for imputation instead of `fillna()`?
**A**: `coalesce()` supports a **priority chain** — it checks multiple fallback columns in order (original → store avg → global avg). `fillna()` only supports a single static value. The tiered approach gives more accurate imputation.

### Q4: Why not impute estimate_amount and payment_mode nulls?
**A**: These require business context decisions:
- **estimate_amount**: Imputing with averages could distort cost analysis. Better handled in gold with domain knowledge.
- **payment_mode**: The null could mean "not yet paid" or "unknown method". Filling with "UNKNOWN" is safest but was left as optional.

### Q5: Why are order timestamps null for OPEN orders?
**A**: This is **structurally expected**. An OPEN order hasn't started work yet, so `actual_work_start_datetime`, `actual_completion_datetime`, `vehicle_out_datetime`, and `actual_delivery_datetime` are legitimately null. These are not data quality issues.

### Q6: What does `dropDuplicates()` do exactly?
**A**: It removes rows where **ALL columns** are identical. It's equivalent to `SELECT DISTINCT *` in SQL. Applied defensively because Fivetran re-syncs can sometimes create duplicate rows.

### Q7: How does the store-level rating imputation work?
**A**: 
1. Join survey with order table to get `store_id` for each survey
2. Compute average rating per store (only from actual responses)
3. For null ratings: fill with that store's average
4. If the store has zero responses (no average available): fill with global average across all stores
5. The `responded_flag` is preserved so downstream analytics can distinguish real vs. imputed ratings

### Q8: What is the `normalize_columns()` function doing?
**A**: It standardizes column names by:
- `strip()`: Removes leading/trailing whitespace (e.g., " Store ID " → "Store ID")
- `lower()`: Converts to lowercase (e.g., "Store ID" → "store id")
- `replace(" ", "_")`: Replaces spaces with underscores (e.g., "store id" → "store_id")

This ensures consistent, SQL-friendly column names across all tables.

### Q9: What is Delta Lake and why use it?
**A**: Delta Lake is an open-source storage layer that adds **ACID transactions**, **schema enforcement**, **time travel** (version history), and **efficient upserts** to data lakes. Every table in this project uses Delta format, providing reliability and data integrity.

### Q10: How to add a new source table to the pipeline?
**A**: 
1. Add a new row to `metadata.csv` in Azure Blob Storage with the new table name
2. Wait for Fivetran to sync it to `workspace.azure_blob_storage`
3. Re-run the Bronze Pipeline — it automatically picks up new tables from metadata
4. Add a new cell in the Silver Pipeline notebook with appropriate transformations

### Q11: What are the table relationships?
**A**: 
- `store.store_id` ← `order.store_id` (1:N — one store has many orders)
- `store.store_id` ← `ns_budget.ns_store_id` (1:N — one store has many budget entries)
- `order.order_id` ← `customer_survey.order_id` (1:1 — one survey per order)
- `order.order_id` ← `estimate.order_id` (1:N — one order can have multiple estimate versions)
- `order.order_id` ← `invoice.order_id` (1:1 — one invoice per completed order)

### Q12: What PySpark functions are used and what do they do?

| Function | Usage | Description |
|----------|-------|-------------|
| `F.col("name")` | Column reference | Creates a Column object for the given column name |
| `F.when(condition, value)` | Conditional logic | SQL CASE WHEN equivalent. Chain with `.when()` and `.otherwise()` |
| `F.lit(value)` | Literal value | Creates a constant column value |
| `F.coalesce(col1, col2, ...)` | Null handling | Returns the first non-null value from the list |
| `F.avg(col)` | Aggregation | Computes the mean of a column |
| `F.round(col)` | Rounding | Rounds to nearest integer (or specified decimal places) |
| `F.to_timestamp(col)` | Type conversion | Parses string to TIMESTAMP |
| `.cast("type")` | Type conversion | Converts column to specified type (int, double, etc.) |
| `.alias("name")` | Renaming | Renames a column in select/agg expressions |
| `.isNull()` | Null check | Returns true if the column value is null |
| `.isNotNull()` | Null check | Returns true if the column value is not null |
| `.filter()` / `.where()` | Row filtering | Keeps only rows matching the condition |
| `.groupBy()` | Grouping | Groups rows by specified columns for aggregation |
| `.join()` | Joining | Combines two DataFrames on a join condition |
| `.crossJoin()` | Cross join | Cartesian product (every row × every row). Safe when one side is a single row. |
| `.drop()` | Column removal | Removes specified columns from the DataFrame |
| `.dropDuplicates()` | Deduplication | Removes rows where all columns are identical |
| `.distinct()` | Deduplication | Same as dropDuplicates but typically used after select |
| `.collect()` | Action | Brings all rows to driver memory as a list. Use only for small DataFrames. |
| `display()` | Visualization | Databricks-specific function to render DataFrames as interactive tables |

---

*Document generated: March 25, 2026*  
*Project: DE_Miniproject_Automobile*