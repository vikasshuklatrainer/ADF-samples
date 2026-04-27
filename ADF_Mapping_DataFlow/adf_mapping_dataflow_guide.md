# Azure Data Factory — Mapping Data Flow Transformation Pipeline

---

## 📌 Scenario Overview

**Scenario: Raw Sales Data Transformation into Gold KPI Tables**

Three source systems (SQL DB, POS CSV, REST API) drop raw sales rows into `dbo.RawSales`. The data is messy — null IDs, zero-price rows, negative quantities, and inconsistent column names. The goal is to transform this raw data through six progressive steps using a **Mapping Data Flow** and land clean, enriched, aggregated Parquet files in ADLS Gold zone — split by revenue segment.

| Transformation | Purpose | Input rows → Output rows |
|---|---|---|
| ① Filter | Drop nulls, negatives, zero prices | 17 → 12 |
| ② Select & Rename | Standardise column names | 12 → 12 |
| ③ Derived Column | Compute LineTotal, SaleYear, MarginPct | 12 → 12 |
| ④ Lookup Join | Enrich with Category, Brand, UnitCost | 12 → 12 |
| ⑤ Aggregate | Group by Region + Category → KPI row | 12 → 8 |
| ⑥ Conditional Split | Route by TotalRevenue ≥ 1000 | 8 → 5 premium + 3 standard |

---

## 🏗️ Architecture

```
[dbo.RawSales]          [dbo.Products]
    │                        │
    ▼                        │ (Lookup Join in step ④)
①  Filter                   │
    │                        │
②  Select & Rename          │
    │                        │
③  Derived Column           │
    │                        │
④  Lookup Join ◄────────────┘
    │
⑤  Aggregate
    │
⑥  Conditional Split
    ├──► (TotalRevenue ≥ 1000) ──► gold/premium.parquet
    └──► (else)                ──► gold/standard.parquet
```

---

## ✅ Prerequisites

- **Azure Data Factory** (v2)
- **Azure SQL Database** — run `adf_mapping_dataflow.sql` first
- **Azure Data Lake Storage Gen2** — container: `gold`
- Linked Services: `LS_AzureSqlDB`, `LS_ADLS`
- Integration Runtime: **AutoResolveIntegrationRuntime** (default) or a custom Azure IR with at least 8 cores for Data Flow execution

---

## 🗄️ Step 1: Run the SQL Scripts

Execute `adf_mapping_dataflow.sql` in order:

| Part | What it creates |
|---|---|
| Part 1 | `dbo.Products` — 12 reference rows (Electronics, Clothing, Furniture) |
| Part 2 | `dbo.RawSales` — 12 good rows + 5 intentionally bad rows |
| Part 3 | SQL simulation query — verify expected output before building ADF |
| Part 4 | Row-level expected output — use as Data Preview comparison |

> 💡 Run Part 3 first to see what the final aggregated output should look like. Use this as your **acceptance test** when validating the Data Flow result.

---

## 🔗 Step 2: Create Datasets

### 2a. Source — dbo.RawSales
1. **Author** → **Datasets** → **+ New** → Azure SQL Database
2. Name: `DS_SQL_RawSales`
3. Linked Service: `LS_AzureSqlDB` | Table: `dbo.RawSales`
4. **Publish All**

### 2b. Lookup Reference — dbo.Products
1. **+ New** → Azure SQL Database
2. Name: `DS_SQL_Products`
3. Linked Service: `LS_AzureSqlDB` | Table: `dbo.Products`
4. **Publish All**

### 2c. Sink — Premium Parquet
1. **+ New** → Azure Data Lake Storage Gen2 → **Parquet**
2. Name: `DS_Gold_Premium`
3. Linked Service: `LS_ADLS`
4. Container: `gold` | Directory: `premium` | File: `premium_@{currentTimestamp()}.parquet`
5. **Publish All**

### 2d. Sink — Standard Parquet
1. **+ New** → Azure Data Lake Storage Gen2 → **Parquet**
2. Name: `DS_Gold_Standard`
3. Linked Service: `LS_ADLS`
4. Container: `gold` | Directory: `standard` | File: `standard_@{currentTimestamp()}.parquet`
5. **Publish All**

---

## 🌊 Step 3: Create the Mapping Data Flow

1. **Author** → **Data flows** → **+ New data flow**
2. Name: `DF_RawSales_Transformation`
3. Enable **Data flow debug** (uses a live Spark cluster — takes ~3 min to start)

> ⚠️ Always turn on debug mode before building a Data Flow. It enables **Data Preview** at each step so you can verify row counts and column values as you build.

---

## 🔴 Step 4: Add Source Transformations

### Source 1 — RawSales

1. Click **Add source** on the canvas
2. Name the stream: `SourceRawSales`
3. **Source settings:**
   - Dataset: `DS_SQL_RawSales`
   - Source options → **Query** (select all columns):
     ```sql
     SELECT * FROM dbo.RawSales
     ```
4. Click **Data Preview** → verify 17 rows load including bad rows

### Source 2 — Products (for Lookup)

1. Click **Add source** again (separate stream)
2. Name the stream: `SourceProducts`
3. Dataset: `DS_SQL_Products`
4. Click **Data Preview** → verify 12 product reference rows

---

## 🔻 Step 5: Filter Transformation

1. Click the **+** on `SourceRawSales` → select **Filter**
2. Name: `FilterBadRows`
3. **Filter on** → click **Add dynamic content** and enter:

```
!isNull(sale_id)
&& !isNull(qty)
&& qty > 0
&& !isNull(prod_id)
&& !isNull(unit_price)
&& unit_price > 0
```

4. Click **Data Preview** → verify **12 rows** remain (5 bad rows dropped)

**Rows removed by filter:**

| sale_id | Reason dropped |
|---|---|
| NULL | sale_id is null |
| 1013 | qty is null |
| 1014 | qty = -2 (negative) |
| 1015 | prod_id is null |
| 1016 | unit_price = 0 |

---

## 🔤 Step 6: Select & Rename Transformation

1. Click **+** on `FilterBadRows` → select **Select**
2. Name: `RenameColumns`
3. In the **Input columns** mapping, rename as follows:

| Source column | Output name |
|---|---|
| `sale_id` | `SaleId` |
| `cust_id` | `CustomerId` |
| `prod_id` | `ProductId` |
| `qty` | `Quantity` |
| `unit_price` | `UnitPrice` |
| `store_region` | `Region` |
| `sale_dt` | `SaleDate` |
| `source_system` | `SourceSystem` |

4. Click **Data Preview** → verify columns are renamed, still 12 rows

---

## 🧮 Step 7: Derived Column Transformation

1. Click **+** on `RenameColumns` → select **Derived column**
2. Name: `ComputeNewFields`
3. Add each new column using the **+ Add column** button:

| Column name | Expression |
|---|---|
| `LineTotal` | `Quantity * UnitPrice` |
| `SaleYear` | `year(toDate(SaleDate,'yyyy-MM-dd'))` |
| `SaleMonth` | `month(toDate(SaleDate,'yyyy-MM-dd'))` |
| `SaleDay` | `dayOfMonth(toDate(SaleDate,'yyyy-MM-dd'))` |
| `LoadDate` | `currentDate()` |

4. Click **Data Preview** → verify `LineTotal` is populated correctly:

| SaleId | Quantity | UnitPrice | LineTotal |
|---|---|---|---|
| 1001 | 1 | 1299.99 | 1299.99 |
| 1002 | 2 | 59.99 | 119.98 |
| 1005 | 3 | 98.00 | 294.00 |
| 1006 | 2 | 399.99 | 799.98 |
| 1011 | 2 | 849.00 | 1698.00 |

---

## 🔗 Step 8: Lookup Join Transformation

1. Click **+** on `ComputeNewFields` → select **Lookup**
2. Name: `JoinProducts`
3. **Lookup settings:**
   - Primary stream: `ComputeNewFields`
   - Lookup stream: **SourceProducts**
   - Lookup type: **Any matching row**
   - Match condition: `ProductId == ProductId`
4. **Output columns** — select from Products stream to add:
   - `ProductName`, `Brand`, `Category`, `SubCategory`, `UnitCost`

5. Add another **Derived Column** after the Lookup (name: `ComputeMargin`):

| Column name | Expression |
|---|---|
| `MarginPct` | `round((UnitPrice - UnitCost) / UnitPrice * 100, 2)` |
| `MarginAmount` | `UnitPrice - UnitCost` |

6. Click **Data Preview** → verify Category and Brand columns now populated:

| SaleId | ProductName | Category | MarginPct |
|---|---|---|---|
| 1001 | Galaxy S25 Ultra | Electronics | 42.31% |
| 1007 | Eames Lounge Chair | Furniture | 50.83% |
| 1009 | LG OLED C4 65" TV | Electronics | 49.97% |

---

## 📊 Step 9: Aggregate Transformation

1. Click **+** on `ComputeMargin` → select **Aggregate**
2. Name: `AggregateKPIs`
3. **Group by** tab — add these columns:

| Column |
|---|
| `Region` |
| `Category` |
| `SaleYear` |
| `SaleMonth` |

4. **Aggregates** tab — add these expressions:

| Output column | Expression |
|---|---|
| `TotalOrders` | `count(SaleId)` |
| `TotalUnits` | `sum(Quantity)` |
| `TotalRevenue` | `sum(LineTotal)` |
| `AvgUnitPrice` | `avg(UnitPrice)` |
| `AvgMarginPct` | `avg(MarginPct)` |
| `UniqueCustomers` | `countDistinct(CustomerId)` |

5. Click **Data Preview** → verify 8 aggregated rows:

| Region | Category | TotalRevenue | TotalOrders |
|---|---|---|---|
| East | Electronics | 799.98 | 1 |
| East | Furniture | 5695.00 | 1 |
| North | Electronics | 3098.99 | 2 |
| North | Clothing | 294.00 | 1 |
| South | Clothing | 119.98 | 1 |
| South | Furniture | 1299.00 | 1 |
| West | Electronics | 1999.00 | 1 |
| West | Furniture | 1698.00 | 1 |

---

## 🔀 Step 10: Conditional Split Transformation

1. Click **+** on `AggregateKPIs` → select **Conditional split**
2. Name: `SplitByRevenue`
3. Add two output streams:

| Stream name | Condition |
|---|---|
| `Premium` | `TotalRevenue >= 1000` |
| `Standard` | `true()` ← default / catch-all |

4. Click **Data Preview** on each stream:

**Premium stream (5 rows — TotalRevenue ≥ 1000):**

| Region | Category | TotalRevenue | Segment |
|---|---|---|---|
| East | Furniture | 5695.00 | Premium |
| North | Electronics | 3098.99 | Premium |
| West | Electronics | 1999.00 | Premium |
| West | Furniture | 1698.00 | Premium |
| South | Furniture | 1299.00 | Premium |

**Standard stream (3 rows — TotalRevenue < 1000):**

| Region | Category | TotalRevenue | Segment |
|---|---|---|---|
| North | Clothing | 294.00 | Standard |
| South | Clothing | 119.98 | Standard |
| East | Electronics | 799.98 | Standard |

---

## 💾 Step 11: Add Sink Transformations

### Sink 1 — Premium

1. Click **+** on `SplitByRevenue` → **Premium** stream → select **Sink**
2. Name: `SinkPremium`
3. **Sink settings:**
   - Dataset: `DS_Gold_Premium`
   - Write method: **Output to single file**
   - File name option: `Pattern` → `premium_data`
4. **Mapping tab** → enable **Auto mapping**
5. **Settings tab:**
   - Clear the folder: **No** (append mode)
   - Partition option: **Single partition** (5 rows — no need to partition)

### Sink 2 — Standard

1. Click **+** on `SplitByRevenue` → **Standard** stream → select **Sink**
2. Name: `SinkStandard`
3. Dataset: `DS_Gold_Standard`
4. Same settings as Sink 1

---

## 🔗 Step 12: Wrap Data Flow in a Pipeline

1. **Author** → **Pipelines** → **+ New Pipeline**
2. Name: `PL_RawSales_Transformation`
3. Drag **Data Flow** activity onto the canvas
4. Name: `DF_TransformRawSales`
5. **Settings tab:**
   - Data flow: `DF_RawSales_Transformation`
   - Compute: **General Purpose** | Core count: **8**
6. (Optional) Add a **Web Activity** after the Data Flow for logging:
   - URL: your `/api/capture` endpoint
   - Body (Add dynamic content):
     ```
     @json(concat('{',
       '"pipelineName":"',   pipeline().Pipeline,                              '",',
       '"status":"',         activity('DF_TransformRawSales').Status,          '",',
       '"rowsRead":"',       string(activity('DF_TransformRawSales').output.runStatus.metrics.rowsRead),    '",',
       '"rowsWritten":"',    string(activity('DF_TransformRawSales').output.runStatus.metrics.rowsWritten), '",',
       '"loggedAt":"',       utcNow(),                                         '"',
     '}'))
     ```
7. Click **Debug** → monitor the pipeline run

---

## 🧪 Step 13: Validate Results

### Expected Data Flow Run Metrics

| Metric | Expected value |
|---|---|
| Rows read (source) | 17 |
| Rows after filter | 12 |
| Rows after aggregate | 8 |
| Rows to Premium sink | 5 |
| Rows to Standard sink | 3 |

### Verify ADLS Output

Check `gold/premium/` and `gold/standard/` in Azure Storage Explorer or Synapse:

```sql
-- Read from ADLS via Synapse external table
SELECT * FROM OPENROWSET(
    BULK 'https://<storage>.dfs.core.windows.net/gold/premium/*.parquet',
    FORMAT = 'PARQUET'
) AS premium
ORDER BY TotalRevenue DESC;
```

---

## 🔑 Key Data Flow Expression Reference

| Expression | Result | Used in |
|---|---|---|
| `Quantity * UnitPrice` | LineTotal | Derived Column |
| `year(toDate(SaleDate,'yyyy-MM-dd'))` | 2026 | Derived Column |
| `round((UnitPrice - UnitCost) / UnitPrice * 100, 2)` | MarginPct % | Derived Column |
| `!isNull(sale_id) && qty > 0` | Filter condition | Filter |
| `count(SaleId)` | Total orders in group | Aggregate |
| `countDistinct(CustomerId)` | Unique customers | Aggregate |
| `sum(LineTotal)` | Total revenue | Aggregate |
| `TotalRevenue >= 1000` | Premium route | Conditional Split |
| `currentDate()` | Today's date | Derived Column |
| `true()` | Default / catch-all route | Conditional Split |

---

## 🚨 Common Pitfalls to Avoid

| Pitfall | Impact | Fix |
|---|---|---|
| **Debug mode not on** | Data Preview is disabled — you build blind | Always enable Data Flow debug before starting |
| **Wrong join type in Lookup** | Inner join drops unmatched rows silently | Use **Any matching row** + add a Filter after to log unmatched |
| **Aggregate before Derived Column** | Can't reference LineTotal in SUM | Always compute derived fields before aggregating |
| **Single partition on large data** | OOM on big datasets | Use **Round robin** or **Key** partitioning in Sink settings |
| **String date not converted** | `year()` fails on raw string | Wrap with `toDate(SaleDate, 'yyyy-MM-dd')` before extracting parts |
| **No default stream in Conditional Split** | Rows not matching any condition are silently dropped | Always add a `true()` catch-all stream |

---

## 💡 Extension Ideas

| Enhancement | How |
|---|---|
| Add **Assert transformation** | After Filter — flags surviving rows that still have anomalous values, without dropping them |
| Add **Window transformation** | After Join — compute running totals or rank products within each region |
| Add **Pivot transformation** | After Aggregate — pivot Category into columns for a crosstab KPI table |
| **Schema drift** | Enable in Source settings → handles new columns from source without breaking the flow |
| **Incremental mode** | Add a watermark parameter to the source query to only process new rows each run |
| **Parameter-driven** | Add Data Flow parameters for `p_region` and `p_batchDate` → filter source rows dynamically |

---

*Generated for Azure Data Factory v2 · Mapping Data Flow · April 2026*
