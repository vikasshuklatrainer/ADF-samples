# Azure Data Factory: Medallion Architecture Pipeline

---

## 📌 Scenario Overview

**Scenario: Retail Sales Analytics Platform**

A retail company receives daily sales data from three different sources — an Azure SQL Database (transactional orders), CSV files dropped into Blob Storage (store-level POS exports), and a REST API (external partner sales). The business needs a reliable, layered data platform that supports both operational reporting in Power BI and advanced analytics in Azure Synapse.

The **Medallion Architecture** (also called the Delta Lake architecture) solves this by organizing data into three progressive quality zones:

| Layer | Color | Purpose |
|---|---|---|
| **Bronze** | Raw / unprocessed | Land data exactly as received — no changes |
| **Silver** | Cleansed / validated | Remove nulls, dedup, standardize, enforce schema |
| **Gold** | Aggregated / curated | Business KPIs, reporting tables, joined dimensions |

**Why this matters:** Each layer is independently queryable and replayable. If a Silver transformation has a bug, you replay from Bronze — you never lose the original data.

---

## 🏗️ Architecture Overview

```
[SQL Database]   [Blob / CSV files]   [REST / HTTP API]
       │                │                    │
       └────────────────┼────────────────────┘
                        │
               ┌────────▼─────────┐
               │   BRONZE LAYER   │  ← Raw landing zone
               │  Ingestion PL    │  Copy Activity (as-is)
               │  ADLS bronze/    │
               └────────┬─────────┘
                        │
               ┌────────▼─────────┐
               │   SILVER LAYER   │  ← Cleansed & validated
               │  Transform PL    │  Mapping Data Flow
               │  ADLS silver/    │
               └────────┬─────────┘
                        │
               ┌────────▼─────────┐
               │    GOLD LAYER    │  ← Business-ready
               │  Aggregation PL  │  Data Flow + Stored Proc
               │  ADLS gold/      │
               └────────┬─────────┘
                        │
              ┌─────────┴──────────┐
              │                    │
        [Power BI]        [Azure Synapse Analytics]
     Dashboards & Reports    SQL Pools & Notebooks
```

---

## ✅ Prerequisites

- **Azure Data Factory** instance (v2)
- **Azure Data Lake Storage Gen2** (ADLS Gen2) with three containers: `bronze/`, `silver/`, `gold/`
- **Azure SQL Database** — source transactional system
- **Azure Synapse Analytics** workspace (optional — for Gold consumption)
- Linked Services configured for: ADLS Gen2, Azure SQL DB, HTTP (for REST API)

---

## 🗃️ Step 1: Set Up the Storage Containers (ADLS Gen2)

Create the three medallion containers in Azure Data Lake Storage Gen2:

```
medallion-storage/
  ├── bronze/
  │     └── sales/
  │           ├── sql/        ← from SQL DB
  │           ├── csv/        ← from Blob CSV files
  │           └── api/        ← from REST API
  ├── silver/
  │     └── sales/            ← unified, cleansed
  └── gold/
        └── sales_summary/    ← aggregated KPIs
```

In the Azure Portal:
1. Open your **Storage Account** → **Containers**
2. Create containers: `bronze`, `silver`, `gold`
3. Enable **Hierarchical namespace** (required for ADLS Gen2)

---

## 🔗 Step 2: Create Linked Services

### 2a. ADLS Gen2 Linked Service

1. In ADF Studio → **Manage** → **Linked Services** → **+ New**
2. Select **Azure Data Lake Storage Gen2**
3. Name: `LS_ADLS_Medallion`
4. Authentication: **Managed Identity** (recommended)
5. Test connection → **Create**

### 2b. Azure SQL Database Linked Service

1. **+ New** → **Azure SQL Database**
2. Name: `LS_AzureSqlDB`
3. Fill in server, database, credentials
4. Test connection → **Create**

### 2c. HTTP Linked Service (REST API)

1. **+ New** → **HTTP**
2. Name: `LS_RestAPI`
3. Base URL: your partner API endpoint
4. Authentication: **Anonymous** or **Basic** depending on API
5. Test connection → **Create**

---

## 📦 Step 3: Create Datasets

### 3a. Source Datasets

| Dataset Name | Type | Linked Service | Notes |
|---|---|---|---|
| `DS_SQL_Orders` | Azure SQL Table | `LS_AzureSqlDB` | Table: `dbo.Orders` |
| `DS_CSV_POS` | DelimitedText (CSV) | `LS_ADLS_Medallion` | Container: `bronze/sales/csv/` |
| `DS_REST_API` | JSON | `LS_RestAPI` | Relative URL: `/api/v1/sales` |

### 3b. Bronze Sink Datasets (Parameterized)

Create one **Binary** dataset that accepts a file path parameter — this lets one dataset serve all three bronze sub-paths:

1. **+ New Dataset** → Azure Data Lake Storage Gen2 → **Binary**
2. Name: `DS_Bronze_Binary`
3. Linked Service: `LS_ADLS_Medallion`
4. **Parameters tab** → Add:
   - `container` | Type: `String`
   - `folderPath` | Type: `String`
   - `fileName` | Type: `String`
5. **Connection tab:**
   - Container: `@dataset().container`
   - Directory: `@dataset().folderPath`
   - File: `@dataset().fileName`
6. **Publish All**

### 3c. Silver and Gold Datasets

| Dataset Name | Type | Path |
|---|---|---|
| `DS_Silver_Parquet` | Parquet | `silver/sales/` |
| `DS_Gold_Parquet` | Parquet | `gold/sales_summary/` |

> 💡 **Why Parquet for Silver and Gold?** Parquet is columnar, compressed, and natively supported by Synapse and Power BI. CSV is fine for raw Bronze ingestion, but Parquet is the standard for analytical layers.

---

## 🟤 Step 4: Build the Bronze Pipeline (Ingestion)

**Pipeline name:** `PL_Bronze_Ingestion`

This pipeline ingests raw data from all three sources into the Bronze landing zone without any transformation.

### 4a. Ingest from SQL Database

1. Drag **Copy Data** activity onto the canvas
2. Name it `CPY_SQL_to_Bronze`
3. **Source tab:**
   - Dataset: `DS_SQL_Orders`
   - Use query:
     ```sql
     SELECT * FROM dbo.Orders
     WHERE CAST(OrderDate AS DATE) = CAST(GETDATE() AS DATE)
     ```
4. **Sink tab:**
   - Dataset: `DS_Bronze_Binary`
   - Dataset properties:
     - `container`: `bronze`
     - `folderPath`: `sales/sql`
     - `fileName`: Add dynamic content:
       ```
       orders_@{formatDateTime(utcNow(),'yyyyMMdd')}.json
       ```
5. **Settings tab:** Enable **Fault tolerance** → Skip incompatible rows

### 4b. Ingest CSV Files from Blob

1. Drag a second **Copy Data** activity
2. Name it `CPY_CSV_to_Bronze`
3. **Source tab:** Dataset: `DS_CSV_POS`, file path wildcard: `*.csv`
4. **Sink tab:** Dataset: `DS_Bronze_Binary`
   - `container`: `bronze`
   - `folderPath`: `sales/csv`
   - `fileName`: `@{item().name}` (use ForEach if iterating multiple files)

### 4c. Ingest from REST API

1. Drag a third **Copy Data** activity
2. Name it `CPY_API_to_Bronze`
3. **Source tab:**
   - Dataset: `DS_REST_API`
   - Request method: `GET`
   - Additional headers: `Authorization: Bearer <token>`
4. **Sink tab:** Dataset: `DS_Bronze_Binary`
   - `container`: `bronze`
   - `folderPath`: `sales/api`
   - `fileName`:
     ```
     api_sales_@{formatDateTime(utcNow(),'yyyyMMdd')}.json
     ```

### 4d. Run all three in parallel

Connect all three Copy activities with no dependency arrows (they run concurrently by default in ADF). The pipeline completes when all three finish.

```
[CPY_SQL_to_Bronze]
[CPY_CSV_to_Bronze]   ← All run in parallel (no arrows between them)
[CPY_API_to_Bronze]
```

---

## 🔵 Step 5: Build the Silver Pipeline (Transformation)

**Pipeline name:** `PL_Silver_Transformation`

This pipeline reads from Bronze, applies cleansing logic using **Mapping Data Flow**, and writes unified Parquet to Silver.

### 5a. Add Mapping Data Flow Activity

1. First, create a **Mapping Data Flow**: Author → Data flows → **+ New data flow**
2. Name it `DF_Bronze_to_Silver`

### 5b. Design the Data Flow Transformations

Inside the Data Flow canvas, chain these transformations:

**Source (Bronze SQL data):**
- Source dataset: `DS_Bronze_Binary` → point to `bronze/sales/sql/`
- Format: JSON, detect schema automatically

**Step 1 — Filter: remove nulls**
```
!isNull(OrderId) && !isNull(CustomerId) && !isNull(OrderDate)
```

**Step 2 — Select: standardize column names**
Rename/drop columns:
- `order_id` → `OrderId`
- `cust_id` → `CustomerId`
- `order_dt` → `OrderDate`
- Drop internal system columns (e.g., `__rowversion`)

**Step 3 — Derived column: add audit fields**
```
LoadDate  = currentTimestamp()
SourceSystem = 'SQL_Orders'
```

**Step 4 — Aggregate: deduplicate by OrderId**
- Group by: `OrderId`
- Keep last modified record using `last(OrderDate)` as tiebreaker

**Step 5 — Union: merge CSV and API sources**
Add additional source branches for `bronze/sales/csv/` and `bronze/sales/api/`, apply same filter/select logic, then Union all three streams.

**Step 6 — Sink (Silver Parquet):**
- Dataset: `DS_Silver_Parquet`
- Output to: `silver/sales/`
- Partition option: **Round robin** (or by `OrderDate` date partition)
- Enable **Auto mapping**

### 5c. Add Data Flow to Pipeline

Back in `PL_Silver_Transformation`:
1. Drag **Data Flow** activity onto the canvas
2. Name it `DF_BronzeToSilver`
3. Settings → Data flow: `DF_Bronze_to_Silver`
4. Set **Compute type**: `General Purpose`, 8 cores (adjust based on volume)

---

## 🟡 Step 6: Build the Gold Pipeline (Aggregation)

**Pipeline name:** `PL_Gold_Aggregation`

This pipeline reads from Silver and produces business-level KPI tables ready for Power BI and Synapse.

### 6a. Create Mapping Data Flow for Aggregation

Create a new Data Flow: `DF_Silver_to_Gold`

**Source:** `DS_Silver_Parquet` → `silver/sales/`

**Step 1 — Derived column: extract date parts**
```
SaleYear  = year(OrderDate)
SaleMonth = month(OrderDate)
SaleDay   = dayOfMonth(OrderDate)
```

**Step 2 — Aggregate: compute KPIs by region and date**
Group by: `Region`, `ProductCategory`, `SaleYear`, `SaleMonth`

Aggregations:
```
TotalRevenue     = sum(OrderAmount)
TotalOrders      = count(OrderId)
AvgOrderValue    = avg(OrderAmount)
UniqueCustomers  = countDistinct(CustomerId)
```

**Step 3 — Sink (Gold Parquet):**
- Dataset: `DS_Gold_Parquet`
- Output to: `gold/sales_summary/`
- Partition by: `SaleYear`, `SaleMonth`

### 6b. Add Activities to Gold Pipeline

1. Drag **Data Flow** activity → `DF_SilverToGold`
2. After Data Flow succeeds, drag **Stored Procedure** activity
3. Name it `SP_RefreshGoldViews`
4. Linked Service: `LS_AzureSqlDB` (or Synapse dedicated SQL pool)
5. Stored procedure name: `[gold].[usp_RefreshReportingViews]`
6. Parameters: `@{formatDateTime(utcNow(),'yyyy-MM-dd')}`

> The stored procedure updates SQL views that Power BI uses for DirectQuery, ensuring reports always reflect the latest Gold data.

---

## 🔗 Step 7: Create the Master Orchestration Pipeline

**Pipeline name:** `PL_Medallion_Master`

This pipeline ties all three layers together in sequence with dependency checks.

### 7a. Chain the Three Pipelines

1. Drag **Execute Pipeline** activity → Name: `EP_Bronze`
   - Called pipeline: `PL_Bronze_Ingestion`

2. Draw **Success** arrow → Drag **Execute Pipeline** → Name: `EP_Silver`
   - Called pipeline: `PL_Silver_Transformation`

3. Draw **Success** arrow → Drag **Execute Pipeline** → Name: `EP_Gold`
   - Called pipeline: `PL_Gold_Aggregation`

```
[EP_Bronze] ──(success)──► [EP_Silver] ──(success)──► [EP_Gold]
```

### 7b. Add Error Handling

After each Execute Pipeline activity, also draw a **Failure** arrow to a **Web Activity** that sends an alert:

1. Drag **Web Activity** → Name `WEB_AlertOnFailure`
2. URL: your Teams webhook or Logic App
3. Method: POST
4. Body:
```json
{
  "title": "Medallion Pipeline Failed",
  "text": "Stage: @{activity('EP_Bronze').Error.message}. Pipeline run: @{pipeline().RunId}"
}
```

---

## ⏱️ Step 8: Add a Schedule Trigger

1. Click **Add trigger** → **New/Edit**
2. Name: `TRG_Daily_Medallion`
3. Type: **Schedule**
4. Start: `Today at 02:00 AM UTC`
5. Recurrence: **Every 1 day**
6. Click **OK** → **Publish All**

---

## 🧪 Step 9: Debug and Validate

### Debug Order

Always debug in layer order:

```
1. PL_Bronze_Ingestion   → verify files land in bronze/
2. PL_Silver_Transformation → verify Parquet written to silver/
3. PL_Gold_Aggregation   → verify KPI Parquet in gold/ and views refreshed
4. PL_Medallion_Master   → end-to-end orchestration test
```

### Validation Checklist

| Layer | What to verify |
|---|---|
| Bronze | Files exist in `bronze/sales/sql/`, `csv/`, `api/` with today's date |
| Silver | Parquet files in `silver/sales/` — no nulls, no dupes in OrderId |
| Gold | Parquet in `gold/sales_summary/` — row counts match expected aggregates |
| Trigger | Confirm scheduled trigger is active and next run is shown |

### Quick Data Quality Check (run in Synapse or ADF Data Preview)

```sql
-- Silver: check for null OrderIds
SELECT COUNT(*) AS NullOrders FROM silver.sales WHERE OrderId IS NULL;
-- expected: 0

-- Gold: verify KPI completeness
SELECT SaleYear, SaleMonth, COUNT(*) AS Regions
FROM gold.sales_summary
GROUP BY SaleYear, SaleMonth
ORDER BY SaleYear, SaleMonth;
```

---

## 📊 Step 10: Connect Consumers

### Power BI

1. Open Power BI Desktop → **Get Data** → **Azure Data Lake Storage Gen2**
2. Connect to `gold/sales_summary/`
3. Select Parquet files → Load
4. Build visuals directly on Gold aggregates (no transforms needed — it's already business-ready)

### Azure Synapse Analytics

1. In Synapse Studio → **Data** → **Linked** → your ADLS Gen2
2. Navigate to `gold/sales_summary/`
3. Right-click → **New SQL Script** → **Select TOP 100**
4. Or create an **External Table** over the Gold Parquet:

```sql
CREATE EXTERNAL TABLE gold.SalesSummary (
    Region          NVARCHAR(100),
    ProductCategory NVARCHAR(100),
    SaleYear        INT,
    SaleMonth       INT,
    TotalRevenue    DECIMAL(18,2),
    TotalOrders     INT,
    AvgOrderValue   DECIMAL(18,2),
    UniqueCustomers INT
)
WITH (
    LOCATION = 'gold/sales_summary/**',
    DATA_SOURCE = MedallionStorage,
    FILE_FORMAT = ParquetFormat
);
```

---

## 🔑 Key Concepts Recap

| Concept | Bronze | Silver | Gold |
|---|---|---|---|
| ADF activity | Copy Activity | Mapping Data Flow | Data Flow + Stored Proc |
| Data format | JSON / CSV (raw) | Parquet (typed) | Parquet (aggregated) |
| Transformation | None | Filter, dedup, rename, union | Group by, aggregate KPIs |
| Replayable? | ✅ Yes (source of truth) | ✅ Yes (from Bronze) | ✅ Yes (from Silver) |
| Query use | Debugging / audit | Data science, ML features | BI dashboards, SQL queries |

---

## 🚨 Common Pitfalls to Avoid

- ❌ **Transforming in Bronze** — Bronze must be a faithful copy of source. Any transformation belongs in Silver or later.
- ❌ **Using CSV for Silver/Gold** — Always use Parquet for analytical layers; CSV has no schema enforcement and poor read performance.
- ❌ **No partition strategy in Gold** — Partition Gold by `Year/Month` or `Region` to enable partition pruning in Power BI DirectQuery.
- ❌ **Skipping the Failure branch** — Silent pipeline failures corrupt the Gold layer; always wire failure alerts.
- ❌ **Hardcoding dates** — Use `@{formatDateTime(utcNow(),'yyyyMMdd')}` everywhere so the pipeline is date-agnostic and replayable for any date via pipeline parameters.

---

## 💡 Extension Ideas

| Enhancement | Implementation |
|---|---|
| Schema drift handling | Enable **Allow schema drift** in Bronze Copy Activity sinks |
| Data quality metrics | Add a Mapping Data Flow **Assert** transformation in Silver |
| Incremental loads | Use **watermark pattern** in Bronze: store last `MaxOrderDate` in a control table |
| Unity Catalog / Purview | Register Bronze/Silver/Gold datasets in Microsoft Purview for lineage |
| Cost optimization | Set Gold pipeline compute to **Memory Optimized** for large aggregations |
| Re-run from any layer | Add pipeline parameter `p_startLayer` (Bronze/Silver/Gold) with If Condition branching |

---

*Generated for Azure Data Factory v2 | Last updated: April 2026*
