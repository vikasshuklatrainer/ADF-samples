-- ============================================================
-- STEP 3: DATA MART - Sales Analytics Mart
-- Purpose: Pre-aggregated views for BI / reporting tools
-- ============================================================

-- -------------------------------------------------------
-- MART VIEW 1: Monthly Sales by Region
-- Used by: Executive dashboard, regional managers
-- -------------------------------------------------------
CREATE VIEW mart.vw_MonthlySalesByRegion AS
SELECT
    d.year_num,
    d.month_num,
    d.month_name,
    r.region_name,
    COUNT(DISTINCT f.transaction_id)    AS total_transactions,
    SUM(f.quantity)                     AS total_units_sold,
    SUM(f.gross_sales)                  AS gross_revenue,
    SUM(f.net_sales)                    AS net_revenue,
    SUM(f.gross_profit)                 AS gross_profit,
    ROUND(SUM(f.gross_profit) / NULLIF(SUM(f.net_sales), 0) * 100, 2) AS profit_margin_pct
FROM dbo.FactSales f
JOIN dbo.DimDate   d ON d.date_key   = f.date_key
JOIN dbo.DimRegion r ON r.region_key = f.region_key
GROUP BY d.year_num, d.month_num, d.month_name, r.region_name;

-- -------------------------------------------------------
-- MART VIEW 2: Product Performance Summary
-- Used by: Product team, category managers
-- -------------------------------------------------------
CREATE VIEW mart.vw_ProductPerformance AS
SELECT
    p.category,
    p.sub_category,
    p.product_name,
    p.brand,
    d.year_num,
    d.quarter_num,
    SUM(f.quantity)                     AS units_sold,
    SUM(f.gross_sales)                  AS gross_revenue,
    SUM(f.net_sales)                    AS net_revenue,
    SUM(f.gross_profit)                 AS gross_profit,
    AVG(f.discount_amount)              AS avg_discount,
    ROUND(SUM(f.gross_profit) / NULLIF(SUM(f.net_sales), 0) * 100, 2) AS margin_pct,
    RANK() OVER (
        PARTITION BY p.category, d.year_num, d.quarter_num
        ORDER BY SUM(f.net_sales) DESC
    ) AS rank_in_category
FROM dbo.FactSales f
JOIN dbo.DimProduct p ON p.product_key = f.product_key
JOIN dbo.DimDate    d ON d.date_key    = f.date_key
GROUP BY p.category, p.sub_category, p.product_name, p.brand, d.year_num, d.quarter_num;

-- -------------------------------------------------------
-- MART VIEW 3: Customer Segment Analysis
-- Used by: Marketing, CRM team
-- -------------------------------------------------------
CREATE VIEW mart.vw_CustomerSegmentAnalysis AS
SELECT
    c.segment,
    d.year_num,
    d.month_num,
    d.month_name,
    COUNT(DISTINCT f.customer_key)              AS active_customers,
    COUNT(DISTINCT f.transaction_id)            AS total_orders,
    SUM(f.net_sales)                            AS total_revenue,
    ROUND(SUM(f.net_sales)
          / NULLIF(COUNT(DISTINCT f.customer_key), 0), 2) AS revenue_per_customer,
    ROUND(SUM(f.net_sales)
          / NULLIF(COUNT(DISTINCT f.transaction_id), 0), 2) AS avg_order_value
FROM dbo.FactSales f
JOIN dbo.DimCustomer c ON c.customer_key = f.customer_key
JOIN dbo.DimDate     d ON d.date_key     = f.date_key
WHERE c.is_current = 1
GROUP BY c.segment, d.year_num, d.month_num, d.month_name;

-- -------------------------------------------------------
-- MART TABLE: Pre-aggregated weekly KPI snapshot
-- Materialized for fast dashboard loads
-- Populated by ADF pipeline on schedule
-- -------------------------------------------------------
CREATE TABLE mart.WeeklyKPISnapshot (
    snapshot_id         INT           NOT NULL PRIMARY KEY IDENTITY(1,1),
    snapshot_date       DATE          NOT NULL,
    week_of_year        TINYINT       NOT NULL,
    year_num            SMALLINT      NOT NULL,
    region_name         VARCHAR(20)   NOT NULL,
    total_transactions  INT           NOT NULL,
    total_revenue       DECIMAL(12,2) NOT NULL,
    total_profit        DECIMAL(12,2) NOT NULL,
    profit_margin_pct   DECIMAL(5,2)  NOT NULL,
    top_product         VARCHAR(100)  NULL,
    top_segment         VARCHAR(20)   NULL,
    created_at          DATETIME      NOT NULL DEFAULT GETDATE()
);

-- ADF pipeline inserts a fresh snapshot each week:
INSERT INTO mart.WeeklyKPISnapshot
    (snapshot_date, week_of_year, year_num, region_name,
     total_transactions, total_revenue, total_profit, profit_margin_pct,
     top_product, top_segment)
SELECT
    CAST(GETDATE() AS DATE),
    DATEPART(WEEK, GETDATE()),
    YEAR(GETDATE()),
    r.region_name,
    COUNT(DISTINCT f.transaction_id),
    SUM(f.net_sales),
    SUM(f.gross_profit),
    ROUND(SUM(f.gross_profit) / NULLIF(SUM(f.net_sales), 0) * 100, 2),
    (SELECT TOP 1 p2.product_name
     FROM dbo.FactSales f2
     JOIN dbo.DimProduct p2 ON p2.product_key = f2.product_key
     JOIN dbo.DimRegion r2  ON r2.region_key  = f2.region_key
     WHERE r2.region_name = r.region_name
       AND DATEPART(WEEK, (SELECT full_date FROM dbo.DimDate WHERE date_key = f2.date_key))
           = DATEPART(WEEK, GETDATE())
     GROUP BY p2.product_name ORDER BY SUM(f2.net_sales) DESC),
    (SELECT TOP 1 c2.segment
     FROM dbo.FactSales f3
     JOIN dbo.DimCustomer c2 ON c2.customer_key = f3.customer_key
     JOIN dbo.DimRegion r3   ON r3.region_key   = f3.region_key
     WHERE r3.region_name = r.region_name
       AND c2.is_current = 1
     GROUP BY c2.segment ORDER BY SUM(f3.net_sales) DESC)
FROM dbo.FactSales f
JOIN dbo.DimRegion r ON r.region_key = f.region_key
GROUP BY r.region_name;
