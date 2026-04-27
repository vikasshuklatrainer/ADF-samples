-- ============================================================
--  ADF Mapping Data Flow — SQL Scripts
--  Scenario : Raw Sales Transformation Pipeline
--  Tables   : dbo.RawSales  (source — messy)
--             dbo.Products  (lookup reference)
--  Rows     : 15 raw sales + intentional bad rows to filter
-- ============================================================


-- ============================================================
--  1.  dbo.Products  (lookup / reference table)
--      Used by the Lookup Join transformation in the data flow
-- ============================================================

IF OBJECT_ID('dbo.Products','U') IS NOT NULL DROP TABLE dbo.Products;
GO

CREATE TABLE dbo.Products
(
    ProductId       NVARCHAR(20)    NOT NULL PRIMARY KEY,
    ProductName     NVARCHAR(150)   NOT NULL,
    Brand           NVARCHAR(100)   NOT NULL,
    Category        NVARCHAR(50)    NOT NULL,    -- Electronics | Clothing | Furniture
    SubCategory     NVARCHAR(80)    NOT NULL,
    UnitCost        DECIMAL(10,2)   NOT NULL,    -- used to compute margin in derived column
    IsActive        BIT             NOT NULL DEFAULT 1
);
GO

INSERT INTO dbo.Products (ProductId, ProductName, Brand, Category, SubCategory, UnitCost)
VALUES
    ('P001', 'Galaxy S25 Ultra',           'Samsung',       'Electronics', 'Smartphones',   750.00),
    ('P002', 'MacBook Pro 14-inch M4',     'Apple',         'Electronics', 'Laptops',       1100.00),
    ('P003', 'Sony WH-1000XM6',            'Sony',          'Electronics', 'Audio',          180.00),
    ('P004', 'LG OLED C4 65" TV',          'LG',            'Electronics', 'Televisions',   900.00),
    ('P005', 'Classic Slim Fit Chinos',    'Levis',          'Clothing',    'Trousers',        22.00),
    ('P006', 'Floral Wrap Midi Dress',     'Zara',           'Clothing',    'Dresses',         28.00),
    ('P007', 'High Waist Yoga Leggings',   'Lululemon',      'Clothing',    'Activewear',      38.00),
    ('P008', 'Puffer Down Winter Jacket',  'The North Face', 'Clothing',    'Jackets',         95.00),
    ('P009', 'Karlstad 3-Seater Sofa',     'IKEA',           'Furniture',   'Sofas',          300.00),
    ('P010', 'Eames Lounge Chair',         'Herman Miller',  'Furniture',   'Chairs',        2800.00),
    ('P011', 'Farmhouse Dining Table',     'Pottery Barn',   'Furniture',   'Dining Tables',  550.00),
    ('P012', 'Solid Walnut Writing Desk',  'Article',        'Furniture',   'Desks',          380.00);
GO


-- ============================================================
--  2.  dbo.RawSales  (source — intentionally messy)
--      Contains nulls, negatives, and duplicates that
--      the data flow Filter transformation will remove.
-- ============================================================

IF OBJECT_ID('dbo.RawSales','U') IS NOT NULL DROP TABLE dbo.RawSales;
GO

CREATE TABLE dbo.RawSales
(
    sale_id         INT             NULL,        -- nullable (bad design in source)
    cust_id         NVARCHAR(20)    NULL,
    prod_id         NVARCHAR(20)    NULL,
    qty             INT             NULL,        -- some rows have NULL or negative
    unit_price      DECIMAL(10,2)   NULL,
    store_region    NVARCHAR(50)    NULL,
    sale_dt         NVARCHAR(30)    NULL,        -- stored as string in source (!)
    source_system   NVARCHAR(30)    NULL
);
GO

-- ── GOOD ROWS (will pass the Filter) ─────────────────────
INSERT INTO dbo.RawSales VALUES (1001,'C001','P001', 1, 1299.99,'North','2026-04-23','StoreSQL');
INSERT INTO dbo.RawSales VALUES (1002,'C002','P005', 2,   59.99,'South','2026-04-23','StoreSQL');
INSERT INTO dbo.RawSales VALUES (1003,'C003','P009', 1,  699.00,'East', '2026-04-23','StoreSQL');
INSERT INTO dbo.RawSales VALUES (1004,'C004','P002', 1, 1999.00,'West', '2026-04-23','StoreSQL');
INSERT INTO dbo.RawSales VALUES (1005,'C005','P007', 3,   98.00,'North','2026-04-23','StoreSQL');
INSERT INTO dbo.RawSales VALUES (1006,'C006','P003', 2,  399.99,'South','2026-04-23','POS_CSV');
INSERT INTO dbo.RawSales VALUES (1007,'C007','P010', 1, 5695.00,'East', '2026-04-23','POS_CSV');
INSERT INTO dbo.RawSales VALUES (1008,'C008','P008', 1,  229.99,'West', '2026-04-23','POS_CSV');
INSERT INTO dbo.RawSales VALUES (1009,'C009','P004', 1, 1799.00,'North','2026-04-23','REST_API');
INSERT INTO dbo.RawSales VALUES (1010,'C010','P011', 1, 1299.00,'South','2026-04-23','REST_API');
INSERT INTO dbo.RawSales VALUES (1011,'C001','P012', 2,  849.00,'West', '2026-04-23','REST_API');
INSERT INTO dbo.RawSales VALUES (1012,'C002','P006', 1,   79.99,'East', '2026-04-23','StoreSQL');

-- ── BAD ROWS (will be caught by the Filter) ───────────────
-- NULL sale_id
INSERT INTO dbo.RawSales VALUES (NULL,'C003','P001', 1, 1299.99,'North','2026-04-23','StoreSQL');
-- NULL quantity
INSERT INTO dbo.RawSales VALUES (1013,'C004','P003', NULL,399.99,'South','2026-04-23','StoreSQL');
-- Negative quantity (returns fed into wrong table)
INSERT INTO dbo.RawSales VALUES (1014,'C005','P007',-2,  98.00,'West', '2026-04-23','POS_CSV');
-- NULL product id
INSERT INTO dbo.RawSales VALUES (1015,'C006', NULL,  1, 199.99,'East', '2026-04-23','REST_API');
-- Zero unit price
INSERT INTO dbo.RawSales VALUES (1016,'C007','P005', 3,   0.00,'North','2026-04-23','StoreSQL');
GO

-- Verify source data
SELECT
    CASE
        WHEN sale_id IS NULL         THEN 'Bad — null SaleId'
        WHEN qty     IS NULL         THEN 'Bad — null Qty'
        WHEN qty     <= 0            THEN 'Bad — zero/negative Qty'
        WHEN prod_id IS NULL         THEN 'Bad — null ProductId'
        WHEN unit_price <= 0         THEN 'Bad — zero price'
        ELSE                              'Good — will pass filter'
    END AS RowQuality,
    COUNT(*) AS RowCount
FROM dbo.RawSales
GROUP BY
    CASE
        WHEN sale_id IS NULL         THEN 'Bad — null SaleId'
        WHEN qty     IS NULL         THEN 'Bad — null Qty'
        WHEN qty     <= 0            THEN 'Bad — zero/negative Qty'
        WHEN prod_id IS NULL         THEN 'Bad — null ProductId'
        WHEN unit_price <= 0         THEN 'Bad — zero price'
        ELSE                              'Good — will pass filter'
    END;
GO


-- ============================================================
--  3.  WHAT THE DATA FLOW PRODUCES  (for reference & testing)
--      This query simulates the full data flow output in SQL
--      so you can verify the ADF result matches exactly.
-- ============================================================

-- Step 1: Filter
WITH Filtered AS
(
    SELECT *
    FROM   dbo.RawSales
    WHERE  sale_id   IS NOT NULL
    AND    qty        IS NOT NULL
    AND    qty        >  0
    AND    prod_id    IS NOT NULL
    AND    unit_price >  0
),

-- Step 2: Select / Rename
Renamed AS
(
    SELECT
        sale_id         AS SaleId,
        cust_id         AS CustomerId,
        prod_id         AS ProductId,
        qty             AS Quantity,
        unit_price      AS UnitPrice,
        store_region    AS Region,
        sale_dt         AS SaleDate,
        source_system   AS SourceSystem
    FROM Filtered
),

-- Step 3: Derived columns
Derived AS
(
    SELECT
        r.*,
        r.Quantity * r.UnitPrice                    AS LineTotal,
        LEFT(r.SaleDate, 4)                         AS SaleYear,
        SUBSTRING(r.SaleDate, 6, 2)                 AS SaleMonth,
        CAST(GETDATE() AS DATE)                     AS LoadDate
    FROM Renamed r
),

-- Step 4: Lookup join with Products
Joined AS
(
    SELECT
        d.*,
        p.ProductName,
        p.Brand,
        p.Category,
        p.SubCategory,
        p.UnitCost,
        -- Margin computed here as bonus derived column
        ROUND((d.UnitPrice - p.UnitCost) / d.UnitPrice * 100, 2) AS MarginPct
    FROM   Derived d
    INNER  JOIN dbo.Products p ON p.ProductId = d.ProductId
),

-- Step 5: Aggregate (GROUP BY Region + Category)
Aggregated AS
(
    SELECT
        Region,
        Category,
        SaleYear,
        SaleMonth,
        COUNT(SaleId)       AS TotalOrders,
        SUM(Quantity)       AS TotalUnits,
        SUM(LineTotal)      AS TotalRevenue,
        AVG(UnitPrice)      AS AvgUnitPrice,
        AVG(MarginPct)      AS AvgMarginPct,
        COUNT(DISTINCT CustomerId) AS UniqueCustomers
    FROM  Joined
    GROUP BY Region, Category, SaleYear, SaleMonth
)

-- Step 6: Conditional split preview
SELECT
    *,
    CASE WHEN TotalRevenue >= 1000 THEN 'Premium' ELSE 'Standard' END AS Segment
FROM Aggregated
ORDER BY TotalRevenue DESC;
GO


-- ============================================================
--  4.  EXPECTED OUTPUT — row-level (before aggregation)
--      Shows derived columns computed per sale row
-- ============================================================

SELECT
    s.sale_id                                       AS SaleId,
    s.cust_id                                       AS CustomerId,
    s.prod_id                                       AS ProductId,
    p.ProductName,
    p.Brand,
    p.Category,
    p.SubCategory,
    s.qty                                           AS Quantity,
    s.unit_price                                    AS UnitPrice,
    s.qty * s.unit_price                            AS LineTotal,
    ROUND((s.unit_price - p.UnitCost)
          / s.unit_price * 100, 2)                  AS MarginPct,
    s.store_region                                  AS Region,
    s.sale_dt                                       AS SaleDate,
    s.source_system                                 AS SourceSystem
FROM  dbo.RawSales s
INNER JOIN dbo.Products p ON p.ProductId = s.prod_id
WHERE s.sale_id IS NOT NULL
  AND s.qty     IS NOT NULL
  AND s.qty      > 0
  AND s.prod_id IS NOT NULL
  AND s.unit_price > 0
ORDER BY s.sale_id;
GO
