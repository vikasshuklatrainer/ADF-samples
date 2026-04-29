-- ============================================================
--  ADF Incremental Data Copy — SQL Scripts
--  Pattern   : High-Watermark (timestamp-based)
--  Database  : Azure SQL Database
--  Scenario  : dbo.Orders → ADLS / Sink (delta rows only)
-- ============================================================


-- ============================================================
--  PART 1 — SOURCE TABLE  (dbo.Orders)
--  The ModifiedDate column is the watermark column.
--  ADF will track the MAX(ModifiedDate) after each run.
-- ============================================================

IF OBJECT_ID('dbo.Orders', 'U') IS NOT NULL
    DROP TABLE dbo.Orders;
GO

CREATE TABLE dbo.Orders
(
    OrderId         INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    CustomerId      NVARCHAR(20)    NOT NULL,
    ProductCategory NVARCHAR(50)    NOT NULL,
    ProductName     NVARCHAR(150)   NOT NULL,
    Quantity        INT             NOT NULL,
    UnitPrice       DECIMAL(10,2)   NOT NULL,
    TotalAmount     DECIMAL(10,2)   NOT NULL,
    OrderStatus     NVARCHAR(30)    NOT NULL DEFAULT 'Pending',
    Region          NVARCHAR(50)    NOT NULL,
    CreatedDate     DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate    DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()   -- ← WATERMARK COLUMN
);
GO

-- Index on the watermark column — critical for incremental query performance
CREATE NONCLUSTERED INDEX IX_Orders_ModifiedDate
    ON dbo.Orders (ModifiedDate ASC)
    INCLUDE (OrderId, CustomerId, OrderStatus);
GO


-- ============================================================
--  PART 2 — INITIAL LOAD DATA  (Run 1 — Full load baseline)
--  These 10 rows represent the state on  2026-04-20
--  The watermark will be set to 2026-04-20 23:59:59 after run 1.
-- ============================================================

INSERT INTO dbo.Orders
    (CustomerId, ProductCategory, ProductName, Quantity, UnitPrice, TotalAmount, OrderStatus, Region, CreatedDate, ModifiedDate)
VALUES
    ('C001', 'Electronics', 'Galaxy S25 Ultra',         1, 1299.99, 1299.99, 'Completed', 'North', '2026-04-20 08:10:00', '2026-04-20 08:10:00'),
    ('C002', 'Clothing',    'Classic Slim Fit Chinos',   2,   59.99,  119.98, 'Completed', 'South', '2026-04-20 09:05:00', '2026-04-20 09:05:00'),
    ('C003', 'Furniture',   'Bamboo Bedside Table',      1,  179.00,  179.00, 'Completed', 'East',  '2026-04-20 10:30:00', '2026-04-20 10:30:00'),
    ('C004', 'Electronics', 'MacBook Pro 14-inch M4',    1, 1999.00, 1999.00, 'Completed', 'West',  '2026-04-20 11:15:00', '2026-04-20 11:15:00'),
    ('C005', 'Clothing',    'High Waist Yoga Leggings',  3,   98.00,  294.00, 'Completed', 'North', '2026-04-20 12:00:00', '2026-04-20 12:00:00'),
    ('C006', 'Furniture',   'Karlstad 3-Seater Sofa',    1,  699.00,  699.00, 'Processing','South', '2026-04-20 13:20:00', '2026-04-20 13:20:00'),
    ('C007', 'Electronics', 'Sony WH-1000XM6',           2,  399.99,  799.98, 'Completed', 'East',  '2026-04-20 14:45:00', '2026-04-20 14:45:00'),
    ('C008', 'Clothing',    'Puffer Down Winter Jacket',  1,  229.99,  229.99, 'Completed', 'West',  '2026-04-20 15:30:00', '2026-04-20 15:30:00'),
    ('C009', 'Electronics', 'iPad Air 13-inch M3',        1,  899.00,  899.00, 'Pending',   'North', '2026-04-20 16:50:00', '2026-04-20 16:50:00'),
    ('C010', 'Furniture',   'Industrial TV Stand 65"',    1,  299.99,  299.99, 'Completed', 'South', '2026-04-20 17:00:00', '2026-04-20 17:00:00');
GO

SELECT 'Initial load (Run 1)' AS Stage, COUNT(*) AS RowCount,
       MIN(ModifiedDate) AS EarliestMod, MAX(ModifiedDate) AS LatestMod
FROM dbo.Orders;
GO


-- ============================================================
--  PART 3 — DELTA DATA  (Run 2 — Incremental on 2026-04-21)
--  3 brand-new orders + 2 updates to existing orders.
--  ADF copies rows WHERE ModifiedDate > '2026-04-20 17:00:00'
-- ============================================================

-- 3 NEW orders inserted on 2026-04-21
INSERT INTO dbo.Orders
    (CustomerId, ProductCategory, ProductName, Quantity, UnitPrice, TotalAmount, OrderStatus, Region, CreatedDate, ModifiedDate)
VALUES
    ('C011', 'Electronics', 'Dell XPS 15 Laptop',        1, 1549.99, 1549.99, 'Pending',   'East',  '2026-04-21 08:20:00', '2026-04-21 08:20:00'),
    ('C012', 'Clothing',    'Cashmere Turtleneck Sweater',2,  189.00,  378.00, 'Completed', 'West',  '2026-04-21 09:45:00', '2026-04-21 09:45:00'),
    ('C013', 'Furniture',   'Velvet Accent Armchair',     1,  599.00,  599.00, 'Processing','North', '2026-04-21 11:00:00', '2026-04-21 11:00:00');
GO

-- 2 EXISTING orders updated on 2026-04-21 (status changes trigger ModifiedDate update)
UPDATE dbo.Orders
SET    OrderStatus  = 'Shipped',
       ModifiedDate = '2026-04-21 10:00:00'
WHERE  OrderId = 6;   -- Karlstad Sofa: Processing → Shipped
GO

UPDATE dbo.Orders
SET    OrderStatus  = 'Completed',
       ModifiedDate = '2026-04-21 14:30:00'
WHERE  OrderId = 9;   -- iPad Air: Pending → Completed
GO

SELECT 'After delta inserts/updates' AS Stage, COUNT(*) AS TotalRows,
       SUM(CASE WHEN ModifiedDate > '2026-04-20 17:00:00' THEN 1 ELSE 0 END) AS DeltaRows
FROM dbo.Orders;
GO


-- ============================================================
--  PART 4 — DELTA DATA  (Run 3 — Incremental on 2026-04-22)
--  2 new orders + 1 cancellation update
-- ============================================================

INSERT INTO dbo.Orders
    (CustomerId, ProductCategory, ProductName, Quantity, UnitPrice, TotalAmount, OrderStatus, Region, CreatedDate, ModifiedDate)
VALUES
    ('C001', 'Electronics', 'Anker 200W USB-C Hub',      3,  89.99,  269.97, 'Completed', 'North', '2026-04-22 09:10:00', '2026-04-22 09:10:00'),
    ('C014', 'Furniture',   'Solid Walnut Writing Desk',  1, 849.00,  849.00, 'Pending',   'West',  '2026-04-22 13:00:00', '2026-04-22 13:00:00');
GO

UPDATE dbo.Orders
SET    OrderStatus  = 'Cancelled',
       ModifiedDate = '2026-04-22 15:00:00'
WHERE  OrderId = 11;   -- Dell XPS: Pending → Cancelled
GO


-- ============================================================
--  PART 5 — WATERMARK CONTROL TABLE
--  ADF reads old watermark from here before each run,
--  then updates it with the new MAX(ModifiedDate) on success.
-- ============================================================

IF OBJECT_ID('dbo.WatermarkTable', 'U') IS NOT NULL
    DROP TABLE dbo.WatermarkTable;
GO

CREATE TABLE dbo.WatermarkTable
(
    TableName       NVARCHAR(100)   NOT NULL PRIMARY KEY,
    WatermarkValue  DATETIME2       NOT NULL,
    LastRunStatus   NVARCHAR(20)    NOT NULL DEFAULT 'Success',
    LastRunAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- Seed with a past date so the FIRST ADF run picks up ALL rows (full initial load)
INSERT INTO dbo.WatermarkTable (TableName, WatermarkValue, LastRunStatus, LastRunAt)
VALUES ('dbo.Orders', '1900-01-01 00:00:00', 'Initial', SYSUTCDATETIME());
GO

SELECT * FROM dbo.WatermarkTable;
GO


-- ============================================================
--  PART 6 — STORED PROCEDURE to update watermark
--  Called by ADF Stored Procedure activity after successful copy.
-- ============================================================

IF OBJECT_ID('dbo.usp_UpdateWatermark', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_UpdateWatermark;
GO

CREATE PROCEDURE dbo.usp_UpdateWatermark
    @TableName      NVARCHAR(100),
    @NewWatermark   DATETIME2
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.WatermarkTable
    SET    WatermarkValue = @NewWatermark,
           LastRunStatus  = 'Success',
           LastRunAt      = SYSUTCDATETIME()
    WHERE  TableName = @TableName;

    -- Raise an error if the table name was not found (safeguard)
    IF @@ROWCOUNT = 0
        THROW 50001, 'Watermark table entry not found for the given TableName.', 1;
END;
GO


-- ============================================================
--  PART 7 — HELPER QUERIES (use to verify ADF pipeline runs)
-- ============================================================

-- Query ADF uses in LOOKUP (Step 1) to get old watermark
SELECT WatermarkValue AS OldWatermark
FROM   dbo.WatermarkTable
WHERE  TableName = 'dbo.Orders';
GO

-- Query ADF uses in LOOKUP (Step 2) to get new watermark
SELECT MAX(ModifiedDate) AS NewWatermark
FROM   dbo.Orders;
GO

-- Query ADF uses in COPY activity source (run after populating delta data)
-- Replace @OldWatermark / @NewWatermark with actual values from the Lookup outputs
DECLARE @OldWatermark DATETIME2 = '2026-04-20 17:00:00';
DECLARE @NewWatermark DATETIME2 = '2026-04-21 14:30:00';

SELECT *
FROM   dbo.Orders
WHERE  ModifiedDate > @OldWatermark
AND    ModifiedDate <= @NewWatermark
ORDER  BY ModifiedDate ASC;
GO

-- Expected result for Run 2 delta: 5 rows
-- OrderId 11 (Dell XPS — NEW)
-- OrderId 12 (Cashmere Sweater — NEW)
-- OrderId 13 (Velvet Armchair — NEW)
-- OrderId  6 (Karlstad Sofa — UPDATED: Processing → Shipped)
-- OrderId  9 (iPad Air — UPDATED: Pending → Completed)
GO

-- Full data view with run classification
SELECT
    OrderId,
    CustomerId,
    ProductName,
    OrderStatus,
    ModifiedDate,
    CASE
        WHEN ModifiedDate <= '2026-04-20 17:00:00' THEN 'Run 1 — Full load'
        WHEN ModifiedDate <= '2026-04-21 14:30:00' THEN 'Run 2 — Delta'
        ELSE                                              'Run 3 — Delta'
    END AS CopiedInRun
FROM dbo.Orders
ORDER BY ModifiedDate;
GO
