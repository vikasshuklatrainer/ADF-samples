-- ============================================================
--  ADF Until + Switch Activity Demo — SQL Scripts
--  Scenario  : Order Processing Pipeline
--  Tables    : dbo.PendingOrders  (source)
--              dbo.ProcessedOrders (sink)
--              dbo.APIRetryLog     (audit log for Until activity)
-- ============================================================


-- ============================================================
--  PART 1 — SOURCE TABLE  dbo.PendingOrders
--  Each order has a ProductCategory column used by the
--  Switch activity to route to the correct branch.
-- ============================================================

IF OBJECT_ID('dbo.PendingOrders','U') IS NOT NULL DROP TABLE dbo.PendingOrders;
GO

CREATE TABLE dbo.PendingOrders
(
    OrderId          INT            NOT NULL IDENTITY(1,1) PRIMARY KEY,
    CustomerId       NVARCHAR(20)   NOT NULL,
    ProductCategory  NVARCHAR(50)   NOT NULL,   -- ← Switch routes on this
    ProductName      NVARCHAR(150)  NOT NULL,
    SKU              NVARCHAR(50)   NOT NULL,
    Quantity         INT            NOT NULL,
    UnitPrice        DECIMAL(10,2)  NOT NULL,
    TotalAmount      DECIMAL(10,2)  NOT NULL,
    Region           NVARCHAR(50)   NOT NULL,
    Priority         NVARCHAR(20)   NOT NULL DEFAULT 'Standard',  -- High | Standard | Low
    RequiresAPIConfirm BIT          NOT NULL DEFAULT 0,           -- ← triggers Until loop
    CreatedDate      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    Status           NVARCHAR(30)   NOT NULL DEFAULT 'Pending'
);
GO

-- ── 10 sample orders — mixed categories for Switch demo ───────
INSERT INTO dbo.PendingOrders
    (CustomerId, ProductCategory, ProductName, SKU, Quantity, UnitPrice, TotalAmount, Region, Priority, RequiresAPIConfirm)
VALUES
--  Electronics (→ Premium check branch)
    ('C001', 'Electronics', 'Galaxy S25 Ultra',        'ELEC-SAM-001', 1, 1299.99, 1299.99, 'North', 'High',     1),
    ('C004', 'Electronics', 'MacBook Pro 14-inch M4',  'ELEC-APL-002', 1, 1999.00, 1999.00, 'West',  'High',     1),
    ('C007', 'Electronics', 'Sony WH-1000XM6',         'ELEC-SNY-003', 2,  399.99,  799.98, 'East',  'Standard', 0),
--  Clothing  (→ Standard copy branch)
    ('C002', 'Clothing',    'Classic Slim Fit Chinos',  'CLTH-LVS-001', 2,   59.99,  119.98, 'South', 'Standard', 0),
    ('C005', 'Clothing',    'High Waist Yoga Leggings', 'CLTH-LUL-005', 3,   98.00,  294.00, 'North', 'Standard', 0),
    ('C008', 'Clothing',    'Puffer Down Winter Jacket','CLTH-TNF-006', 1,  229.99,  229.99, 'West',  'Low',      0),
--  Furniture (→ Bulk check branch)
    ('C003', 'Furniture',   'Karlstad 3-Seater Sofa',  'FURN-IKA-001', 1,  699.00,  699.00, 'East',  'Standard', 1),
    ('C006', 'Furniture',   'Eames Lounge Chair',      'FURN-HM-003',  1, 5695.00, 5695.00, 'South', 'High',     1),
    ('C009', 'Furniture',   'Solid Walnut Writing Desk','FURN-ART-007', 2,  849.00, 1698.00, 'North', 'Standard', 0),
--  Default / Unknown (→ Default branch in Switch)
    ('C010', 'Accessories', 'Leather Laptop Sleeve',   'ACC-GEN-010',  1,   49.99,   49.99, 'West',  'Low',      0);
GO

SELECT 'PendingOrders' AS [Table],
       ProductCategory,
       COUNT(*)        AS OrderCount,
       SUM(TotalAmount)AS TotalValue
FROM dbo.PendingOrders
GROUP BY ProductCategory
ORDER BY ProductCategory;
GO


-- ============================================================
--  PART 2 — SINK TABLE  dbo.ProcessedOrders
--  ADF writes results here after Switch + Until processing.
-- ============================================================

IF OBJECT_ID('dbo.ProcessedOrders','U') IS NOT NULL DROP TABLE dbo.ProcessedOrders;
GO

CREATE TABLE dbo.ProcessedOrders
(
    ProcessedId      INT            NOT NULL IDENTITY(1,1) PRIMARY KEY,
    OrderId          INT            NOT NULL,
    CustomerId       NVARCHAR(20)   NOT NULL,
    ProductCategory  NVARCHAR(50)   NOT NULL,
    ProductName      NVARCHAR(150)  NOT NULL,
    SKU              NVARCHAR(50)   NOT NULL,
    Quantity         INT            NOT NULL,
    TotalAmount      DECIMAL(10,2)  NOT NULL,
    Region           NVARCHAR(50)   NOT NULL,
    SwitchBranch     NVARCHAR(50)   NOT NULL,   -- which branch handled it
    APIConfirmed     BIT            NOT NULL DEFAULT 0,
    APIRetries       INT            NOT NULL DEFAULT 0,
    ProcessedDate    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    ProcessStatus    NVARCHAR(30)   NOT NULL DEFAULT 'Success'
);
GO


-- ============================================================
--  PART 3 — AUDIT LOG TABLE  dbo.APIRetryLog
--  Captures every iteration of the Until activity loop
--  so you can see the retry behaviour clearly.
-- ============================================================

IF OBJECT_ID('dbo.APIRetryLog','U') IS NOT NULL DROP TABLE dbo.APIRetryLog;
GO

CREATE TABLE dbo.APIRetryLog
(
    LogId            INT            NOT NULL IDENTITY(1,1) PRIMARY KEY,
    PipelineRunId    NVARCHAR(100)  NOT NULL,
    OrderId          INT            NOT NULL,
    AttemptNumber    INT            NOT NULL,
    AttemptedAt      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    APIEndpoint      NVARCHAR(500)  NOT NULL,
    HTTPStatus       INT            NULL,
    IsSuccess        BIT            NOT NULL DEFAULT 0,
    ResponseMessage  NVARCHAR(500)  NULL,
    WaitSeconds      INT            NOT NULL DEFAULT 0
);
GO


-- ============================================================
--  PART 4 — STORED PROCEDURE: mark order as processed
--  Called inside the ForEach after Until completes.
-- ============================================================

IF OBJECT_ID('dbo.usp_MarkOrderProcessed','P') IS NOT NULL
    DROP PROCEDURE dbo.usp_MarkOrderProcessed;
GO

CREATE PROCEDURE dbo.usp_MarkOrderProcessed
    @OrderId         INT,
    @SwitchBranch    NVARCHAR(50),
    @APIConfirmed    BIT,
    @APIRetries      INT,
    @ProcessStatus   NVARCHAR(30) = 'Success'
AS
BEGIN
    SET NOCOUNT ON;

    -- Insert into processed sink
    INSERT INTO dbo.ProcessedOrders
        (OrderId, CustomerId, ProductCategory, ProductName, SKU,
         Quantity, TotalAmount, Region, SwitchBranch, APIConfirmed, APIRetries, ProcessStatus)
    SELECT
        OrderId, CustomerId, ProductCategory, ProductName, SKU,
        Quantity, TotalAmount, Region,
        @SwitchBranch, @APIConfirmed, @APIRetries, @ProcessStatus
    FROM dbo.PendingOrders
    WHERE OrderId = @OrderId;

    -- Update status on source
    UPDATE dbo.PendingOrders
    SET    Status = @ProcessStatus
    WHERE  OrderId = @OrderId;
END;
GO


-- ============================================================
--  PART 5 — HELPER QUERIES to verify ADF pipeline runs
-- ============================================================

-- 1. Orders by Switch branch (what ADF will see)
SELECT
    ProductCategory                              AS SwitchOn,
    CASE ProductCategory
        WHEN 'Electronics' THEN 'Premium check'
        WHEN 'Clothing'    THEN 'Standard copy'
        WHEN 'Furniture'   THEN 'Bulk check'
        ELSE                    'Default branch'
    END                                          AS TargetBranch,
    COUNT(*)                                     AS OrderCount,
    SUM(CASE WHEN RequiresAPIConfirm=1 THEN 1 ELSE 0 END) AS NeedAPIConfirm
FROM dbo.PendingOrders
GROUP BY ProductCategory
ORDER BY ProductCategory;
GO

-- 2. Orders that will trigger the Until retry loop
SELECT OrderId, CustomerId, ProductCategory, ProductName, Priority
FROM   dbo.PendingOrders
WHERE  RequiresAPIConfirm = 1
ORDER  BY Priority DESC, OrderId;
GO

-- 3. Post-pipeline results check
SELECT
    p.SwitchBranch,
    COUNT(*)                                     AS Processed,
    SUM(CASE WHEN p.APIConfirmed=1 THEN 1 ELSE 0 END) AS APIConfirmed,
    AVG(p.APIRetries * 1.0)                      AS AvgRetries,
    MAX(p.APIRetries)                            AS MaxRetries
FROM dbo.ProcessedOrders p
GROUP BY p.SwitchBranch
ORDER BY p.SwitchBranch;
GO

-- 4. Full retry audit trail
SELECT
    r.OrderId,
    r.AttemptNumber,
    r.AttemptedAt,
    r.HTTPStatus,
    r.IsSuccess,
    r.WaitSeconds,
    r.ResponseMessage
FROM dbo.APIRetryLog r
ORDER BY r.OrderId, r.AttemptNumber;
GO
