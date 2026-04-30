-- ============================================================
-- STEP 2: DATA WAREHOUSE - Azure Synapse Analytics / SQL Pool
-- Star Schema DDL Scripts
-- ============================================================

-- -------------------------------------------------------
-- DIMENSION: DimDate
-- -------------------------------------------------------
CREATE TABLE dbo.DimDate (
    date_key        INT           NOT NULL PRIMARY KEY,  -- e.g. 20240105
    full_date       DATE          NOT NULL,
    day_of_week     TINYINT       NOT NULL,              -- 1=Monday ... 7=Sunday
    day_name        VARCHAR(10)   NOT NULL,
    day_of_month    TINYINT       NOT NULL,
    day_of_year     SMALLINT      NOT NULL,
    week_of_year    TINYINT       NOT NULL,
    month_num       TINYINT       NOT NULL,
    month_name      VARCHAR(10)   NOT NULL,
    quarter_num     TINYINT       NOT NULL,
    year_num        SMALLINT      NOT NULL,
    is_weekend      BIT           NOT NULL DEFAULT 0
);

-- Populate DimDate for Jan 2024
INSERT INTO dbo.DimDate VALUES
(20240105, '2024-01-05', 5, 'Friday',    5,  5, 1, 1, 'January', 1, 2024, 0),
(20240106, '2024-01-06', 6, 'Saturday',  6,  6, 1, 1, 'January', 1, 2024, 1),
(20240107, '2024-01-07', 7, 'Sunday',    7,  7, 1, 1, 'January', 1, 2024, 1),
(20240108, '2024-01-08', 1, 'Monday',    8,  8, 2, 1, 'January', 1, 2024, 0),
(20240109, '2024-01-09', 2, 'Tuesday',   9,  9, 2, 1, 'January', 1, 2024, 0),
(20240110, '2024-01-10', 3, 'Wednesday',10, 10, 2, 1, 'January', 1, 2024, 0),
(20240111, '2024-01-11', 4, 'Thursday', 11, 11, 2, 1, 'January', 1, 2024, 0),
(20240112, '2024-01-12', 5, 'Friday',   12, 12, 2, 1, 'January', 1, 2024, 0),
(20240113, '2024-01-13', 6, 'Saturday', 13, 13, 2, 1, 'January', 1, 2024, 1),
(20240114, '2024-01-14', 7, 'Sunday',   14, 14, 2, 1, 'January', 1, 2024, 1);

-- -------------------------------------------------------
-- DIMENSION: DimCustomer
-- -------------------------------------------------------
CREATE TABLE dbo.DimCustomer (
    customer_key    INT           NOT NULL PRIMARY KEY IDENTITY(1,1),
    customer_id     VARCHAR(10)   NOT NULL,   -- natural key from source
    first_name      VARCHAR(50)   NOT NULL,
    last_name       VARCHAR(50)   NOT NULL,
    full_name       VARCHAR(100)  NOT NULL,
    email           VARCHAR(100)  NULL,
    city            VARCHAR(50)   NULL,
    state           CHAR(2)       NULL,
    country         VARCHAR(50)   NULL,
    segment         VARCHAR(20)   NULL,       -- Premium / Standard / Basic
    signup_date     DATE          NULL,
    -- SCD Type 2 columns
    row_effective_date  DATE      NOT NULL DEFAULT GETDATE(),
    row_expiry_date     DATE      NULL,
    is_current          BIT       NOT NULL DEFAULT 1
);

INSERT INTO dbo.DimCustomer
    (customer_id, first_name, last_name, full_name, email, city, state, country, segment, signup_date)
VALUES
('C001','Alice','Johnson','Alice Johnson','alice.j@email.com','Chicago','IL','USA','Premium','2022-03-15'),
('C002','Bob','Smith','Bob Smith','bob.s@email.com','Houston','TX','USA','Standard','2021-11-20'),
('C003','Carol','Williams','Carol Williams','carol.w@email.com','New York','NY','USA','Standard','2023-01-08'),
('C004','David','Brown','David Brown','david.b@email.com','Phoenix','AZ','USA','Basic','2022-07-30'),
('C005','Eve','Davis','Eve Davis','eve.d@email.com','Los Angeles','CA','USA','Premium','2021-05-12'),
('C006','Frank','Miller','Frank Miller','frank.m@email.com','San Antonio','TX','USA','Basic','2023-04-01'),
('C007','Grace','Wilson','Grace Wilson','grace.w@email.com','San Diego','CA','USA','Standard','2022-09-17'),
('C008','Henry','Moore','Henry Moore','henry.m@email.com','Dallas','TX','USA','Premium','2021-12-03'),
('C009','Iris','Taylor','Iris Taylor','iris.t@email.com','San Jose','CA','USA','Basic','2023-06-22'),
('C010','Jack','Anderson','Jack Anderson','jack.a@email.com','Austin','TX','USA','Standard','2022-02-14');

-- -------------------------------------------------------
-- DIMENSION: DimProduct
-- -------------------------------------------------------
CREATE TABLE dbo.DimProduct (
    product_key     INT           NOT NULL PRIMARY KEY IDENTITY(1,1),
    product_id      VARCHAR(10)   NOT NULL,
    product_name    VARCHAR(100)  NOT NULL,
    category        VARCHAR(50)   NOT NULL,
    sub_category    VARCHAR(50)   NOT NULL,
    brand           VARCHAR(50)   NULL,
    cost_price      DECIMAL(10,2) NOT NULL,
    list_price      DECIMAL(10,2) NOT NULL,
    sku             VARCHAR(50)   NULL
);

INSERT INTO dbo.DimProduct
    (product_id, product_name, category, sub_category, brand, cost_price, list_price, sku)
VALUES
('P101','Wireless Mouse','Electronics','Peripherals','TechBrand',12.50,29.99,'SKU-WM-001'),
('P102','Bluetooth Headphones','Electronics','Audio','SoundPro',65.00,149.99,'SKU-BH-002'),
('P103','USB-C Cable 2m','Electronics','Accessories','CableCo',2.10,9.99,'SKU-UC-003'),
('P104','AA Batteries (4-pack)','Electronics','Power','EnergyMax',1.50,4.99,'SKU-BA-004'),
('P105','Mechanical Keyboard','Electronics','Peripherals','TechBrand',120.00,299.99,'SKU-MK-005');

-- -------------------------------------------------------
-- DIMENSION: DimRegion
-- -------------------------------------------------------
CREATE TABLE dbo.DimRegion (
    region_key      INT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    region_name     VARCHAR(20)  NOT NULL,
    region_code     CHAR(1)      NOT NULL
);

INSERT INTO dbo.DimRegion (region_name, region_code) VALUES
('North','N'), ('South','S'), ('East','E'), ('West','W');

-- -------------------------------------------------------
-- FACT TABLE: FactSales
-- -------------------------------------------------------
CREATE TABLE dbo.FactSales (
    sale_key            BIGINT        NOT NULL PRIMARY KEY IDENTITY(1,1),
    -- Foreign keys to dimensions
    date_key            INT           NOT NULL REFERENCES dbo.DimDate(date_key),
    customer_key        INT           NOT NULL REFERENCES dbo.DimCustomer(customer_key),
    product_key         INT           NOT NULL REFERENCES dbo.DimProduct(product_key),
    region_key          INT           NOT NULL REFERENCES dbo.DimRegion(region_key),
    -- Degenerate dimension (no dimension table needed)
    transaction_id      VARCHAR(20)   NOT NULL,
    -- Measures
    quantity            INT           NOT NULL,
    unit_price          DECIMAL(10,2) NOT NULL,
    discount_amount     DECIMAL(10,2) NOT NULL DEFAULT 0,
    gross_sales         DECIMAL(10,2) NOT NULL,   -- quantity * unit_price
    net_sales           DECIMAL(10,2) NOT NULL,   -- gross_sales - discount
    cost_of_goods       DECIMAL(10,2) NOT NULL,   -- quantity * cost_price
    gross_profit        DECIMAL(10,2) NOT NULL    -- net_sales - cost_of_goods
);

-- ADF Data Flow will populate this by joining the staged data to dimension tables
-- Sample of what ADF Mapping Data Flow produces:
INSERT INTO dbo.FactSales
    (date_key, customer_key, product_key, region_key, transaction_id,
     quantity, unit_price, discount_amount, gross_sales, net_sales, cost_of_goods, gross_profit)
SELECT
    CAST(FORMAT(t.transaction_date, 'yyyyMMdd') AS INT)  AS date_key,
    dc.customer_key,
    dp.product_key,
    dr.region_key,
    t.transaction_id,
    t.quantity,
    t.unit_price,
    t.discount,
    t.quantity * t.unit_price                                              AS gross_sales,
    (t.quantity * t.unit_price) - t.discount                              AS net_sales,
    t.quantity * dp.cost_price                                            AS cost_of_goods,
    ((t.quantity * t.unit_price) - t.discount) - (t.quantity * dp.cost_price) AS gross_profit
FROM staging.SalesTransactions t
JOIN dbo.DimCustomer dc ON dc.customer_id = t.customer_id AND dc.is_current = 1
JOIN dbo.DimProduct  dp ON dp.product_id  = t.product_id
JOIN dbo.DimRegion   dr ON dr.region_name = t.region;
