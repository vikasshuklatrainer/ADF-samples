-- ============================================================
-- Create Database (run this only if starting fresh)
-- ============================================================
-- CREATE DATABASE CompanyDB;
-- GO

-- ============================================================
-- Use the target database
-- ============================================================
USE CompanyDB;
GO

-- ============================================================
-- Drop table if it already exists (for re-runs / testing)
-- ============================================================
IF OBJECT_ID('dbo.Employees', 'U') IS NOT NULL
    DROP TABLE dbo.Employees;
GO

-- ============================================================
-- Create the Employees table
-- ============================================================
CREATE TABLE dbo.Employees (
    Id                INT             IDENTITY(1,1)   NOT NULL,   -- Auto-increment PK
    Name              NVARCHAR(100)                   NOT NULL,   -- Full name
    Department        NVARCHAR(100)                   NOT NULL,   -- Department name
    Salary            DECIMAL(10, 2)                  NOT NULL,   -- e.g. 85000.00
    JoinDate          DATE                            NOT NULL,   -- e.g. 2022-03-15
    Email             NVARCHAR(150)                   NOT NULL,   -- Unique email
    City              NVARCHAR(100)                       NULL,   -- City of work
    Country           NVARCHAR(100)                       NULL,   -- Country
    EmploymentType    NVARCHAR(50)                        NULL,   -- Full-Time / Part-Time / Contract
    YearsExperience   INT                                 NULL,   -- Years of experience
    IsActive          BIT                             NOT NULL    -- 1 = Active, 0 = Inactive
        DEFAULT 1,
    CreatedAt         DATETIME2       DEFAULT GETUTCDATE(),       -- Auto-set on insert
    CONSTRAINT PK_Employees PRIMARY KEY CLUSTERED (Id ASC),
    CONSTRAINT UQ_Employees_Email UNIQUE (Email)
);
GO

-- ============================================================
-- Optional: Add indexes for common query patterns
-- ============================================================
CREATE NONCLUSTERED INDEX IX_Employees_Department
    ON dbo.Employees (Department ASC);

CREATE NONCLUSTERED INDEX IX_Employees_JoinDate
    ON dbo.Employees (JoinDate ASC);
GO

-- ============================================================
-- Verify the table was created
-- ============================================================
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Employees'
ORDER BY ORDINAL_POSITION;
GO