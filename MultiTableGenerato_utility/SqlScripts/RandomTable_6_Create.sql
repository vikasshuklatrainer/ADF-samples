IF OBJECT_ID('RandomTable_6', 'U') IS NOT NULL
    DROP TABLE RandomTable_6;

CREATE TABLE RandomTable_6 (
Id INT IDENTITY(1,1) PRIMARY KEY,
ProductName NVARCHAR(100),
Category NVARCHAR(50),
Price DECIMAL(10,2),
Quantity INT,
CreatedDate DATETIME
);
