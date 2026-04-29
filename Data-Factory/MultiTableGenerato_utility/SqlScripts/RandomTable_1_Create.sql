IF OBJECT_ID('RandomTable_1', 'U') IS NOT NULL
    DROP TABLE RandomTable_1;

CREATE TABLE RandomTable_1 (
Id INT IDENTITY(1,1) PRIMARY KEY,
ProductName NVARCHAR(100),
Category NVARCHAR(50),
Price DECIMAL(10,2),
Quantity INT,
CreatedDate DATETIME
);
