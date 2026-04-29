IF OBJECT_ID('RandomTable_9', 'U') IS NOT NULL
    DROP TABLE RandomTable_9;

CREATE TABLE RandomTable_9 (
Id INT IDENTITY(1,1) PRIMARY KEY,
ProductName NVARCHAR(100),
Category NVARCHAR(50),
Price DECIMAL(10,2),
Quantity INT,
CreatedDate DATETIME
);
