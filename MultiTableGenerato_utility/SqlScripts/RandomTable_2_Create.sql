IF OBJECT_ID('RandomTable_2', 'U') IS NOT NULL
    DROP TABLE RandomTable_2;

CREATE TABLE RandomTable_2 (
Id INT IDENTITY(1,1) PRIMARY KEY,
ProductName NVARCHAR(100),
Category NVARCHAR(50),
Price DECIMAL(10,2),
Quantity INT,
CreatedDate DATETIME
);
