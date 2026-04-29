CREATE TABLE Employees (
    Id INT PRIMARY KEY,
    Name VARCHAR(100),
    Department VARCHAR(50),
    Salary INT
);

INSERT INTO Employees (Id, Name, Department, Salary) VALUES
(1, 'Alice', 'IT', 60000),
(2, 'Bob', 'HR', 50000),
(3, 'Charlie', 'Finance', 70000),
(4, 'David', 'IT', 65000);
