-- Check all inserted records
SELECT * FROM dbo.Employees ORDER BY Id;

-- Count by Department
SELECT Department, COUNT(*) AS HeadCount, AVG(Salary) AS AvgSalary
FROM dbo.Employees
GROUP BY Department
ORDER BY HeadCount DESC;

-- Filter active full-time employees
SELECT Name, Email, Salary
FROM dbo.Employees
WHERE IsActive = 1 AND EmploymentType = 'Full-Time'
ORDER BY Salary DESC;