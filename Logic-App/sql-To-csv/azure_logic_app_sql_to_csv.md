#  Azure Logic App: SQL to CSV Export

##  Overview

This document describes how to create an automated workflow using Microsoft Azure Logic Apps to:

- Extract data from an Azure SQL table  
- Convert the data into CSV format  
- Store the CSV file in Azure Blob Storage  

---

## ️ Architecture

Recurrence Trigger
      ↓
Azure SQL Database (Get Rows)
      ↓
Create CSV Table
      ↓
Azure Blob Storage (Create Blob)

---

##  Prerequisites

- Azure Subscription  
- Azure SQL Database with sample table  
- Storage Account (Blob Storage enabled)  
- Logic App (Consumption or Standard)

---

##  Step-by-Step Implementation

### 1. Create Resource Group

```bash
az group create --name rg-logicapp-demo --location centralindia
```

---

### 2. Create Storage Account

- Go to Azure Portal  
- Create Storage Account  
- Enable Blob Storage  

---

### 3. Create SQL Table

```sql
CREATE TABLE Employees (
    Id INT PRIMARY KEY,
    Name VARCHAR(100),
    Department VARCHAR(50),
    Salary INT
);

INSERT INTO Employees VALUES
(1, 'Alice', 'IT', 60000),
(2, 'Bob', 'HR', 50000),
(3, 'Charlie', 'Finance', 70000),
(4, 'David', 'IT', 65000);
```

---

### 4. Create Logic App

- Navigate to Azure Portal  
- Create → Logic App (Consumption recommended)  

---

### 5. Add Trigger

- Choose: Recurrence  
- Example: Every 1 hour  

---

### 6. Add SQL Action

Action: Get rows (V2)

```sql
SELECT * FROM Employees
```

---

### 7. Convert to CSV

Add action: Create CSV Table  

- Input: value (from SQL output)

---

### 8. Store CSV in Blob Storage

Add action: Create Blob  

- Container: data  
- Blob Name:
```
employees_@{utcNow()}.csv
```

---

##  Sample Output

```csv
Id,Name,Department,Salary
1,Alice,IT,60000
2,Bob,HR,50000
3,Charlie,Finance,70000
4,David,IT,65000
```

---

## Best Practices

- Use Managed Identity instead of credentials  
- Enable pagination for large data  
- Add error handling (Scope + alerts)  

---



## Conclusion

This solution provides a scalable and serverless way to export SQL data into CSV using Azure Logic Apps.
