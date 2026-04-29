#  Azure Logic App: JSON to SQL Ingestion

##  Overview
This Logic App reads a JSON file from Azure Blob Storage and inserts records into an Azure SQL table.

---

## ️ Architecture

Blob Storage (JSON File)
        ↓
Logic App Trigger
        ↓
Get Blob Content
        ↓
Parse JSON
        ↓
For Each Record
        ↓
Insert Row (SQL)

---

##  Prerequisites

- Azure Storage Account
- Azure SQL Database
- Logic App (Consumption/Standard)
- store the json in azure storage account
```json
[
  {
    "Name": "Alice Johnson",
    "Department": "Engineering",
    "Salary": 85000.00,
    "JoinDate": "2022-03-15",
    "Email": "alice.johnson@company.com",
    "City": "New York",
    "Country": "USA",
    "EmploymentType": "Full-Time",
    "YearsExperience": 5,
    "IsActive": true
  },
  {
    "Name": "Bob Smith",
    "Department": "Marketing",
    "Salary": 72000.00,
    "JoinDate": "2021-07-01",
    "Email": "bob.smith@company.com",
    "City": "Los Angeles",
    "Country": "USA",
    "EmploymentType": "Full-Time",
    "YearsExperience": 3,
    "IsActive": true
  },
  {
    "Name": "Carol White",
    "Department": "HR",
    "Salary": 68000.00,
    "JoinDate": "2023-01-10",
    "Email": "carol.white@company.com",
    "City": "Chicago",
    "Country": "USA",
    "EmploymentType": "Part-Time",
    "YearsExperience": 2,
    "IsActive": true
  },
  {
    "Name": "David Lee",
    "Department": "Engineering",
    "Salary": 95000.00,
    "JoinDate": "2020-06-20",
    "Email": "david.lee@company.com",
    "City": "San Francisco",
    "Country": "USA",
    "EmploymentType": "Full-Time",
    "YearsExperience": 8,
    "IsActive": true
  },
  {
    "Name": "Eva Martinez",
    "Department": "Finance",
    "Salary": 78000.00,
    "JoinDate": "2021-11-30",
    "Email": "eva.martinez@company.com",
    "City": "Houston",
    "Country": "USA",
    "EmploymentType": "Full-Time",
    "YearsExperience": 4,
    "IsActive": true
  },
  {
    "Name": "Frank Brown",
    "Department": "Marketing",
    "Salary": 65000.00,
    "JoinDate": "2023-05-18",
    "Email": "frank.brown@company.com",
    "City": "Phoenix",
    "Country": "USA",
    "EmploymentType": "Contract",
    "YearsExperience": 1,
    "IsActive": true
  },
  {
    "Name": "Grace Kim",
    "Department": "Engineering",
    "Salary": 91000.00,
    "JoinDate": "2019-09-25",
    "Email": "grace.kim@company.com",
    "City": "Seattle",
    "Country": "USA",
    "EmploymentType": "Full-Time",
    "YearsExperience": 7,
    "IsActive": true
  },
  {
    "Name": "Henry Davis",
    "Department": "HR",
    "Salary": 62000.00,
    "JoinDate": "2022-08-14",
    "Email": "henry.davis@company.com",
    "City": "Boston",
    "Country": "USA",
    "EmploymentType": "Part-Time",
    "YearsExperience": 2,
    "IsActive": false
  },
  {
    "Name": "Isla Turner",
    "Department": "Finance",
    "Salary": 80000.00,
    "JoinDate": "2020-12-03",
    "Email": "isla.turner@company.com",
    "City": "Dallas",
    "Country": "USA",
    "EmploymentType": "Full-Time",
    "YearsExperience": 6,
    "IsActive": true
  },
  {
    "Name": "James Wilson",
    "Department": "Engineering",
    "Salary": 88000.00,
    "JoinDate": "2021-04-22",
    "Email": "james.wilson@company.com",
    "City": "Austin",
    "Country": "USA",
    "EmploymentType": "Full-Time",
    "YearsExperience": 5,
    "IsActive": true
  }
]
```


---

## SQL Table Schema

```sql
CREATE TABLE Employees (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Name VARCHAR(100),
    Department VARCHAR(50),
    Salary DECIMAL(10,2),
    JoinDate DATE,
    Email VARCHAR(100),
    City VARCHAR(50),
    Country VARCHAR(50),
    EmploymentType VARCHAR(50),
    YearsExperience INT,
    IsActive BIT
);
```

---

##  Implementation Steps

### 1. Trigger
Use: When a blob is added or modified OR Recurrence

### 2. Get Blob Content
Use Azure Blob Storage connector

### 3. Parse JSON

```json
{
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "Name": { "type": "string" },
      "Department": { "type": "string" },
      "Salary": { "type": "number" },
      "JoinDate": { "type": "string" },
      "Email": { "type": "string" },
      "City": { "type": "string" },
      "Country": { "type": "string" },
      "EmploymentType": { "type": "string" },
      "YearsExperience": { "type": "integer" },
      "IsActive": { "type": "boolean" }
    }
  }
}
```

### 4. For Each
Loop through parsed JSON array

### 5. Insert Row
Use Insert Row (V2) and map fields

---

## Testing

1. Upload JSON file
2. Run Logic App
3. Verify SQL table

---

## Conclusion

Serverless pipeline to ingest JSON into SQL using Azure Logic Apps.
