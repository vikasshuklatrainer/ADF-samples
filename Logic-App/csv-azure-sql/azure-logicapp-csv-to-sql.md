# Azure Logic App: CSV from Storage Account → Azure SQL Database

## Overview

This guide walks you through building an Azure Logic App that:
1. Detects a CSV file in an Azure Blob Storage container
2. Reads and parses the CSV content
3. Inserts each row as a record into an Azure SQL Database table

---

## Prerequisites

Before you begin, ensure the following are in place:

- An active **Azure Subscription**
- An **Azure Storage Account** with a Blob container (e.g., `csv-uploads`)
- A sample **CSV file** uploaded to the container
- An **Azure SQL Database** with a table matching your CSV columns
- Basic familiarity with the Azure Portal

---

## Step 1: Prepare the Azure SQL Database Table

1. Go to **Azure Portal** → **SQL Databases** → select your database.
2. Open **Query Editor** (or use SSMS / Azure Data Studio).
3. Create a table that matches your CSV structure. Example:

```sql
F OBJECT_ID('dbo.Employees', 'U') IS NOT NULL
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
```

> **Tip:** Make sure the column names and data types align with what's in your CSV file.

---

## Step 2: Upload a Sample CSV File to Azure Blob Storage

1. Go to **Azure Portal** → **Storage Accounts** → select your storage account.
2. Navigate to **Containers** → open your container (e.g., `csv-uploads`).
3. Click **Upload** and upload a CSV file. Example file content:

```
Name,Department,Salary,JoinDate,Email,City,Country,EmploymentType,YearsExperience,IsActive
Alice Johnson,Engineering,85000.00,2022-03-15,alice.johnson@company.com,New York,USA,Full-Time,5,true
Bob Smith,Marketing,72000.00,2021-07-01,bob.smith@company.com,Los Angeles,USA,Full-Time,3,true
Carol White,HR,68000.00,2023-01-10,carol.white@company.com,Chicago,USA,Part-Time,2,true
David Lee,Engineering,95000.00,2020-06-20,david.lee@company.com,San Francisco,USA,Full-Time,8,true
Eva Martinez,Finance,78000.00,2021-11-30,eva.martinez@company.com,Houston,USA,Full-Time,4,true
Frank Brown,Marketing,65000.00,2023-05-18,frank.brown@company.com,Phoenix,USA,Contract,1,true
Grace Kim,Engineering,91000.00,2019-09-25,grace.kim@company.com,Seattle,USA,Full-Time,7,true
Henry Davis,HR,62000.00,2022-08-14,henry.davis@company.com,Boston,USA,Part-Time,2,false
Isla Turner,Finance,80000.00,2020-12-03,isla.turner@company.com,Dallas,USA,Full-Time,6,true
James Wilson,Engineering,88000.00,2021-04-22,james.wilson@company.com,Austin,USA,Full-Time,5,true
```

> Save the file as `employees.csv`.

---

## Step 3: Create the Logic App

1. In the **Azure Portal**, search for **Logic Apps** in the top search bar.
2. Click **+ Create**.
3. Fill in the details:
   - **Subscription:** Select your subscription
   - **Resource Group:** Create new or select existing
   - **Logic App Name:** e.g., `csv-to-sql-logicapp`
   - **Region:** Choose a region close to your resources
   - **Plan Type:** Select **Consumption** (pay-per-use) for simplicity
4. Click **Review + Create** → **Create**.
5. Once deployed, click **Go to resource**.

---

## Step 4: Open the Logic App Designer

1. Inside your Logic App resource, click **Logic app designer** from the left menu.
2. On the template page, select **Blank Logic App** to start from scratch.

---

## Step 5: Add the Trigger — "When a blob is added or modified"

This trigger fires whenever a new CSV is uploaded to Blob Storage.

1. In the designer search box, type `Azure Blob Storage`.
2. Select the trigger: **"When a blob is added or modified (properties only)"**
3. If prompted, create a new connection:
   - **Connection Name:** e.g., `BlobStorageConnection`
   - **Authentication Type:** Select `Access Key`
   - Paste your **Storage Account Name** and **Access Key** (found under Storage Account → Access Keys)
   - Click **Create**
4. Configure the trigger:
   - **Storage account name:** Select or enter your storage account
   - **Container:** Select your container (e.g., `/csv-uploads`)
   - **How often do you want to check for items?** Set interval, e.g., `1 Minute`
5. Click **Save**.

---

## Step 6: Add Action — "Get blob content"

The trigger only provides metadata. This action fetches the actual file content.

1. Click **+ New step**.
2. Search for `Azure Blob Storage` → select action: **"Get blob content"**
3. Configure:
   - **Storage Account Name:** Select the same connection
   - **Blob:** Click inside the field → select **Dynamic content** → choose **`List of Files Path`** (from the trigger output)
4. Click **Save**.

---

## Step 7: Add Action — Initialize a Variable (Row Counter / CSV Text)

We'll store the blob content as a string variable for parsing.

1. Click **+ New step**.
2. Search for `Variables` → select **"Initialize variable"**
3. Configure:
   - **Name:** `csvContent`
   - **Type:** `String`
   - **Value:** Click **Dynamic content** → select **`File Content`** from the "Get blob content" step
4. Click **Save**.

---

## Step 8: Parse CSV Using a Compose Action

Azure Logic Apps doesn't have a built-in CSV parser, so we'll use a workaround with **inline code** or **split functions**.

### Option A: Using "Execute JavaScript Code" (Recommended)

> **Note:** This requires the Logic Apps **Standard** plan or the **Integration Account**. If using Consumption plan, use Option B.

1. Click **+ New step** → search **Inline Code** → select **"Execute JavaScript Code"**
2. In the code editor, enter:

```javascript
var csv = workflowContext.actions.Initialize_variable.outputs.body;
var lines = csv.trim().split('\n');
var headers = lines[0].split(',');
var rows = [];

for (var i = 1; i < lines.length; i++) {
    var values = lines[i].split(',');
    var row = {};
    for (var j = 0; j < headers.length; j++) {
        row[headers[j].trim()] = values[j] ? values[j].trim() : null;
    }
    rows.push(row);
}

return rows;
```

3. Click **Save**.

---

### Option B: Using Split + For Each (Consumption Plan)

If you're on a Consumption plan, follow this approach using built-in expressions.

#### 8.1 — Split CSV into Lines

1. Click **+ New step** → search **Variables** → **"Initialize variable"**
2. Configure:
   - **Name:** `csvLines`
   - **Type:** `Array`
   - **Value (Expression):**
     ```
     skip(split(variables('csvContent'), '\n'), 1)
     ```
     *(This splits by newline and skips the header row)*

#### 8.2 — Add a "For Each" Loop

1. Click **+ New step** → search **Control** → select **"For each"**
2. In the **"Select an output from previous steps"** field, choose `csvLines` from Dynamic content.

#### 8.3 — Inside the Loop: Parse Each Line

1. Inside the For Each loop, click **Add an action**.
2. Search **Variables** → **"Initialize variable"** (or use **Compose**).
3. Use the following expressions to extract individual column values from each line.

Add a **Compose** action and set its input to:

```
split(item(), ',')
```

This splits the current CSV row into an array of values.

---

## Step 9: Insert Data into Azure SQL Database

### 9.1 — Add SQL "Insert Row" Action

1. Inside the **For Each** loop (after the Compose), click **Add an action**.
2. Search for **SQL Server** → select **"Insert row (V2)"**
3. Create a new connection:
   - **Connection Name:** e.g., `SQLConnection`
   - **Authentication Type:** `SQL Server Authentication`
   - **SQL Server Name:** Your server (e.g., `myserver.database.windows.net`)
   - **SQL Database Name:** Your database name
   - **Username:** SQL admin username
   - **Password:** SQL admin password
   - Click **Create**
4. Configure the Insert Row action:
   - **Server Name:** Select from dropdown
   - **Database Name:** Select your database
   - **Table Name:** Select `Employees` (or your table name)

5. Map CSV values to table columns using expressions. Example (index-based from the Compose output):

| Column       | Expression (Dynamic Content / Expression)            |
|--------------|------------------------------------------------------|
| `Name`       | `outputs('Compose')[0]`                              |
| `Department` | `outputs('Compose')[1]`                              |
| `Salary`     | `float(trim(outputs('Compose')[2]))`                 |
| `JoinDate`   | `trim(outputs('Compose')[3])`                        |

> **Tip:** Use the **Expression** tab in the Dynamic content panel to enter these formulas.

6. Click **Save**.

---

## Step 10: Add Error Handling (Optional but Recommended)

1. Click the **three dots (…)** on the For Each loop → **Configure run after**.
2. Optionally wrap the SQL insert in a **Scope** action and add a parallel branch to handle failures (send an email, log to a table, etc.).

To add an email alert on failure:
1. Add a step after the For Each: **"Send an email (V2)"** using Outlook or Gmail connector.
2. Set the condition to run **"after → has failed"**.

---

## Step 11: Test the Logic App

1. Go to your Logic App → click **Run Trigger** → **Run**.
2. Alternatively, upload a new CSV to the blob container to trigger it automatically.
3. Monitor execution under **Runs history** in the Overview pane.
4. Click on a run to inspect each step — green = success, red = failure.
5. Verify data in your SQL table:

```sql
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
```

---

## Step 12: Review Full Logic App Flow

```
[Trigger]  When a blob is added or modified (every 1 min)
     ↓
[Action 1] Get blob content
     ↓
[Action 2] Initialize variable → csvContent (string)
     ↓
[Action 3] Initialize variable → csvLines (array, skip header)
     ↓
[Action 4] For Each → loop over csvLines
              ↓
         [Action 4a] Compose → split(item(), ',')
              ↓
         [Action 4b] SQL Server → Insert row into Employees table
```

---

## Common Issues & Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| Trigger not firing | Polling interval too long | Set interval to 1 minute for testing |
| Blob content is Base64 | Logic Apps encodes binary blobs | Use expression `base64ToString(body('Get_blob_content'))` |
| SQL connection fails | Firewall blocking Logic App IPs | Add Logic App outbound IPs to SQL Server firewall rules |
| Column mismatch | CSV headers don't match SQL columns | Verify column names/order in both |
| Empty rows inserted | CSV has trailing newlines | Add a `trim()` and filter empty strings before the loop |
| Type mismatch error | String inserted into numeric column | Wrap with `int()`, `float()`, or `formatDateTime()` |

---

## Tips for Production Use

- **Use Managed Identity** instead of access keys for more secure connections.
- **Enable diagnostic logs** on the Logic App for monitoring.
- **Archive processed files** by moving them to a different container after successful processing.
- **Use Azure Key Vault** to store SQL credentials securely.
- Consider **Azure Data Factory** for large CSV files (thousands of rows), as Logic Apps has action limits per run.

---

*Guide covers Azure Logic App (Consumption & Standard plans) — Azure Portal UI as of 2024/2025.*
