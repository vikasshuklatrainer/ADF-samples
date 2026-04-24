# Azure Data Factory: Get Metadata & If Condition Activity Demo

---

## 📌 Scenario Overview

**Scenario: File Arrival Validation Before Processing**

A vendor drops a daily sales file (`sales_YYYYMMDD.csv`) into an **Azure Blob Storage** container.  
Before processing it, your pipeline must:

1. Use **Get Metadata Activity** to inspect the file — check if it **exists** and whether its **size is greater than 0 bytes** (non-empty).
2. Use **If Condition Activity** to branch:
   - ✅ **TRUE branch** → File is valid → Run a **Copy Activity** to move it to a processed zone.
   - ❌ **FALSE branch** → File is missing or empty → Send a failure notification via a **Web Activity** (e.g., call a Logic App or Teams webhook).

**Why this matters:**  
Processing a missing or empty file causes silent failures or corrupt data loads. This pattern adds a **defensive validation gate** before any data movement begins.

---

## 🏗️ Architecture

```
[Azure Blob Storage]
  └─ /landing/sales_20260423.csv
         │
         ▼
  ┌─────────────────────┐
  │  Get Metadata       │  ← Checks: exists? size > 0?
  └────────┬────────────┘
           │
           ▼
  ┌─────────────────────┐
  │   If Condition      │  ← Evaluates metadata output
  └────────┬────────────┘
     ┌─────┴──────┐
   TRUE          FALSE
     │              │
     ▼              ▼
 Copy File     Web Activity
 to /processed  (Alert Team)
```

---

## ✅ Prerequisites

- **Azure Data Factory** instance (v2)
- **Azure Blob Storage** with two containers:
  - `landing`   — where vendor drops the file
  - `processed` — destination for valid files
- A **Linked Service** for Azure Blob Storage: `LS_AzureBlobStorage`
- (Optional) A **Logic App** or **Teams Webhook URL** for failure alerts

---

## 🗂️ Step 1: Create Datasets

### 1a. Landing File Dataset (Source — Parameterized)

1. Go to **Author** → **Datasets** → **+ New Dataset**
2. Select **Azure Blob Storage** → **DelimitedText (CSV)**
3. Name it `DS_Landing_CSV`
4. Linked Service: `LS_AzureBlobStorage`
5. **Parameters tab** → Add:
   - `fileName` | Type: `String`
6. **Connection tab:**
   - Container: `landing`
   - File: Add dynamic content → `@dataset().fileName`
   - ✅ Check **First row as header**
7. **Publish All**

### 1b. Processed Zone Dataset (Sink)

1. **+ New Dataset** → Azure Blob Storage → DelimitedText
2. Name it `DS_Processed_CSV`
3. Linked Service: `LS_AzureBlobStorage`
4. **Parameters tab** → Add:
   - `fileName` | Type: `String`
5. **Connection tab:**
   - Container: `processed`
   - File: Add dynamic content → `@dataset().fileName`
6. **Publish All**

---

## 🔍 Step 2: Add the Get Metadata Activity

1. Go to **Author** → **Pipelines** → **+ New Pipeline**
2. Name it `PL_ValidateAndProcessFile`
3. Add a **Pipeline Parameter:**
   - **Name:** `p_fileName` | **Type:** `String` | **Default:** `sales_20260423.csv`
4. From the **Activities** panel, drag **Get Metadata** onto the canvas
5. Name it `GMT_CheckFile`
6. Click the activity → **Settings** tab:
   - **Dataset:** `DS_Landing_CSV`
   - **Dataset properties:** `fileName` → Add dynamic content:  
     `@pipeline().parameters.p_fileName`
   - **Field list** → Click **+ New** and add these two fields:

| Field List Item | What It Returns |
|---|---|
| `exists` | `true` / `false` — whether the file is present |
| `size` | File size in bytes |

7. Click **OK**

> 💡 Other useful metadata fields you can add: `itemName`, `itemType`, `lastModified`, `childItems` (for folders).

---

## 🔀 Step 3: Add the If Condition Activity

1. Drag an **If Condition** activity onto the canvas
2. Name it `IF_FileIsValid`
3. Draw a **Success** arrow from `GMT_CheckFile` → `IF_FileIsValid`
4. Click `IF_FileIsValid` → **Activities** tab → **Expression** field
5. Click **Add dynamic content** and enter the condition:

```
@and(
    equals(activity('GMT_CheckFile').output.exists, true),
    greater(activity('GMT_CheckFile').output.size, 0)
)
```

> **What this does:**
> - `activity('GMT_CheckFile').output.exists` → returns `true` if file was found
> - `activity('GMT_CheckFile').output.size` → returns file size in bytes
> - `@and(...)` → BOTH conditions must be true to proceed with the TRUE branch

---

## ✅ Step 4: Build the TRUE Branch (File is Valid)

1. Click the **pencil icon (✏️)** on `IF_FileIsValid` → select **True** activities
2. Drag a **Copy Data** activity onto the TRUE canvas
3. Name it `CPY_MoveToProcessed`

### Source Tab:
- **Dataset:** `DS_Landing_CSV`
- **Dataset properties:**
  - `fileName` → `@pipeline().parameters.p_fileName`

### Sink Tab:
- **Dataset:** `DS_Processed_CSV`
- **Dataset properties:**
  - `fileName` → `@pipeline().parameters.p_fileName`

### Settings Tab (Optional but Recommended):
- Enable **Delete files after completion** in the Source → moves the file instead of copying

4. Click **← back arrow** to return to the If Condition editor

---

## ❌ Step 5: Build the FALSE Branch (File is Missing or Empty)

1. While still in `IF_FileIsValid`, switch to **False** activities tab
2. Drag a **Web Activity** onto the FALSE canvas
3. Name it `WEB_SendFailureAlert`
4. Click the activity → **Settings** tab:
   - **URL:** Your Teams Webhook or Logic App HTTP trigger URL
   - **Method:** `POST`
   - **Headers:** Add header:
     - Name: `Content-Type` | Value: `application/json`
   - **Body:** Add dynamic content:

```json
{
    "title": "⚠️ ADF Pipeline Alert",
    "text": "File validation FAILED for: @{pipeline().parameters.p_fileName}. The file is either missing or empty in the landing container. Pipeline: @{pipeline().Pipeline}. Run ID: @{pipeline().RunId}."
}
```

5. Click **← back arrow** → back to the main pipeline canvas

---

## 🧪 Step 6: Debug and Test Both Branches

### Test 1 — TRUE Branch (Happy Path)

1. Upload a valid CSV file to the `landing` container named `sales_20260423.csv`
2. Click **Debug** in the pipeline toolbar
3. In the parameter dialog, enter: `p_fileName` = `sales_20260423.csv`
4. Watch the **Output** panel:
   - `GMT_CheckFile` → should show `exists: true`, `size: 1024` (or similar)
   - `IF_FileIsValid` → TRUE branch executes
   - `CPY_MoveToProcessed` → ✅ Succeeds
5. Verify `sales_20260423.csv` appears in the `processed` container

### Test 2 — FALSE Branch (File Missing)

1. Delete or rename the file in the `landing` container
2. Click **Debug** again with the same parameter
3. Watch the **Output** panel:
   - `GMT_CheckFile` → shows `exists: false`
   - `IF_FileIsValid` → FALSE branch executes
   - `WEB_SendFailureAlert` → ✅ Alert is sent to Teams/Logic App

---

## 📊 Step 7: Full Pipeline View Summary

```
Pipeline: PL_ValidateAndProcessFile
Parameter: p_fileName (String)

[GMT_CheckFile]  ── Get Metadata ──►  exists, size
        │ (on success)
        ▼
[IF_FileIsValid]  ── @and(exists=true, size>0)
        │
  ┌─────┴────────┐
  │ TRUE         │ FALSE
  ▼              ▼
[CPY_Move     [WEB_Send
 ToProcessed]  FailureAlert]
```

---

## 🔑 Key Expressions Reference

| Expression | Purpose |
|---|---|
| `@activity('GMT_CheckFile').output.exists` | Returns `true`/`false` for file existence |
| `@activity('GMT_CheckFile').output.size` | Returns file size in bytes (integer) |
| `@activity('GMT_CheckFile').output.lastModified` | Returns last modified timestamp |
| `@activity('GMT_CheckFile').output.itemName` | Returns the actual file name string |
| `@and(expr1, expr2)` | Both expressions must be true |
| `@or(expr1, expr2)` | Either expression must be true |
| `@greater(value, 0)` | Checks value is greater than 0 |
| `@equals(value, true)` | Checks value equals true |

---

## 🚨 Common Pitfalls to Avoid

- ❌ **Not adding `exists` to the Field List** — Without it, `output.exists` returns null and the If condition errors out.
- ❌ **Using `size == 0` to mean "missing"** — A missing file returns `exists: false`, not `size: 0`. Always check `exists` first.
- ❌ **Forgetting the `@and()`** — Checking only `exists` allows an empty (0-byte) file through, which can break downstream processes.
- ❌ **Hardcoding the filename** — Always use a pipeline parameter so the pipeline is reusable across dates and file names.
- ❌ **No FALSE branch** — Leaving the FALSE branch empty means failures are silently ignored. Always add an alert.

---

## 💡 Extension Ideas

| Enhancement | How |
|---|---|
| Check `lastModified` is today | Add `@equals(formatDateTime(activity('GMT_CheckFile').output.lastModified,'yyyy-MM-dd'), formatDateTime(utcNow(),'yyyy-MM-dd'))` to the condition |
| Validate minimum file size (e.g., > 1KB) | Replace `greater(..., 0)` with `greater(..., 1024)` |
| Loop over multiple files first | Wrap this pipeline with a ForEach that iterates a file list |
| Retry on failure | Set **Retry** = `2` and **Retry interval** = `30` sec on `GMT_CheckFile` |

---

*Generated for Azure Data Factory v2 | Last updated: April 2026*
