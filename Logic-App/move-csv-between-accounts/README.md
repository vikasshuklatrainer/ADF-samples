#  Azure Logic App: Move CSV Between Storage Accounts

##  Overview
This Logic App moves a CSV file from one Azure Storage Account (source) to another (destination).

---

## ️ Architecture

Source Blob Storage → Logic App → Destination Blob Storage

---

##  Prerequisites

- Two Azure Storage Accounts
- Blob containers in both accounts
- Logic App (Consumption)

---

##  Steps

### 1. Trigger
Use **When a blob is added or modified (properties only)**

- Storage Account: Source
- Container: source-container

---

### 2. Get Blob Content

Action: **Get blob content**

- Use dynamic content from trigger

---

### 3. Create Blob in Destination

Action: **Create blob**

- Storage Account: Destination
- Container: dest-container
- Blob Name: @{triggerOutputs()?['headers']['x-ms-file-name']}
- Blob Content: Output of previous step

---

### 4. (Optional) Delete Source File

Action: **Delete blob**

- Cleans up source after move

---

##  Example

Input:
```
file1.csv
```

Output:
```
file1.csv (moved to destination)
```

---

##  Best Practices

- Use Managed Identity
- Add retry policies
- Handle duplicate file names

---

##  Testing

1. Upload CSV to source container
2. Check destination container
3. Verify file moved

---

## Conclusion

This Logic App enables automated file movement across storage accounts.
