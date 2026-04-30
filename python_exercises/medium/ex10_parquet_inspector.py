# ============================================================
# Exercise 10 — Parquet Schema Inspector              [MEDIUM]
# Topic: File Handling
# ============================================================
#
# REQUIRES: pip install pyarrow pandas
#
# TASK:
# Create a PyArrow Table from the data dict below.
# Then:
#   1. Print the schema (column name, type, nullable)
#   2. Print row and column counts
#   3. Write to "students.parquet" with Snappy compression
#   4. Read it back and convert to a Pandas DataFrame
#
# HINT:
# Use pa.Table.from_pydict(data) to create the table.
# Access schema with table.schema — it is iterable.
# Each field has .name, .type, and .nullable attributes.
# Use pq.write_table() and pq.read_table() for file I/O.
# ============================================================

import pyarrow as pa
import pyarrow.parquet as pq

data = {
    "id":        [1, 2, 3],
    "name":      ["Alice", "Bob", "Carol"],
    "score":     [88.5, 72.0, 95.3],
    "is_active": [True, False, True],
}

# --- YOUR CODE HERE ---


# ============================================================
# SOLUTION
# ============================================================

def solution():
    table = pa.Table.from_pydict(data)

    print("Schema:")
    for field in table.schema:
        print(f"  {field.name:<12} {str(field.type):<10} nullable={field.nullable}")

    print(f"\nRows: {table.num_rows}, Cols: {table.num_columns}")

    # Write to Parquet
    pq.write_table(table, "students.parquet", compression="snappy")
    print("\nWritten to students.parquet")

    # Read back and convert to pandas
    df = pq.read_table("students.parquet").to_pandas()
    print("\nDataFrame:")
    print(df)

if __name__ == "__main__":
    solution()
