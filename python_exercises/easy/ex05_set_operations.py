# ============================================================
# Exercise 5 — Set Operations on Column Names          [EASY]
# Topic: Data Types
# ============================================================
#
# TASK:
# Two DataFrames must be compared before a JOIN.
# Given the two sets of column names below, find:
#   a) columns in BOTH tables      (JOIN keys)
#   b) columns ONLY in table A
#   c) columns ONLY in table B
#   d) total number of unique columns across both
#
# HINT:
# Convert lists to set. Use:
#   &  for intersection
#   -  for difference
#   |  for union
# ============================================================

cols_a = {'id', 'name', 'email', 'created_at', 'region'}
cols_b = {'id', 'order_id', 'amount', 'region', 'status'}

# --- YOUR CODE HERE ---


# ============================================================
# SOLUTION
# ============================================================

def solution():
    cols_a = {'id', 'name', 'email', 'created_at', 'region'}
    cols_b = {'id', 'order_id', 'amount', 'region', 'status'}

    common   = cols_a & cols_b   # intersection
    only_a   = cols_a - cols_b   # left difference
    only_b   = cols_b - cols_a   # right difference
    all_cols = cols_a | cols_b   # union

    print(f"JOIN keys (common):  {sorted(common)}")
    print(f"Only in A:           {sorted(only_a)}")
    print(f"Only in B:           {sorted(only_b)}")
    print(f"All unique cols:     {len(all_cols)}")

    # JOIN keys (common):  ['id', 'region']
    # Only in A:           ['created_at', 'email', 'name']
    # Only in B:           ['amount', 'order_id', 'status']
    # All unique cols:     8

if __name__ == "__main__":
    solution()
