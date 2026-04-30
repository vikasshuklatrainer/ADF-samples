# ============================================================
# Exercise 8 — CSV Reader and Aggregator              [MEDIUM]
# Topic: File Handling
# ============================================================
#
# TASK:
# Parse the CSV string below using Python's built-in csv module.
# Calculate the total and average amount per region, and print
# a formatted summary table.
#
# Expected output:
#   Region     Total    Average  Count
#   --------------------------------------
#   East      555.00    185.00      3
#   North     510.00    255.00      2
#   West      395.75    197.88      2
#
# HINT:
# Use csv.DictReader with io.StringIO to parse the string.
# Group by region using a defaultdict(list).
# ============================================================

import csv, io
from collections import defaultdict

raw_csv = """date,region,amount
2024-01-01,East,150.00
2024-01-01,West,220.50
2024-01-02,East,310.00
2024-01-02,North,80.00
2024-01-03,West,175.25
2024-01-03,East,95.00
2024-01-04,North,430.00"""

# --- YOUR CODE HERE ---


# ============================================================
# SOLUTION
# ============================================================

def solution():
    reader = csv.DictReader(io.StringIO(raw_csv))
    by_region = defaultdict(list)

    for row in reader:
        by_region[row['region']].append(float(row['amount']))

    print(f"{'Region':<8} {'Total':>10} {'Average':>10} {'Count':>6}")
    print("-" * 38)
    for region, amounts in sorted(by_region.items()):
        total = sum(amounts)
        avg   = total / len(amounts)
        print(f"{region:<8} {total:>10.2f} {avg:>10.2f} {len(amounts):>6}")

if __name__ == "__main__":
    solution()
