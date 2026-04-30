"""
=============================================================
 PANDAS & NUMPY FEATURE DEMONSTRATION
 Using: raw_sales_transactions.csv / raw_customers.csv / raw_products.csv
=============================================================
 Topics covered:
   1.  Reading CSVs (read_csv options)
   2.  Inspection  (shape, dtypes, info, describe)
   3.  Selecting   (loc, iloc, [], column lists)
   4.  Filtering   (boolean masks, query(), isin())
   5.  Cleaning    (missing values, type casting, string ops)
   6.  NumPy       (arrays, ufuncs, broadcasting, statistics)
   7.  Derived cols (assign, apply, map, vectorised ops)
   8.  Grouping    (groupby, agg, transform, pivot_table)
   9.  Merging     (merge, join, concat)
  10.  Sorting     (sort_values, nlargest, nsmallest)
  11.  Time series (to_datetime, dt accessor, resample)
  12.  Statistics  (corr, cov, rolling, cumulative)
  13.  Reshaping   (melt, pivot, stack/unstack)
  14.  Exporting   (to_csv, to_json, to_excel)
=============================================================
"""

import pandas as pd
import numpy as np
from io import StringIO

# ── helpers ──────────────────────────────────────────────────────────────────
SEP = "\n" + "─" * 60

def section(n, title):
    print(f"\n{'═'*60}")
    print(f"  SECTION {n}: {title}")
    print(f"{'═'*60}")

_MISSING = object()

def show(label, value=_MISSING):
    print(f"\n▸ {label}")
    if value is not _MISSING:
        print(value)

# ─────────────────────────────────────────────────────────────────────────────
# INLINE DATA  (mirrors the CSV files in sample_data/)
# ─────────────────────────────────────────────────────────────────────────────
TRANSACTIONS_CSV = """transaction_id,transaction_date,customer_id,product_id,store_id,quantity,unit_price,discount,region
TXN-001,2024-01-05,C001,P101,S01,2,29.99,0.00,North
TXN-002,2024-01-05,C002,P102,S02,1,149.99,10.00,South
TXN-003,2024-01-06,C003,P103,S01,3,9.99,0.00,North
TXN-004,2024-01-06,C001,P101,S03,1,29.99,5.00,East
TXN-005,2024-01-07,C004,P104,S02,5,4.99,0.00,South
TXN-006,2024-01-07,C005,P102,S04,2,149.99,20.00,West
TXN-007,2024-01-08,C002,P105,S01,1,299.99,0.00,North
TXN-008,2024-01-08,C003,P103,S03,4,9.99,2.00,East
TXN-009,2024-01-09,C006,P101,S02,2,29.99,0.00,South
TXN-010,2024-01-09,C001,P104,S04,10,4.99,5.00,West
TXN-011,2024-01-10,C007,P102,S01,1,149.99,0.00,North
TXN-012,2024-01-10,C004,P105,S03,2,299.99,15.00,East
TXN-013,2024-01-11,C005,P103,S02,6,9.99,0.00,South
TXN-014,2024-01-11,C008,P101,S04,3,29.99,0.00,West
TXN-015,2024-01-12,C002,P104,S01,8,4.99,0.00,North
TXN-016,2024-01-12,C006,P102,S02,1,149.99,10.00,South
TXN-017,2024-01-13,C009,P105,S03,1,299.99,25.00,East
TXN-018,2024-01-13,C003,P101,S04,2,29.99,0.00,West
TXN-019,2024-01-14,C007,P103,S01,5,9.99,0.00,North
TXN-020,2024-01-14,C010,P104,S02,3,4.99,0.00,South"""

CUSTOMERS_CSV = """customer_id,first_name,last_name,email,phone,city,state,country,signup_date,segment
C001,Alice,Johnson,alice.j@email.com,555-0101,Chicago,IL,USA,2022-03-15,Premium
C002,Bob,Smith,bob.s@email.com,555-0102,Houston,TX,USA,2021-11-20,Standard
C003,Carol,Williams,carol.w@email.com,555-0103,New York,NY,USA,2023-01-08,Standard
C004,David,Brown,david.b@email.com,555-0104,Phoenix,AZ,USA,2022-07-30,Basic
C005,Eve,Davis,eve.d@email.com,555-0105,Los Angeles,CA,USA,2021-05-12,Premium
C006,Frank,Miller,frank.m@email.com,555-0106,San Antonio,TX,USA,2023-04-01,Basic
C007,Grace,Wilson,grace.w@email.com,555-0107,San Diego,CA,USA,2022-09-17,Standard
C008,Henry,Moore,henry.m@email.com,555-0108,Dallas,TX,USA,2021-12-03,Premium
C009,Iris,Taylor,iris.t@email.com,555-0109,San Jose,CA,USA,2023-06-22,Basic
C010,Jack,Anderson,jack.a@email.com,555-0110,Austin,TX,USA,2022-02-14,Standard"""

PRODUCTS_CSV = """product_id,product_name,category,sub_category,brand,cost_price,list_price,sku,supplier_id
P101,Wireless Mouse,Electronics,Peripherals,TechBrand,12.50,29.99,SKU-WM-001,SUP01
P102,Bluetooth Headphones,Electronics,Audio,SoundPro,65.00,149.99,SKU-BH-002,SUP02
P103,USB-C Cable 2m,Electronics,Accessories,CableCo,2.10,9.99,SKU-UC-003,SUP01
P104,AA Batteries (4-pack),Electronics,Power,EnergyMax,1.50,4.99,SKU-BA-004,SUP03
P105,Mechanical Keyboard,Electronics,Peripherals,TechBrand,120.00,299.99,SKU-MK-005,SUP02"""


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — READING CSVs
# ══════════════════════════════════════════════════════════════════════════════
section(1, "READING CSVs")

# ── Basic read ────────────────────────────────────────────────────────────────
# From an actual file you would write:
#   df = pd.read_csv("sample_data/raw_sales_transactions.csv")
# Here we read from an inline string using StringIO:
df = pd.read_csv(StringIO(TRANSACTIONS_CSV))
cust = pd.read_csv(StringIO(CUSTOMERS_CSV))
prod = pd.read_csv(StringIO(PRODUCTS_CSV))

show("Basic read — first 3 rows:", df.head(3).to_string(index=False))

# ── read_csv options ──────────────────────────────────────────────────────────
show("read_csv with explicit dtype, parse_dates, usecols, index_col:")
df2 = pd.read_csv(
    StringIO(TRANSACTIONS_CSV),
    dtype={
        "transaction_id": str,
        "customer_id":    str,
        "product_id":     str,
        "quantity":       np.int32,       # use int32 to save memory
        "unit_price":     np.float64,
        "discount":       np.float64,
    },
    parse_dates=["transaction_date"],     # auto-parse date column
    usecols=["transaction_id","transaction_date","customer_id",
             "product_id","quantity","unit_price","discount","region"],
    index_col="transaction_id",           # use transaction_id as the index
)
print(df2.dtypes)

# ── Re-load cleanly for the rest of the demo ─────────────────────────────────
df = pd.read_csv(StringIO(TRANSACTIONS_CSV), parse_dates=["transaction_date"])
cust = pd.read_csv(StringIO(CUSTOMERS_CSV), parse_dates=["signup_date"])
prod = pd.read_csv(StringIO(PRODUCTS_CSV))


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — INSPECTION
# ══════════════════════════════════════════════════════════════════════════════
section(2, "INSPECTION")

show("df.shape  — (rows, cols):", df.shape)

show("df.dtypes:", df.dtypes)

show("df.info()  — memory + nulls:")
df.info()

show("df.describe()  — numeric summary:")
print(df.describe().round(2))

show("df.describe(include='object')  — categorical summary:")
print(df.describe(include="object"))

show("df.nunique()  — distinct values per column:")
print(df.nunique())

show("df['region'].value_counts():")
print(df["region"].value_counts())

show("df.isna().sum()  — missing values per column:")
print(df.isna().sum())


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — SELECTING DATA
# ══════════════════════════════════════════════════════════════════════════════
section(3, "SELECTING DATA")

# Single column → Series
show("df['quantity']  (Series, first 5):", df["quantity"].head().to_string())

# Multiple columns → DataFrame
show("df[['transaction_id','region','quantity']]  (first 4 rows):",
     df[["transaction_id","region","quantity"]].head(4).to_string(index=False))

# .loc — label-based (row label OR boolean mask, column name)
show("df.loc[0:3, 'customer_id':'quantity']  (rows 0–3, cols by name):",
     df.loc[0:3, "customer_id":"quantity"].to_string(index=False))

# .iloc — integer-based (pure position)
show("df.iloc[5:8, 2:6]  (rows 5–7, cols 2–5 by position):",
     df.iloc[5:8, 2:6].to_string(index=False))

# .at / .iat — single scalar access (fastest)
show("df.at[0, 'unit_price']  (single cell by label):", df.at[0, "unit_price"])
show("df.iat[0, 6]            (single cell by position):", df.iat[0, 6])


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — FILTERING
# ══════════════════════════════════════════════════════════════════════════════
section(4, "FILTERING")

# Boolean mask
mask_north = df["region"] == "North"
show("Rows where region == 'North'  (boolean mask):",
     df[mask_north][["transaction_id","region","quantity","unit_price"]].to_string(index=False))

# Compound conditions  — use & | ~ (not 'and' / 'or')
show("High-value orders: quantity > 2 AND unit_price > 50:",
     df[(df["quantity"] > 2) & (df["unit_price"] > 50)][
         ["transaction_id","quantity","unit_price","region"]].to_string(index=False))

# .query() — readable string expressions
show("df.query('discount > 0 and region != \"North\"'):",
     df.query('discount > 0 and region != "North"')[
         ["transaction_id","discount","region"]].to_string(index=False))

# .isin()
show("Transactions for C001, C005, C008  (.isin()):",
     df[df["customer_id"].isin(["C001","C005","C008"])][
         ["transaction_id","customer_id","unit_price"]].to_string(index=False))

# .between()
show("unit_price between 10 and 100  (.between()):",
     df[df["unit_price"].between(10, 100)][
         ["transaction_id","unit_price"]].to_string(index=False))

# .str accessor for string filtering
show("product_id starting with 'P10'  (.str.startswith()):",
     df[df["product_id"].str.startswith("P10")]["product_id"].unique())


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — CLEANING & TYPE CASTING
# ══════════════════════════════════════════════════════════════════════════════
section(5, "CLEANING & TYPE CASTING")

# Introduce intentional messiness for demo
dirty = df.copy()
dirty.loc[3, "discount"] = np.nan          # missing value
dirty.loc[7, "region"]   = "  north  "    # whitespace + wrong case
dirty.loc[12,"quantity"]  = -1             # impossible value

show("Dirty sample (rows 3, 7, 12):",
     dirty.iloc[[3,7,12]][["transaction_id","discount","region","quantity"]].to_string(index=False))

# fillna — fill missing with default
dirty["discount"] = dirty["discount"].fillna(0.0)

# String normalisation via .str accessor
dirty["region"] = dirty["region"].str.strip().str.title()

# Replace invalid values
dirty.loc[dirty["quantity"] < 0, "quantity"] = np.nan
dirty["quantity"] = dirty["quantity"].fillna(dirty["quantity"].median()).astype(int)

show("After cleaning:",
     dirty.iloc[[3,7,12]][["transaction_id","discount","region","quantity"]].to_string(index=False))

# Type casting
show("Cast quantity to int16 (memory saving):")
df["quantity"] = df["quantity"].astype(np.int16)
print(f"  dtype: {df['quantity'].dtype}, memory: {df['quantity'].nbytes} bytes")

# pd.Categorical — encode low-cardinality strings efficiently
df["region"] = pd.Categorical(df["region"], categories=["North","South","East","West"])
show("region as Categorical:", df["region"].dtype)

# pd.cut — bin continuous into discrete bands
df["price_band"] = pd.cut(
    df["unit_price"],
    bins=[0, 10, 50, 200, 500],
    labels=["Budget","Mid","Premium","Luxury"]
)
show("price_band distribution (pd.cut):", df["price_band"].value_counts().to_string())


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — NUMPY ARRAYS & OPERATIONS
# ══════════════════════════════════════════════════════════════════════════════
section(6, "NUMPY — ARRAYS, UFUNCS, BROADCASTING")

# ── Creating arrays ───────────────────────────────────────────────────────────
arr = np.array([1, 2, 3, 4, 5], dtype=np.float64)
matrix = np.array([[1,2,3],[4,5,6],[7,8,9]])
zeros  = np.zeros((3, 4))
ones   = np.ones((2, 5))
rng    = np.arange(0, 20, 2)        # 0, 2, 4 … 18
lin    = np.linspace(0, 1, 6)       # 6 evenly spaced values 0→1
rand   = np.random.default_rng(42).standard_normal((4, 3))

show("np.arange(0, 20, 2):",  rng)
show("np.linspace(0, 1, 6):", lin)
show("random normal (4×3):\n", rand.round(3))

# ── Array attributes ──────────────────────────────────────────────────────────
show("matrix.shape / ndim / dtype / size:",
     f"shape={matrix.shape}  ndim={matrix.ndim}  dtype={matrix.dtype}  size={matrix.size}")

# ── Indexing & slicing ────────────────────────────────────────────────────────
show("matrix[1, :]  (row 1):",       matrix[1, :])
show("matrix[:, 2]  (column 2):",    matrix[:, 2])
show("matrix[0:2, 1:3]  (submatrix):\n", matrix[0:2, 1:3])

# ── Universal functions (ufuncs) ──────────────────────────────────────────────
prices  = df["unit_price"].to_numpy()
qty     = df["quantity"].to_numpy().astype(float)

show("np.sqrt(prices)[:5]:",   np.sqrt(prices)[:5].round(3))
show("np.log1p(prices)[:5]:",  np.log1p(prices)[:5].round(3))   # log(1+x), safe for 0
show("np.round(prices, 0)[:5]:", np.round(prices, 0)[:5])

# ── Broadcasting ──────────────────────────────────────────────────────────────
# Multiply each price by a column-wise scalar array — no loop needed
tax_rates = np.array([0.05, 0.08, 0.10])            # shape (3,)
price_sample = prices[:3].reshape(3, 1)              # shape (3,1)
show("Broadcasting: 3 prices × 3 tax rates → 3×3 matrix:\n",
     (price_sample * tax_rates).round(2))

# ── Aggregation ───────────────────────────────────────────────────────────────
gross = qty * prices
show("Gross sales per transaction (qty × price), first 6:", gross[:6].round(2))

show("NumPy statistics on gross_sales:",
     f"  sum={np.sum(gross):.2f}  mean={np.mean(gross):.2f}  "
     f"std={np.std(gross):.2f}\n"
     f"  min={np.min(gross):.2f}  max={np.max(gross):.2f}  "
     f"median={np.median(gross):.2f}")

# ── Boolean masking on arrays ─────────────────────────────────────────────────
show("Transactions where gross > 100  (np boolean mask):",
     np.where(gross > 100, "HIGH", "low")[:10])

# ── Linear algebra ────────────────────────────────────────────────────────────
A = np.array([[2, 1], [5, 3]])
b = np.array([8, 13])
x = np.linalg.solve(A, b)          # solves Ax = b
show("np.linalg.solve  (Ax=b → x):", x)

evals, evecs = np.linalg.eig(A)
show("Eigenvalues of A:", np.round(evals, 4))

# ── Reshaping & stacking ──────────────────────────────────────────────────────
flat = np.arange(12)
cube = flat.reshape(3, 2, 2)
show("reshape(3,2,2) shape:", cube.shape)

stacked = np.vstack([prices[:3], qty[:3]])
show("np.vstack (prices + qty):\n", stacked.round(2))

show("np.percentile(gross, [25, 50, 75, 90]):",
     np.percentile(gross, [25, 50, 75, 90]).round(2))


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — DERIVED COLUMNS
# ══════════════════════════════════════════════════════════════════════════════
section(7, "DERIVED COLUMNS")

# Direct vectorised arithmetic (fastest — no Python loop)
df["gross_sales"]   = df["quantity"] * df["unit_price"]
df["net_sales"]     = df["gross_sales"] - df["discount"]
df["discount_pct"]  = (df["discount"] / df["gross_sales"] * 100).round(2)

show("Vectorised arithmetic — gross/net/discount_pct (first 5):",
     df[["transaction_id","gross_sales","net_sales","discount_pct"]].head(5).to_string(index=False))

# .assign() — chainable column addition
df = df.assign(
    net_sales_rounded = lambda x: x["net_sales"].round(0),
    is_discounted     = lambda x: x["discount"] > 0,
    quantity_bucket   = lambda x: pd.cut(x["quantity"],
                                         bins=[0,1,3,6,99],
                                         labels=["Single","Small","Medium","Bulk"])
)
show("Columns after .assign():", list(df.columns))

# .apply() — row-wise custom logic (use sparingly; slower than vectorised)
def classify_order(row):
    if row["net_sales"] >= 200:
        return "Enterprise"
    elif row["net_sales"] >= 50:
        return "Standard"
    return "Micro"

df["order_class"] = df.apply(classify_order, axis=1)
show("order_class via .apply() (value counts):", df["order_class"].value_counts().to_string())

# .map() — Series-level element mapping via dict
segment_score = {"North": 1, "South": 2, "East": 3, "West": 4}
df["region_code"] = df["region"].map(segment_score)
show("region mapped to numeric code (.map()):",
     df[["region","region_code"]].drop_duplicates().to_string(index=False))

# np.select — vectorised if/elif/else (much faster than .apply for scalars)
conditions = [
    df["unit_price"] <= 10,
    df["unit_price"] <= 50,
    df["unit_price"] <= 200,
]
choices = ["Budget", "Mid-range", "Premium"]
df["price_tier"] = np.select(conditions, choices, default="Luxury")
show("price_tier via np.select:", df["price_tier"].value_counts().to_string())


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — GROUPBY & AGGREGATION
# ══════════════════════════════════════════════════════════════════════════════
section(8, "GROUPBY & AGGREGATION")

# ── Basic groupby ─────────────────────────────────────────────────────────────
show("Total net_sales and mean discount by region:",
     df.groupby("region")[["net_sales","discount"]].agg(
         total_net_sales=("net_sales","sum"),
         mean_discount   =("discount","mean"),
         txn_count       =("net_sales","count")
     ).round(2).to_string())

# ── Multiple aggregation functions ───────────────────────────────────────────
show("quantity stats by price_tier (.agg with list):",
     df.groupby("price_tier")["quantity"].agg(["min","max","mean","sum","std"])
       .round(2).to_string())

# ── Named aggregation (pandas ≥ 0.25) ────────────────────────────────────────
summary = df.groupby("region").agg(
    orders        = ("transaction_id", "count"),
    gross_revenue = ("gross_sales",    "sum"),
    net_revenue   = ("net_sales",      "sum"),
    avg_order_val = ("net_sales",      "mean"),
    items_sold    = ("quantity",       "sum"),
).round(2)
show("Named aggregation result:", summary.to_string())

# ── transform() — return group stat aligned to original index ─────────────────
df["region_total_net"] = df.groupby("region")["net_sales"].transform("sum")
df["pct_of_region"]    = (df["net_sales"] / df["region_total_net"] * 100).round(2)
show("Each transaction's % share of its region's revenue (first 6):",
     df[["transaction_id","region","net_sales","region_total_net","pct_of_region"]].head(6).to_string(index=False))

# ── Pivot table ───────────────────────────────────────────────────────────────
pivot = pd.pivot_table(
    df,
    values  ="net_sales",
    index   ="region",
    columns ="price_tier",
    aggfunc ="sum",
    fill_value=0
)
show("Pivot table — net_sales by region × price_tier:\n", pivot.round(2).to_string())

# ── Groupby on multiple keys ──────────────────────────────────────────────────
show("Region + order_class cross-tab:",
     df.groupby(["region","order_class"])["net_sales"].sum()
       .unstack(fill_value=0).round(2).to_string())


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — MERGING & JOINING
# ══════════════════════════════════════════════════════════════════════════════
section(9, "MERGING & JOINING")

# ── Inner merge ───────────────────────────────────────────────────────────────
merged = df.merge(cust, on="customer_id", how="inner")
show("Inner merge (transactions + customers)  shape:", merged.shape)
show("New columns from customer table:", [c for c in merged.columns if c in cust.columns])

# ── Left merge ────────────────────────────────────────────────────────────────
full = df.merge(cust, on="customer_id", how="left") \
          .merge(prod, on="product_id",  how="left")
show("Left merge chain (txn + cust + prod) shape:", full.shape)
show("Enriched sample (first 3 rows):",
     full[["transaction_id","first_name","product_name","segment","net_sales"]].head(3).to_string(index=False))

# ── merge with suffixes (handling duplicate column names) ─────────────────────
left  = df[["transaction_id","unit_price"]].head(3)
right = prod[["product_id","list_price"]].head(3).rename(columns={"list_price":"unit_price"})
merged_suf = left.merge(right, left_index=True, right_index=True, suffixes=("_txn","_prod"))
show("Merge with suffixes (duplicate col demo):\n", merged_suf.to_string(index=False))

# ── pd.concat ─────────────────────────────────────────────────────────────────
north_txn = df[df["region"] == "North"].head(2)
south_txn = df[df["region"] == "South"].head(2)
combined  = pd.concat([north_txn, south_txn], ignore_index=True)
show("pd.concat (North + South, reset index):",
     combined[["transaction_id","region"]].to_string(index=False))


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — SORTING
# ══════════════════════════════════════════════════════════════════════════════
section(10, "SORTING")

show("Sort by net_sales descending:",
     df.sort_values("net_sales", ascending=False)[["transaction_id","net_sales"]].head(5).to_string(index=False))

show("Sort by region asc then net_sales desc (multi-key):",
     df.sort_values(["region","net_sales"], ascending=[True,False])[
         ["transaction_id","region","net_sales"]].head(6).to_string(index=False))

show("Top 3 highest net_sales  (nlargest):",
     df.nlargest(3, "net_sales")[["transaction_id","net_sales"]].to_string(index=False))

show("Bottom 3 net_sales  (nsmallest):",
     df.nsmallest(3, "net_sales")[["transaction_id","net_sales"]].to_string(index=False))

show("Index of max net_sales  (idxmax):", df["net_sales"].idxmax())
show("Index of min net_sales  (idxmin):", df["net_sales"].idxmin())


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 11 — TIME SERIES & DATETIME
# ══════════════════════════════════════════════════════════════════════════════
section(11, "TIME SERIES & DATETIME")

# ── .dt accessor ──────────────────────────────────────────────────────────────
show("dt accessor — extract date parts:")
df["year"]        = df["transaction_date"].dt.year
df["month"]       = df["transaction_date"].dt.month
df["day"]         = df["transaction_date"].dt.day
df["day_of_week"] = df["transaction_date"].dt.day_name()
df["week"]        = df["transaction_date"].dt.isocalendar().week.astype(int)
print(df[["transaction_date","year","month","day","day_of_week","week"]].head(6).to_string(index=False))

# ── Date arithmetic ───────────────────────────────────────────────────────────
df["days_since_first"] = (df["transaction_date"] - df["transaction_date"].min()).dt.days
show("days_since_first_transaction (first 5):", df["days_since_first"].head().to_string())

# ── Resample — aggregate by date frequency ────────────────────────────────────
ts = df.set_index("transaction_date")["net_sales"]
show("Daily total net_sales (.resample('D').sum()):",
     ts.resample("D").sum().round(2).to_string())

show("Every-2-day total net_sales (.resample('2D').sum()):",
     ts.resample("2D").sum().round(2).to_string())

# ── date_range ────────────────────────────────────────────────────────────────
date_idx = pd.date_range(start="2024-01-05", periods=10, freq="D")
show("pd.date_range (10 days from 2024-01-05):", date_idx)

# ── Period ────────────────────────────────────────────────────────────────────
df["period_W"] = df["transaction_date"].dt.to_period("W")
show("transaction_date as weekly period (first 5):", df["period_W"].head().to_string())


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 12 — STATISTICS & ROLLING WINDOWS
# ══════════════════════════════════════════════════════════════════════════════
section(12, "STATISTICS, CORRELATION & ROLLING")

# ── Correlation matrix ────────────────────────────────────────────────────────
numeric_cols = df[["quantity","unit_price","discount","gross_sales","net_sales"]].astype(float)
show("Pearson correlation matrix (.corr()):",
     numeric_cols.corr().round(3).to_string())

show("Spearman correlation (.corr(method='spearman')):",
     numeric_cols.corr(method="spearman").round(3).to_string())

# ── Covariance ────────────────────────────────────────────────────────────────
show("Covariance matrix (.cov()) — quantity vs unit_price:",
     numeric_cols[["quantity","unit_price"]].cov().round(2).to_string())

# ── Rolling window ───────────────────────────────────────────────────────────
daily_net = ts.resample("D").sum().reset_index()
daily_net.columns = ["date","daily_net"]
daily_net["rolling_3d_mean"] = daily_net["daily_net"].rolling(window=3, min_periods=1).mean()
daily_net["rolling_3d_std"]  = daily_net["daily_net"].rolling(window=3, min_periods=1).std()
show("3-day rolling mean & std of daily net_sales:",
     daily_net.round(2).to_string(index=False))

# ── Cumulative operations ─────────────────────────────────────────────────────
daily_net["cumulative_net"] = daily_net["daily_net"].cumsum()
show("Cumulative net_sales by day:",
     daily_net[["date","daily_net","cumulative_net"]].round(2).to_string(index=False))

# ── Expanding window ─────────────────────────────────────────────────────────
daily_net["expanding_mean"] = daily_net["daily_net"].expanding().mean()
show("Expanding (running) mean of daily net_sales:",
     daily_net[["date","daily_net","expanding_mean"]].round(2).to_string(index=False))

# ── Z-score normalisation using NumPy ────────────────────────────────────────
arr_net = df["net_sales"].to_numpy()
z_scores = (arr_net - np.mean(arr_net)) / np.std(arr_net)
show("Z-scores for net_sales (first 6):", np.round(z_scores[:6], 3))
show("Outliers (|z| > 1.5):", df.loc[np.abs(z_scores) > 1.5, ["transaction_id","net_sales"]].to_string(index=False))


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 13 — RESHAPING
# ══════════════════════════════════════════════════════════════════════════════
section(13, "RESHAPING — MELT, PIVOT, STACK/UNSTACK")

# ── pd.melt — wide → long ─────────────────────────────────────────────────────
wide = df[["transaction_id","gross_sales","net_sales","discount"]].head(4)
long = pd.melt(wide, id_vars="transaction_id",
               value_vars=["gross_sales","net_sales","discount"],
               var_name="metric", value_name="amount")
show("pd.melt — wide to long format:", long.to_string(index=False))

# ── df.pivot — long → wide ───────────────────────────────────────────────────
re_wide = long.pivot(index="transaction_id", columns="metric", values="amount")
show("df.pivot — long back to wide:", re_wide.round(2).to_string())

# ── stack / unstack ───────────────────────────────────────────────────────────
region_day = df.groupby(["region","day_of_week"])["net_sales"].sum().round(2)
show("MultiIndex series (region × day):", region_day.head(8).to_string())

region_day_wide = region_day.unstack("day_of_week").fillna(0)
show("After .unstack('day_of_week'):", region_day_wide.round(2).to_string())

stacked_back = region_day_wide.stack()
show("After re-.stack() — back to MultiIndex (first 5):", stacked_back.head(5).to_string())

# ── pd.crosstab ───────────────────────────────────────────────────────────────
full = df.merge(cust, on="customer_id", how="left")
show("pd.crosstab — region × segment (counts):",
     pd.crosstab(full["region"], full["segment"]).to_string())

show("pd.crosstab with margins and normalisation (rows):",
     pd.crosstab(full["region"], full["segment"],
                 normalize="index").round(3).to_string())


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 14 — EXPORTING
# ══════════════════════════════════════════════════════════════════════════════
section(14, "EXPORTING DATA")

# Build a clean summary DataFrame to export
export_df = df[["transaction_id","transaction_date","customer_id",
                "product_id","region","net_sales","order_class","price_tier"]]

# ── to_csv ────────────────────────────────────────────────────────────────────
export_df.to_csv("enriched_transactions.csv", index=False, float_format="%.2f")
show("Saved: enriched_transactions.csv  (to_csv, no index)")

# ── to_json ───────────────────────────────────────────────────────────────────
export_df.head(5).to_json("transactions_sample.json", orient="records", indent=2,
                           date_format="iso")
show("Saved: transactions_sample.json  (to_json, orient='records')")

# ── to_csv in-memory (StringIO — useful in pipelines) ────────────────────────
buf = StringIO()
summary.to_csv(buf)
show("In-memory CSV (first 100 chars):", buf.getvalue()[:100])

# ── NumPy save / load ─────────────────────────────────────────────────────────
np.save("net_sales_array.npy", arr_net)
loaded = np.load("net_sales_array.npy")
show("np.save → np.load round-trip matches:", np.array_equal(arr_net, loaded))

np.savetxt("net_sales.txt", arr_net, fmt="%.2f", header="net_sales")
show("np.savetxt → net_sales.txt (first 5 lines):",
     "\n".join(open("net_sales.txt").readlines()[:6]))


# ══════════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
section("✓", "COMPLETE — KEY TAKEAWAYS")
print("""
  pandas                         numpy
  ──────────────────────────     ──────────────────────────
  read_csv / to_csv              np.array / np.save
  DataFrame / Series             ndarray
  .loc / .iloc / .at             boolean masking
  .query / .isin / .between      np.where / np.select
  .fillna / .dropna              np.nan handling
  .groupby / .agg / .transform   np.sum / np.mean / np.std
  .merge / pd.concat             np.vstack / np.hstack
  .pivot_table / pd.crosstab     np.percentile / np.linalg
  .rolling / .resample           broadcasting
  pd.cut / pd.Categorical        np.arange / np.linspace
  .dt accessor                   ufuncs (sqrt, log1p, …)
  .str accessor                  np.round / np.select

  Rule of thumb:
    ✔  Use NumPy for raw numeric arrays & math
    ✔  Use pandas for labelled, mixed-type tabular data
    ✔  Avoid Python loops — use vectorised operations always
""")
