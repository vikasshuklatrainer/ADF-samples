"""
=============================================================
  EXPLORATORY DATA ANALYSIS (EDA) — COMPLETE WALKTHROUGH
  Dataset: Retail Sales Transactions (20 rows × 3 tables)
=============================================================
  EDA Workflow:
    0.  Setup & Data Loading
    1.  Shape & Schema Audit
    2.  Missing Value Analysis
    3.  Duplicate Detection
    4.  Univariate Analysis   — numeric distributions
    5.  Univariate Analysis   — categorical frequencies
    6.  Bivariate Analysis    — numeric vs numeric
    7.  Bivariate Analysis    — numeric vs categorical
    8.  Outlier Detection     — IQR + Z-score + Tukey
    9.  Distribution Testing  — skewness, kurtosis, normality
   10.  Correlation Deep-Dive — Pearson, Spearman, Kendall
   11.  Feature Engineering   — derived signals for modelling
   12.  Time Patterns         — trends, weekday, rolling stats
   13.  Cohort & Segmentation — customer segment profiling
   14.  EDA Summary Report    — auto-generated findings
=============================================================
"""

import warnings
import numpy as np
import pandas as pd
from io import StringIO

warnings.filterwarnings("ignore")
pd.set_option("display.float_format", "{:.2f}".format)
pd.set_option("display.max_columns", 20)
pd.set_option("display.width", 120)

# ── pretty printer ────────────────────────────────────────────────────────────
_MISS = object()

def section(n, title):
    bar = "═" * 62
    print(f"\n{bar}\n  SECTION {n}: {title}\n{bar}")

def h(title):
    print(f"\n  ── {title} {'─' * max(1, 50 - len(title))}")

def show(label, val=_MISS):
    print(f"\n▸ {label}")
    if val is not _MISS:
        print(val)

def divider():
    print("  " + "·" * 56)


# ─────────────────────────────────────────────────────────────────────────────
#  INLINE DATA
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
#  SECTION 0 — SETUP & DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════
section(0, "SETUP & DATA LOADING")

# ── Load and merge all three tables into one analytical DataFrame ─────────────
# To load from disk instead, replace StringIO(...) with the file path string:
#   df = pd.read_csv("sample_data/raw_sales_transactions.csv", parse_dates=["transaction_date"])
df   = pd.read_csv(StringIO(TRANSACTIONS_CSV), parse_dates=["transaction_date"])
cust = pd.read_csv(StringIO(CUSTOMERS_CSV),    parse_dates=["signup_date"])
prod = pd.read_csv(StringIO(PRODUCTS_CSV))

# Enrich with customer and product attributes
df = (df
      .merge(cust[["customer_id","segment","city","state","signup_date"]], on="customer_id", how="left")
      .merge(prod[["product_id","product_name","sub_category","cost_price"]], on="product_id", how="left"))

# Compute key financial measures up front
df["gross_sales"]  = df["quantity"] * df["unit_price"]
df["net_sales"]    = df["gross_sales"] - df["discount"]
df["cogs"]         = df["quantity"] * df["cost_price"]       # cost of goods sold
df["gross_profit"] = df["net_sales"] - df["cogs"]
df["margin_pct"]   = (df["gross_profit"] / df["net_sales"] * 100).round(2)
df["discount_pct"] = (df["discount"] / df["gross_sales"].replace(0, np.nan) * 100).round(2).fillna(0)

show("Enriched DataFrame — shape:", df.shape)
show("Columns:", list(df.columns))
show("First 3 rows:", df.head(3).to_string(index=False))


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — SHAPE & SCHEMA AUDIT
# ══════════════════════════════════════════════════════════════════════════════
section(1, "SHAPE & SCHEMA AUDIT")

h("Dimensions")
print(f"  Rows : {df.shape[0]}")
print(f"  Cols : {df.shape[1]}")
print(f"  Cells: {df.size:,}")

h("Data types per column")
type_map = df.dtypes.reset_index()
type_map.columns = ["column", "dtype"]
type_map["category"] = type_map["dtype"].apply(
    lambda d: "numeric" if pd.api.types.is_numeric_dtype(d)
    else "datetime" if pd.api.types.is_datetime64_any_dtype(d)
    else "text"
)
print(type_map.to_string(index=False))

h("Memory footprint")
mem = df.memory_usage(deep=True)
print(f"  Total: {mem.sum() / 1024:.1f} KB")
print(f"  Largest column: '{mem.idxmax()}' at {mem.max() / 1024:.2f} KB")

h("Cardinality (unique values per column)")
card = df.nunique().reset_index()
card.columns = ["column", "unique_count"]
card["pct_unique"] = (card["unique_count"] / len(df) * 100).round(1)
card["is_id_like"] = card["pct_unique"] == 100
print(card.to_string(index=False))


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — MISSING VALUE ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
section(2, "MISSING VALUE ANALYSIS")

# ── Overall missing summary ───────────────────────────────────────────────────
h("Missing value count and percentage")
missing = pd.DataFrame({
    "null_count": df.isna().sum(),
    "pct_missing": (df.isna().mean() * 100).round(2),
    "dtype": df.dtypes
}).query("null_count > 0")

if missing.empty:
    print("  ✔  No missing values found in any column.")
else:
    print(missing.to_string())

# ── Inject artificial missing values to demonstrate detection & imputation ────
h("Injecting missing values for demonstration")
demo = df.copy()
rng  = np.random.default_rng(seed=7)

# 3 random discount values missing (MCAR — Missing Completely At Random)
miss_idx = rng.choice(len(demo), size=3, replace=False)
demo.loc[miss_idx, "discount"] = np.nan

# margin_pct missing for all Budget products (MAR — Missing At Random)
demo.loc[demo["unit_price"] < 10, "margin_pct"] = np.nan

# city missing for old signups (MNAR — Missing Not At Random)
demo.loc[demo["signup_date"] < "2022-01-01", "city"] = np.nan

missing2 = pd.DataFrame({
    "null_count": demo.isna().sum(),
    "pct_missing": (demo.isna().mean() * 100).round(1),
}).query("null_count > 0")
print(missing2.to_string())

# ── Imputation strategies ─────────────────────────────────────────────────────
h("Imputation strategies")

# Numeric: fill with median (robust to outliers)
demo["discount"] = demo["discount"].fillna(demo["discount"].median())
print(f"  discount   → filled with median  ({demo['discount'].median():.2f})")

# Numeric: fill with group mean (segment-aware imputation)
demo["margin_pct"] = demo.groupby("sub_category")["margin_pct"] \
                         .transform(lambda x: x.fillna(x.mean()))
print(f"  margin_pct → filled with sub_category group mean")

# Categorical: fill with mode
mode_city = demo["city"].mode()[0]
demo["city"] = demo["city"].fillna(mode_city)
print(f"  city       → filled with mode   ('{mode_city}')")

print(f"\n  Remaining nulls after imputation: {demo.isna().sum().sum()}")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — DUPLICATE DETECTION
# ══════════════════════════════════════════════════════════════════════════════
section(3, "DUPLICATE DETECTION")

h("Exact row duplicates")
n_dupes = df.duplicated().sum()
print(f"  Full row duplicates : {n_dupes}")

h("Business-key duplicates  (same transaction_id appearing twice)")
key_dupes = df.duplicated(subset=["transaction_id"], keep=False)
print(f"  transaction_id dupes: {key_dupes.sum()}")

h("Inject a duplicate row to demonstrate detection")
duped = pd.concat([df, df.iloc[[0]]], ignore_index=True)
print(f"  Rows before: {len(df)}  |  Rows after inject: {len(duped)}")
print(f"  Duplicates detected: {duped.duplicated(subset=['transaction_id']).sum()}")
duped_clean = duped.drop_duplicates(subset=["transaction_id"], keep="first")
print(f"  Rows after dedup   : {len(duped_clean)}")

h("Near-duplicate detection — same customer + product + date")
near = df.groupby(["customer_id","product_id","transaction_date"]).size()
near_dupes = near[near > 1]
if near_dupes.empty:
    print("  ✔  No near-duplicates found.")
else:
    print(near_dupes)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — UNIVARIATE ANALYSIS: NUMERIC DISTRIBUTIONS
# ══════════════════════════════════════════════════════════════════════════════
section(4, "UNIVARIATE ANALYSIS — NUMERIC DISTRIBUTIONS")

numeric_cols = ["quantity", "unit_price", "discount", "gross_sales",
                "net_sales", "cogs", "gross_profit", "margin_pct", "discount_pct"]

# ── Extended descriptive statistics ──────────────────────────────────────────
h("Extended descriptive statistics")
arr_data = {c: df[c].to_numpy() for c in numeric_cols}

stats_rows = []
for col, arr in arr_data.items():
    arr_clean = arr[~np.isnan(arr)]
    q1, q2, q3 = np.percentile(arr_clean, [25, 50, 75])
    iqr = q3 - q1
    skew_val = float(pd.Series(arr_clean).skew())
    kurt_val = float(pd.Series(arr_clean).kurt())
    cv = np.std(arr_clean) / np.mean(arr_clean) * 100 if np.mean(arr_clean) != 0 else np.nan
    stats_rows.append({
        "column"   : col,
        "count"    : len(arr_clean),
        "mean"     : round(np.mean(arr_clean), 2),
        "median"   : round(np.median(arr_clean), 2),
        "std"      : round(np.std(arr_clean, ddof=1), 2),
        "CV%"      : round(cv, 1),
        "min"      : round(np.min(arr_clean), 2),
        "Q1"       : round(q1, 2),
        "Q3"       : round(q3, 2),
        "max"      : round(np.max(arr_clean), 2),
        "IQR"      : round(iqr, 2),
        "skewness" : round(skew_val, 3),
        "kurtosis" : round(kurt_val, 3),
    })

stats_df = pd.DataFrame(stats_rows).set_index("column")
print(stats_df.to_string())

# ── ASCII histogram for each numeric column ───────────────────────────────────
h("ASCII histograms (distribution shape at a glance)")

def ascii_hist(series, bins=8, width=40, label=""):
    arr = series.dropna().to_numpy()
    counts, edges = np.histogram(arr, bins=bins)
    max_c = counts.max() or 1
    print(f"\n  {label or series.name}  (n={len(arr)}, "
          f"mean={arr.mean():.2f}, std={arr.std():.2f})")
    for i, (lo, hi, c) in enumerate(zip(edges, edges[1:], counts)):
        bar = "█" * int(c / max_c * width)
        print(f"  [{lo:>8.2f} – {hi:>8.2f}]  {bar:<{width}}  {c}")

for col in ["quantity", "unit_price", "net_sales", "margin_pct"]:
    ascii_hist(df[col])

# ── Quantile table ────────────────────────────────────────────────────────────
h("Percentile table for net_sales")
pcts = [1, 5, 10, 25, 50, 75, 90, 95, 99]
vals = np.percentile(df["net_sales"].to_numpy(), pcts)
for p, v in zip(pcts, vals):
    bar = "▓" * int(v / vals[-1] * 30)
    print(f"  P{p:>3}  {v:>8.2f}  {bar}")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 5 — UNIVARIATE ANALYSIS: CATEGORICAL FREQUENCIES
# ══════════════════════════════════════════════════════════════════════════════
section(5, "UNIVARIATE ANALYSIS — CATEGORICAL FREQUENCIES")

cat_cols = ["region", "segment", "sub_category", "product_name", "store_id"]

for col in cat_cols:
    vc = df[col].value_counts()
    total = vc.sum()
    h(f"'{col}'  ({df[col].nunique()} unique values)")
    for val, cnt in vc.items():
        pct = cnt / total * 100
        bar = "█" * int(pct / 100 * 30)
        print(f"  {str(val):<24}  {cnt:>3}  ({pct:>5.1f}%)  {bar}")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 6 — BIVARIATE ANALYSIS: NUMERIC vs NUMERIC
# ══════════════════════════════════════════════════════════════════════════════
section(6, "BIVARIATE ANALYSIS — NUMERIC vs NUMERIC")

h("Correlation matrix heatmap (ASCII)")

corr_cols = ["quantity", "unit_price", "discount", "net_sales", "gross_profit", "margin_pct"]
corr = df[corr_cols].astype(float).corr().round(2)

# Print as a coloured-text heatmap using symbols
def corr_symbol(v):
    if   v >=  0.8: return "██"    # strong positive
    elif v >=  0.5: return "▓▓"    # moderate positive
    elif v >=  0.2: return "░░"    # weak positive
    elif v >= -0.2: return "··"    # negligible
    elif v >= -0.5: return "▒▒"    # weak negative
    elif v >= -0.8: return "▓▓"    # moderate negative
    else:           return "■■"    # strong negative

print(f"\n  Legend: ██ strong+  ▓▓ mod+/-  ░░ weak+  ▒▒ weak-  ·· near-0  ■■ strong-\n")
header = "  " + " " * 14 + "  ".join(f"{c[:8]:>8}" for c in corr.columns)
print(header)
for row_name, row in corr.iterrows():
    sym_row = "  ".join(corr_symbol(v) for v in row)
    print(f"  {row_name:<14}  {sym_row}")

h("Pearson r values for key pairs")
pairs = [
    ("unit_price",   "gross_profit"),
    ("quantity",     "net_sales"),
    ("discount",     "margin_pct"),
    ("unit_price",   "margin_pct"),
    ("quantity",     "unit_price"),
    ("discount_pct", "gross_profit"),
]
for a, b in pairs:
    r = np.corrcoef(df[a].astype(float), df[b].astype(float))[0, 1]
    strength = ("strong" if abs(r) > 0.7
                else "moderate" if abs(r) > 0.4
                else "weak")
    direction = "positive" if r > 0 else "negative"
    print(f"  {a:<18} vs {b:<18}  r = {r:+.3f}  ({strength} {direction})")

h("Scatter plot proxy — net_sales vs unit_price (ASCII)")
# Bucket both axes into a 5×5 grid
x = pd.cut(df["unit_price"], bins=5, labels=range(5))
y = pd.cut(df["net_sales"],  bins=5, labels=range(4, -1, -1))
grid = pd.crosstab(y, x).reindex(index=range(4,-1,-1), columns=range(5), fill_value=0)
print("\n  net_sales ↑")
for row_label, row in grid.iterrows():
    cells = "  ".join("●" * v if v > 0 else "·" for v in row)
    print(f"  {cells}")
print("  " + "──" * 11 + "→ unit_price")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 7 — BIVARIATE ANALYSIS: NUMERIC vs CATEGORICAL
# ══════════════════════════════════════════════════════════════════════════════
section(7, "BIVARIATE ANALYSIS — NUMERIC vs CATEGORICAL")

h("net_sales distribution by region")
for region, grp in df.groupby("region"):
    arr = grp["net_sales"].to_numpy()
    bar = "█" * int(arr.mean() / df["net_sales"].max() * 30)
    print(f"  {region:<8}  n={len(arr):>2}  "
          f"mean={arr.mean():>7.2f}  "
          f"median={np.median(arr):>7.2f}  "
          f"std={arr.std():>7.2f}  {bar}")

h("margin_pct distribution by segment")
for seg, grp in df.groupby("segment"):
    arr = grp["margin_pct"].to_numpy()
    bar = "█" * int(arr.mean() / 100 * 30)
    print(f"  {seg:<10}  n={len(arr):>2}  "
          f"mean={arr.mean():>6.2f}%  "
          f"min={arr.min():>6.2f}%  "
          f"max={arr.max():>6.2f}%  {bar}")

h("net_sales by product (box-plot proxy using NumPy percentiles)")
print(f"\n  {'Product':<24}  {'Min':>7}  {'Q1':>7}  {'Med':>7}  {'Q3':>7}  {'Max':>7}  {'Mean':>7}")
print("  " + "─" * 74)
for prod_name, grp in df.groupby("product_name"):
    arr = grp["net_sales"].to_numpy()
    mn, q1, med, q3, mx = np.percentile(arr, [0, 25, 50, 75, 100])
    # IQR box in ASCII
    box = f"|{'─'*3}[{'█'*4}]{''*3}|"
    print(f"  {prod_name:<24}  {mn:>7.2f}  {q1:>7.2f}  {med:>7.2f}  "
          f"{q3:>7.2f}  {mx:>7.2f}  {arr.mean():>7.2f}")

h("Mean net_sales by segment × region  (contingency table)")
ct = df.pivot_table(values="net_sales", index="segment",
                    columns="region", aggfunc="mean", fill_value=0)
print(ct.round(2).to_string())


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 8 — OUTLIER DETECTION
# ══════════════════════════════════════════════════════════════════════════════
section(8, "OUTLIER DETECTION")

target = "net_sales"
arr    = df[target].to_numpy(dtype=float)

# ── Method 1: IQR (Tukey fences) ─────────────────────────────────────────────
h("Method 1: IQR / Tukey fences")
q1, q3  = np.percentile(arr, [25, 75])
iqr     = q3 - q1
lower_t = q1 - 1.5 * iqr
upper_t = q3 + 1.5 * iqr
mask_iqr = (arr < lower_t) | (arr > upper_t)

print(f"  Q1={q1:.2f}  Q3={q3:.2f}  IQR={iqr:.2f}")
print(f"  Tukey fences: [{lower_t:.2f}, {upper_t:.2f}]")
print(f"  Outliers detected: {mask_iqr.sum()}")
if mask_iqr.sum():
    print(df.loc[mask_iqr, ["transaction_id", target, "product_name"]].to_string(index=False))

# ── Method 2: Z-score ─────────────────────────────────────────────────────────
h("Method 2: Z-score  (threshold |z| > 2)")
z_scores   = (arr - arr.mean()) / arr.std()
mask_z     = np.abs(z_scores) > 2.0

print(f"  Mean={arr.mean():.2f}  Std={arr.std():.2f}")
print(f"  Outliers detected: {mask_z.sum()}")
if mask_z.sum():
    out_df = df.loc[mask_z, ["transaction_id", target]].copy()
    out_df["z_score"] = z_scores[mask_z].round(3)
    print(out_df.to_string(index=False))

# ── Method 3: Modified Z-score (MAD — robust to non-normal data) ──────────────
h("Method 3: Modified Z-score using MAD  (threshold > 3.5)")
median  = np.median(arr)
mad     = np.median(np.abs(arr - median))
mod_z   = 0.6745 * (arr - median) / (mad if mad != 0 else 1e-9)
mask_mad = np.abs(mod_z) > 3.5

print(f"  Median={median:.2f}  MAD={mad:.2f}")
print(f"  Outliers detected: {mask_mad.sum()}")

# ── Method 4: Extreme fences (Tukey 3×IQR) ───────────────────────────────────
h("Method 4: Extreme fences  (3×IQR — only 'far out' points)")
lower_e = q1 - 3.0 * iqr
upper_e = q3 + 3.0 * iqr
mask_ext = (arr < lower_e) | (arr > upper_e)
print(f"  Extreme fences: [{lower_e:.2f}, {upper_e:.2f}]")
print(f"  Extreme outliers: {mask_ext.sum()}")

# ── Agreement across methods ─────────────────────────────────────────────────
h("Outlier flags agreement across all methods")
flag_df = df[["transaction_id", target]].copy()
flag_df["iqr_flag"] = mask_iqr
flag_df["z_flag"]   = mask_z
flag_df["mad_flag"]  = mask_mad
flag_df["flag_count"] = flag_df[["iqr_flag","z_flag","mad_flag"]].sum(axis=1)
flagged = flag_df[flag_df["flag_count"] > 0]
print(flagged.to_string(index=False) if not flagged.empty else "  No consistent outliers found across methods.")

# ── Outlier treatment options ────────────────────────────────────────────────
h("Outlier treatment comparison on 'net_sales'")
raw_mean  = arr.mean()
cap_arr   = np.clip(arr, lower_t, upper_t)           # winsorize / cap
log_arr   = np.log1p(arr)                             # log transform
trim_arr  = arr[(arr >= lower_t) & (arr <= upper_t)] # trim

print(f"  {'Method':<25}  {'Mean':>9}  {'Std':>9}  {'Skew':>7}")
print(f"  {'─'*25}  {'─'*9}  {'─'*9}  {'─'*7}")
for label, a in [("Raw", arr), ("Winsorised", cap_arr),
                  ("Log-transformed", log_arr), ("Trimmed", trim_arr)]:
    sk = pd.Series(a).skew()
    print(f"  {label:<25}  {a.mean():>9.3f}  {a.std():>9.3f}  {sk:>7.3f}")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 9 — DISTRIBUTION TESTING
# ══════════════════════════════════════════════════════════════════════════════
section(9, "DISTRIBUTION TESTING — SKEWNESS, KURTOSIS, NORMALITY")

h("Skewness and kurtosis for all numeric measures")
print(f"\n  {'Column':<20}  {'Mean':>9}  {'Std':>9}  {'Skewness':>10}  {'Kurtosis':>10}  Shape")
print("  " + "─" * 80)

def shape_label(sk, kt):
    s = "symmetric" if abs(sk) < 0.5 else ("right-skewed" if sk > 0 else "left-skewed")
    k = " leptokurtic" if kt > 1 else (" platykurtic" if kt < -1 else "")
    return s + k

for col in numeric_cols:
    a  = df[col].dropna().to_numpy(dtype=float)
    sk = pd.Series(a).skew()
    kt = pd.Series(a).kurt()
    print(f"  {col:<20}  {a.mean():>9.2f}  {a.std():>9.2f}  "
          f"{sk:>10.3f}  {kt:>10.3f}  {shape_label(sk, kt)}")

h("Normality check via Empirical Rule  (68-95-99.7)")
# For a normal distribution: ~68% within 1σ, ~95% within 2σ, ~99.7% within 3σ
print(f"\n  {'Column':<20}  {'Within 1σ':>10}  {'Within 2σ':>10}  {'Within 3σ':>10}  {'~Normal?':>10}")
print("  " + "─" * 72)
for col in ["net_sales", "quantity", "unit_price", "margin_pct"]:
    a   = df[col].dropna().to_numpy(dtype=float)
    mu, sigma = a.mean(), a.std()
    w1 = (np.abs(a - mu) <= 1 * sigma).mean() * 100
    w2 = (np.abs(a - mu) <= 2 * sigma).mean() * 100
    w3 = (np.abs(a - mu) <= 3 * sigma).mean() * 100
    normal = "likely" if (60 < w1 < 75 and 88 < w2 < 100) else "unlikely"
    print(f"  {col:<20}  {w1:>9.1f}%  {w2:>9.1f}%  {w3:>9.1f}%  {normal:>10}")

h("Shapiro-Wilk normality test (manual approximation via moments)")
# Full scipy.stats.shapiro not available without scipy; use moment-based proxy
print("  Using skewness + kurtosis proxy (full Shapiro-Wilk needs scipy):")
for col in ["net_sales", "quantity", "unit_price", "margin_pct"]:
    a  = df[col].dropna().to_numpy(dtype=float)
    sk = abs(pd.Series(a).skew())
    kt = abs(pd.Series(a).kurt())
    # Heuristic: |skew| < 0.5 and |excess kurtosis| < 0.5 → approximately normal
    verdict = "approx normal" if (sk < 0.5 and kt < 0.5) else "non-normal"
    print(f"  {col:<20}  |skew|={sk:.3f}  |kurt|={kt:.3f}  → {verdict}")

h("Log-transform effect on skewed columns")
for col in ["net_sales", "gross_profit"]:
    a     = df[col].dropna().to_numpy(dtype=float)
    a_log = np.log1p(a)
    sk_raw = pd.Series(a).skew()
    sk_log = pd.Series(a_log).skew()
    print(f"  {col:<20}  raw skew={sk_raw:+.3f}  log skew={sk_log:+.3f}  "
          f"{'✔ improved' if abs(sk_log) < abs(sk_raw) else '✘ no improvement'}")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 10 — CORRELATION DEEP-DIVE
# ══════════════════════════════════════════════════════════════════════════════
section(10, "CORRELATION DEEP-DIVE")

num_df = df[corr_cols].astype(float)

h("Pearson  (linear relationships — sensitive to outliers)")
pearson = num_df.corr(method="pearson").round(3)
print(pearson.to_string())

h("Spearman  (monotonic relationships — rank-based, robust to outliers)")
spearman = num_df.corr(method="spearman").round(3)
print(spearman.to_string())



# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 11 — FEATURE ENGINEERING
# ══════════════════════════════════════════════════════════════════════════════
section(11, "FEATURE ENGINEERING")

fe = df.copy()

h("Price-to-cost ratio (markup)")
fe["markup_ratio"] = (fe["unit_price"] / fe["cost_price"]).round(3)
show("markup_ratio stats:", fe["markup_ratio"].describe().round(3))

h("Revenue efficiency = net_sales per unit (average selling price after discount)")
fe["asp"] = (fe["net_sales"] / fe["quantity"]).round(2)   # Average Selling Price
show("asp (avg selling price) stats:", fe["asp"].describe().round(2))

h("Log transforms for skewed measures")
for col in ["net_sales", "gross_profit", "unit_price"]:
    fe[f"log_{col}"] = np.log1p(fe[col])
print("  Created: log_net_sales, log_gross_profit, log_unit_price")

h("Binary flags")
fe["is_discounted"]   = (fe["discount"] > 0).astype(int)
fe["is_high_margin"]  = (fe["margin_pct"] > fe["margin_pct"].median()).astype(int)
fe["is_bulk"]         = (fe["quantity"] >= 5).astype(int)
print("  Created: is_discounted, is_high_margin, is_bulk")
print(fe[["transaction_id","is_discounted","is_high_margin","is_bulk"]].head(6).to_string(index=False))

h("Ordinal encoding — segment")
segment_order = {"Basic": 0, "Standard": 1, "Premium": 2}
fe["segment_ord"] = fe["segment"].map(segment_order)
show("segment → ordinal:", fe[["segment","segment_ord"]].drop_duplicates().sort_values("segment_ord").to_string(index=False))

h("One-hot encoding — region")
region_dummies = pd.get_dummies(fe["region"], prefix="region", dtype=int)
fe = pd.concat([fe, region_dummies], axis=1)
print(f"  Region dummies created: {list(region_dummies.columns)}")

h("Interaction feature — quantity × unit_price (same as gross_sales but as a feature)")
fe["qty_x_price"] = fe["quantity"] * fe["unit_price"]
r_interact = np.corrcoef(fe["qty_x_price"], fe["net_sales"])[0, 1]
print(f"  Correlation of qty×price with net_sales: r = {r_interact:.3f}")

h("Z-score normalisation (for ML-ready features)")
for col in ["net_sales", "quantity", "unit_price"]:
    mu, sigma = fe[col].mean(), fe[col].std()
    fe[f"z_{col}"] = ((fe[col] - mu) / sigma).round(4)
print("  Created: z_net_sales, z_quantity, z_unit_price")
print(fe[["z_net_sales","z_quantity","z_unit_price"]].describe().round(3).to_string())

h("Min-max normalisation → [0, 1]")
for col in ["net_sales", "margin_pct"]:
    mn, mx = fe[col].min(), fe[col].max()
    fe[f"norm_{col}"] = ((fe[col] - mn) / (mx - mn)).round(4)
print("  Created: norm_net_sales, norm_margin_pct")
print(fe[["norm_net_sales","norm_margin_pct"]].describe().round(3).to_string())


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 12 — TIME PATTERNS
# ══════════════════════════════════════════════════════════════════════════════
section(12, "TIME PATTERNS")

ts = df.copy()
ts["dow"]          = ts["transaction_date"].dt.day_name()
ts["day_num"]      = ts["transaction_date"].dt.dayofweek   # 0=Mon
ts["week"]         = ts["transaction_date"].dt.isocalendar().week.astype(int)

h("Daily revenue trend")
daily = ts.groupby("transaction_date")["net_sales"].sum().reset_index()
daily.columns = ["date", "net_sales"]
daily["pct_change"]     = daily["net_sales"].pct_change().mul(100).round(1)
daily["rolling_3d_avg"] = daily["net_sales"].rolling(3, min_periods=1).mean().round(2)
daily["cumulative"]     = daily["net_sales"].cumsum().round(2)
print(daily.to_string(index=False))

h("Best and worst days")
print(f"  Best day  : {daily.loc[daily['net_sales'].idxmax(), 'date'].date()}  "
      f"(${daily['net_sales'].max():.2f})")
print(f"  Worst day : {daily.loc[daily['net_sales'].idxmin(), 'date'].date()}  "
      f"(${daily['net_sales'].min():.2f})")
print(f"  Day-over-day volatility (std of pct_change): "
      f"{daily['pct_change'].std():.1f}%")

h("Revenue by day of week")
dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
dow_rev = ts.groupby("dow")["net_sales"].agg(["sum","mean","count"])
dow_rev = dow_rev.reindex([d for d in dow_order if d in dow_rev.index])
for day, row in dow_rev.iterrows():
    bar = "█" * int(row["sum"] / dow_rev["sum"].max() * 30)
    print(f"  {day:<12}  total={row['sum']:>7.2f}  avg={row['mean']:>7.2f}  "
          f"n={int(row['count']):>2}  {bar}")

h("Week-over-week comparison")
wk = ts.groupby("week")["net_sales"].agg(["sum","count"]).rename(
     columns={"sum":"total","count":"orders"})
wk["wow_growth"] = wk["total"].pct_change().mul(100).round(1)
print(wk.to_string())

h("Rolling statistics (3-day window)")
daily["roll_std"]  = daily["net_sales"].rolling(3, min_periods=1).std().round(2)
daily["roll_min"]  = daily["net_sales"].rolling(3, min_periods=1).min().round(2)
daily["roll_max"]  = daily["net_sales"].rolling(3, min_periods=1).max().round(2)
print(daily[["date","net_sales","rolling_3d_avg","roll_std","roll_min","roll_max"]].to_string(index=False))

h("Trend detection via NumPy linear regression")
x = np.arange(len(daily))
y = daily["net_sales"].to_numpy()
m, b = np.polyfit(x, y, 1)
y_hat = m * x + b
ss_res = np.sum((y - y_hat) ** 2)
ss_tot = np.sum((y - y.mean()) ** 2)
r2 = 1 - ss_res / ss_tot
print(f"  Linear trend: y = {m:.2f}x + {b:.2f}")
print(f"  R² = {r2:.3f}  "
      f"→ {'upward' if m > 0 else 'downward'} trend, "
      f"{'strong' if r2 > 0.5 else 'weak'} fit")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 13 — COHORT & SEGMENTATION ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
section(13, "COHORT & SEGMENTATION ANALYSIS")

seg = df.copy()

h("Customer-level aggregation (RFM proxies)")
today = seg["transaction_date"].max()
rfm = seg.groupby("customer_id").agg(
    recency_days   = ("transaction_date",   lambda x: (today - x.max()).days),
    frequency      = ("transaction_id",     "count"),
    monetary_total = ("net_sales",          "sum"),
    avg_order_val  = ("net_sales",          "mean"),
    total_items    = ("quantity",           "sum"),
    avg_margin     = ("margin_pct",         "mean"),
).round(2)

rfm = rfm.merge(cust[["customer_id","segment"]], on="customer_id", how="left")
show("RFM table (all customers):", rfm.set_index("customer_id").to_string())

h("Segment-level profile")
seg_profile = rfm.groupby("segment").agg(
    customers      = ("frequency",      "count"),
    avg_recency    = ("recency_days",   "mean"),
    avg_frequency  = ("frequency",      "mean"),
    avg_monetary   = ("monetary_total", "mean"),
    total_revenue  = ("monetary_total", "sum"),
    avg_margin_pct = ("avg_margin",     "mean"),
).round(2)
seg_profile["revenue_share%"] = (seg_profile["total_revenue"] /
                                  seg_profile["total_revenue"].sum() * 100).round(1)
print(seg_profile.to_string())

h("RFM scoring (1–3 scale per dimension)")
def score_col(series, ascending=True):
    """Assign scores 1, 2, 3 using tertile bins."""
    labels = [1, 2, 3] if ascending else [3, 2, 1]
    try:
        return pd.qcut(series, q=3, labels=labels, duplicates="drop").astype(int)
    except ValueError:
        return pd.Series([2] * len(series), index=series.index)

rfm["R_score"] = score_col(rfm["recency_days"],   ascending=False)  # lower recency = better
rfm["F_score"] = score_col(rfm["frequency"],      ascending=True)
rfm["M_score"] = score_col(rfm["monetary_total"], ascending=True)
rfm["RFM_sum"] = rfm["R_score"] + rfm["F_score"] + rfm["M_score"]

def rfm_label(score):
    if score >= 8: return "Champions"
    if score >= 6: return "Loyal"
    if score >= 4: return "At Risk"
    return "Lost"

rfm["rfm_segment"] = rfm["RFM_sum"].apply(rfm_label)
print(rfm[["customer_id","R_score","F_score","M_score","RFM_sum","rfm_segment","segment"]].to_string(index=False))

h("Product contribution analysis (ABC segmentation)")
product_rev = seg.groupby("product_name")["net_sales"].sum().sort_values(ascending=False)
total_rev   = product_rev.sum()
product_rev = product_rev.reset_index()
product_rev["cumulative_pct"] = (product_rev["net_sales"].cumsum() / total_rev * 100).round(1)
product_rev["abc"] = product_rev["cumulative_pct"].apply(
    lambda p: "A (top 70%)" if p <= 70 else "B (70–90%)" if p <= 90 else "C (tail)")
print(product_rev.to_string(index=False))

h("High-value vs low-value transaction split")
threshold = df["net_sales"].median()
hv = df[df["net_sales"] >= threshold]
lv = df[df["net_sales"] <  threshold]

print(f"\n  Threshold (median): ${threshold:.2f}")
print(f"\n  {'Metric':<25}  {'High-Value':>12}  {'Low-Value':>12}")
print("  " + "─" * 52)
for label, val_hv, val_lv in [
    ("Transactions",      len(hv),                  len(lv)),
    ("Total revenue",     hv["net_sales"].sum(),     lv["net_sales"].sum()),
    ("Avg order value",   hv["net_sales"].mean(),    lv["net_sales"].mean()),
    ("Avg margin %",      hv["margin_pct"].mean(),   lv["margin_pct"].mean()),
    ("Avg discount",      hv["discount"].mean(),     lv["discount"].mean()),
]:
    print(f"  {label:<25}  {val_hv:>12.2f}  {val_lv:>12.2f}")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 14 — EDA SUMMARY REPORT
# ══════════════════════════════════════════════════════════════════════════════
section(14, "AUTO-GENERATED EDA SUMMARY REPORT")

findings = []

# ── Shape ────────────────────────────────────────────────────────────────────
findings.append(f"Dataset has {df.shape[0]} rows × {df.shape[1]} columns "
                f"across {df['customer_id'].nunique()} customers and "
                f"{df['product_id'].nunique()} products.")

# ── Missing ───────────────────────────────────────────────────────────────────
n_miss = df.isna().sum().sum()
findings.append("No missing values detected in the raw dataset." if n_miss == 0
                else f"{n_miss} missing values found — imputation required before modelling.")

# ── Duplicates ────────────────────────────────────────────────────────────────
findings.append(f"No duplicate transaction IDs found — data integrity confirmed."
                if df.duplicated(subset=["transaction_id"]).sum() == 0
                else "Duplicate transaction IDs detected — deduplication needed.")

# ── Revenue ───────────────────────────────────────────────────────────────────
total_rev  = df["net_sales"].sum()
avg_order  = df["net_sales"].mean()
top_region = df.groupby("region")["net_sales"].sum().idxmax()
findings.append(f"Total net revenue = ${total_rev:,.2f}  |  "
                f"Average order value = ${avg_order:.2f}.")
findings.append(f"'{top_region}' is the highest-revenue region.")

# ── Margin ────────────────────────────────────────────────────────────────────
avg_margin = df["margin_pct"].mean()
best_prod  = df.groupby("product_name")["margin_pct"].mean().idxmax()
worst_prod = df.groupby("product_name")["margin_pct"].mean().idxmin()
findings.append(f"Average gross margin = {avg_margin:.1f}%.  "
                f"Best: '{best_prod}'.  Worst: '{worst_prod}'.")

# ── Outliers ──────────────────────────────────────────────────────────────────
arr = df["net_sales"].to_numpy(dtype=float)
q1_, q3_ = np.percentile(arr, [25, 75])
iqr_ = q3_ - q1_
n_out = ((arr < q1_ - 1.5*iqr_) | (arr > q3_ + 1.5*iqr_)).sum()
findings.append(f"{n_out} outlier(s) found in net_sales by IQR method — review before regression.")

# ── Correlation ───────────────────────────────────────────────────────────────
r_up = np.corrcoef(df["unit_price"].astype(float), df["net_sales"].astype(float))[0,1]
r_qty = np.corrcoef(df["quantity"].astype(float), df["net_sales"].astype(float))[0,1]
findings.append(f"unit_price has the strongest positive correlation with net_sales "
                f"(r={r_up:.2f}); quantity is negatively correlated (r={r_qty:.2f}).")

# ── Skewness ──────────────────────────────────────────────────────────────────
sk = pd.Series(df["net_sales"]).skew()
findings.append(f"net_sales is {'right' if sk > 0 else 'left'}-skewed (skew={sk:.2f}) "
                f"— log transform recommended for linear models.")

# ── Segment ───────────────────────────────────────────────────────────────────
best_seg = df.groupby("segment")["net_sales"].mean().idxmax()
findings.append(f"'{best_seg}' customers generate the highest average order value.")

# ── Trend ─────────────────────────────────────────────────────────────────────
findings.append(f"Linear revenue trend: slope={m:.2f} $/day, R²={r2:.2f} "
                f"({'upward' if m > 0 else 'downward'} trend, "
                f"{'strong' if r2 > 0.5 else 'weak'} fit).")

# ── Print report ──────────────────────────────────────────────────────────────
print("\n" + "╔" + "═"*60 + "╗")
print("║   EDA FINDINGS SUMMARY" + " "*38 + "║")
print("╠" + "═"*60 + "╣")
for i, f in enumerate(findings, 1):
    # Word-wrap at 56 chars
    words, line = f.split(), ""
    lines_out = []
    for w in words:
        if len(line) + len(w) + 1 > 56:
            lines_out.append(line)
            line = w
        else:
            line = (line + " " + w).strip()
    if line:
        lines_out.append(line)
    for j, l in enumerate(lines_out):
        prefix = f"  {i}. " if j == 0 else "     "
        print(f"║ {prefix}{l:<55} ║")
    print("║" + " "*60 + "║")
print("╚" + "═"*60 + "╝")

print("\n✔ EDA complete.  Next steps: feature selection, model training.\n")
