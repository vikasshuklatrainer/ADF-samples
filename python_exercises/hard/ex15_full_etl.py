# ============================================================
# Exercise 15 — Full ETL Automation Script              [HARD]
# Topic: Automation
# ============================================================
#
# TASK:
# Combine everything you've learned into a complete ETL pipeline:
#
#   1. EXTRACT  — Read rows from a CSV string
#   2. VALIDATE — Cast id→int, name→str, score→float; raise ValidationError on failure
#   3. ENRICH   — Add a "grade" field (A/B/C) using a mock API call
#                 (use the @retry decorator in case it fails)
#   4. LOAD     — Write enriched records to "output.json"
#   5. TIMING   — Log each stage name and how long it took
#
# Use:
#   - Custom exceptions (ETLError, ValidationError)
#   - @retry decorator
#   - logging with timestamps
#   - time.perf_counter() for stage timing
#
# HINT:
# Split into: extract(), validate(), enrich_one(), load(), run().
# In run(), loop through stages, call each one, and log duration.
# The retry decorator wraps enrich_one() to handle flakiness.
# ============================================================

# ============================================================
# Exercise 15 — Full ETL Automation Script              [HARD]
# Topic: Automation
# ============================================================

import csv, io, json, time, logging, functools, random

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

RAW_CSV = "id,name,score\n1,Alice,88.5\n2,Bob,72\n3,Carol,95.1"


# 1. Custom exceptions
class ETLError(Exception):       pass
class ValidationError(ETLError): pass


# 2. Retry decorator
def retry(attempts=3, delay=0):
    def dec(fn):
        @functools.wraps(fn)
        def wrap(*a, **kw):
            for i in range(attempts):
                try:
                    return fn(*a, **kw)
                except Exception as e:
                    if i == attempts - 1:
                        raise
                    log.warning(f"Retry {i+1}: {e}")
                    time.sleep(delay)
        return wrap
    return dec


# 3. Extract
def extract() -> list:
    return list(csv.DictReader(io.StringIO(RAW_CSV)))


# 4. Validate
def validate(rows: list) -> list:
    out = []
    for r in rows:
        try:
            out.append({
                "id":    int(r["id"]),
                "name":  str(r["name"]),
                "score": float(r["score"])
            })
        except ValueError as e:
            raise ValidationError(f"Row {r}: {e}") from e
    return out


# 5. Enrich (with retry for flaky mock API)
@retry(attempts=3)
def enrich_one(row: dict) -> dict:
    if random.random() < 0.3:
        raise ConnectionError("Mock API flaky")
    row["grade"] = "A" if row["score"] >= 90 else "B" if row["score"] >= 70 else "C"
    return row


# 6. Load
def load(rows: list, path: str):
    with open(path, "w") as f:
        json.dump(rows, f, indent=2)


# 7. Pipeline runner
def run():
    stages = [
        ("extract",  lambda:    extract()),
        ("validate", lambda r:  validate(r)),
        ("enrich",   lambda r:  [enrich_one(x) for x in r]),
        ("load",     lambda r:  load(r, "output.json") or r),
    ]
    data = None
    for name, fn in stages:
        t = time.perf_counter()
        data = fn() if data is None else fn(data)
        log.info(f"[{name.upper():<8}] {time.perf_counter() - t:.3f}s")
    log.info(f"Pipeline done — {len(data)} records written to output.json")


if __name__ == "__main__":
    run()