# ============================================================
# Exercise 13 — Pipeline with Custom Exceptions        [HARD]
# Topic: Error Handling
# ============================================================
#
# TASK:
# Build a mini ETL pipeline with three stages: Extract, Transform, Load.
# Define a custom exception hierarchy:
#   PipelineError (base)
#   ├── ExtractError
#   ├── TransformError
#   └── LoadError
#
# Each stage function should raise its own error type on failure.
# The run_pipeline() function should catch each specifically and
# log the failing stage name.
#
# Test with:
#   run_pipeline("sales.csv",  "warehouse")   # TransformError (bad int cast)
#   run_pipeline("sales.txt",  "warehouse")   # ExtractError (wrong extension)
#   run_pipeline("sales.csv",  "readonly_db") # LoadError (read-only target)
#
# HINT:
# Inherit from Exception to create custom exceptions:
#   class ExtractError(PipelineError): pass
#
# Use "raise X from e" to chain exceptions — it preserves the
# original traceback and is considered best practice.
# ============================================================

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

# --- YOUR CODE HERE ---

# Define exception hierarchy:

# Define pipeline stages:
def extract(source: str):
    pass

def transform(rows: list):
    pass

def load(rows: list, dest: str):
    pass

def run_pipeline(source: str, dest: str):
    pass


run_pipeline("sales.csv",  "warehouse")
run_pipeline("sales.txt",  "warehouse")
run_pipeline("sales.csv",  "readonly_db")


# ============================================================
# SOLUTION
# ============================================================

def solution():
    class PipelineError(Exception):    pass
    class ExtractError(PipelineError): pass
    class TransformError(PipelineError): pass
    class LoadError(PipelineError):    pass

    def extract(source: str):
        if not source.endswith(".csv"):
            raise ExtractError(f"Unsupported format: {source}")
        return [{"id": 1, "val": "42"}, {"id": 2, "val": "bad"}]

    def transform(rows: list):
        try:
            return [{"id": r["id"], "val": int(r["val"])} for r in rows]
        except ValueError as e:
            raise TransformError(f"Cast failed: {e}") from e

    def load(rows: list, dest: str):
        if dest == "readonly_db":
            raise LoadError("Target is read-only")
        logging.info(f"Loaded {len(rows)} rows to {dest}")

    def run_pipeline(source: str, dest: str):
        try:
            rows = extract(source)
            rows = transform(rows)
            load(rows, dest)
        except ExtractError   as e: logging.error(f"[EXTRACT]   {e}")
        except TransformError as e: logging.error(f"[TRANSFORM] {e}")
        except LoadError      as e: logging.error(f"[LOAD]      {e}")

    run_pipeline("sales.csv",  "warehouse")
    run_pipeline("sales.txt",  "warehouse")
    run_pipeline("sales.csv",  "readonly_db")

if __name__ == "__main__":
    solution()
