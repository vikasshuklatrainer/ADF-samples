# ============================================================
# Exercise 6 — Safe Type Converter with Logging       [MEDIUM]
# Topic: Error Handling
# ============================================================
#
# TASK:
# Write a function safe_convert(value, target_type) that:
#   - Attempts to convert value to the given type (int, float, bool, str)
#   - If conversion fails, logs a WARNING and returns None
#   - Uses the logging module (not print)
#
# Test it with the list of (value, type) pairs below.
#
# HINT:
# Import logging and call logging.basicConfig(level=logging.WARNING).
# Use logging.warning() instead of print() for failures.
# For bool conversion, first cast to int then bool:
#   bool(int(value))
# ============================================================

import logging

logging.basicConfig(
    level=logging.WARNING,
    format='%(levelname)s: %(message)s'
)

tests = [
    ("42",    int),
    ("3.14",  float),
    ("hello", int),    # should warn
    ("1",     bool),
    (None,    str),
]

# --- YOUR CODE HERE ---
def safe_convert(value, target_type):
    pass


for val, typ in tests:
    result = safe_convert(val, typ)
    print(f"{val!r:<10} → {typ.__name__:<6} = {result!r}")


# ============================================================
# SOLUTION
# ============================================================

def solution():
    def safe_convert(value, target_type):
        """Convert value to target_type; return None on failure."""
        try:
            if target_type is bool:
                return bool(int(value))
            return target_type(value)
        except (ValueError, TypeError) as e:
            logging.warning(
                f"Cannot convert {value!r} to {target_type.__name__}: {e}"
            )
            return None

    for val, typ in tests:
        result = safe_convert(val, typ)
        print(f"{val!r:<10} → {typ.__name__:<6} = {result!r}")

if __name__ == "__main__":
    solution()
