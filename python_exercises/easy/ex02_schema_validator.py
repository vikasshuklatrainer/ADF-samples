# ============================================================
# Exercise 2 — Schema Validator                        [EASY]
# Topic: Data Types
# ============================================================
#
# TASK:
# You receive a dictionary representing a database row:
#   row = {'id': '123', 'name': 'Alice', 'score': '87.5', 'active': 'true'}
#
# Write a function cast_row(row) that converts each value to
# its correct Python type:
#   id     → int
#   name   → str (stripped)
#   score  → float
#   active → bool
# Return the cleaned dict. Handle bad data gracefully.
#
# Expected output:
#   {'id': 123, 'name': 'Alice', 'score': 87.5, 'active': True}
#
# HINT:
# For the boolean, check if the string lowercased equals 'true'.
# Use a try/except around each conversion in case of bad data.
# ============================================================

row = {'id': '123', 'name': 'Alice', 'score': '87.5', 'active': 'true'}

# --- YOUR CODE HERE ---
def cast_row(row: dict) -> dict:
    pass


print(cast_row(row))


# ============================================================
# SOLUTION
# ============================================================

def solution():
    def cast_row(row: dict) -> dict:
        """Cast a raw string-dict to proper Python types."""
        casters = {
            'id':     lambda v: int(v),
            'name':   lambda v: str(v).strip(),
            'score':  lambda v: float(v),
            'active': lambda v: v.lower() == 'true',
        }
        result = {}
        for key, val in row.items():
            try:
                result[key] = casters[key](val) if key in casters else val
            except (ValueError, AttributeError) as e:
                print(f"Warning: could not cast '{key}' — {e}")
                result[key] = None
        return result

    row = {'id': '123', 'name': 'Alice', 'score': '87.5', 'active': 'true'}
    cleaned = cast_row(row)
    print(cleaned)
    # {'id': 123, 'name': 'Alice', 'score': 87.5, 'active': True}

if __name__ == "__main__":
    solution()
