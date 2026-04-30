# ============================================================
# Exercise 9 — JSON Transformer                       [MEDIUM]
# Topic: File Handling
# ============================================================
#
# TASK:
# Write a function flatten(d, prefix="") that flattens a nested
# dictionary into dot notation keys.
# e.g. {"user": {"address": {"city": "London"}}}
#   → {"user.address.city": "London"}
#
# Then print the result as formatted JSON.
#
# HINT:
# Use a recursive approach:
#   for each key/value in the dict:
#     if the value is a dict → recurse with prefix + "." + key
#     otherwise → assign directly to result[full_key]
# Use json.dumps(flat, indent=2) to print nicely.
# ============================================================

import json

payload = {
    "user": {
        "id": 42,
        "name": "Alice",
        "address": {
            "city": "London",
            "postcode": "EC1A 1BB"
        }
    },
    "order": {"id": 999, "total": 84.50}
}

# --- YOUR CODE HERE ---
def flatten(d: dict, prefix: str = "") -> dict:
    pass


flat = flatten(payload)
print(json.dumps(flat, indent=2))


# ============================================================
# SOLUTION
# ============================================================

def solution():
    def flatten(d: dict, prefix: str = "") -> dict:
        """Recursively flatten a nested dict using dot notation."""
        result = {}
        for key, val in d.items():
            full_key = f"{prefix}.{key}" if prefix else key
            if isinstance(val, dict):
                result.update(flatten(val, full_key))
            else:
                result[full_key] = val
        return result

    flat = flatten(payload)
    print(json.dumps(flat, indent=2))

    # Optionally write to file:
    # with open("flat_output.json", "w") as f:
    #     json.dump(flat, f, indent=2)

if __name__ == "__main__":
    solution()
