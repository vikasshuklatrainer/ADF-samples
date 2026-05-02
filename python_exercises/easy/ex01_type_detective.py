# ============================================================
# Exercise 1 — Type Detective                          [EASY]
# Topic: Data Types
# ============================================================
#
# TASK:
# Given the list below, write a script that prints each value
# alongside its Python type, and counts how many items are
# integers (excluding booleans).
#
# values = [42, '99', 3.14, True, None, '007', False]
#
# Expected output:
#   42         → int
#   '99'       → str
#   3.14       → float
#   True       → bool
#   None       → NoneType
#   '007'      → str
#   False      → bool
#   Integer count (excl. bool): 1
#
# HINT:
# Use the built-in type() function and isinstance(v, int).
# Note: in Python, bool is a subclass of int, so handle
# that carefully with: isinstance(v, int) and not isinstance(v, bool)
# ============================================================

values = [42, '99', 3.14, True, None, '007', False]

# --- YOUR CODE HERE ---


# ============================================================
# SOLUTION (remove or fold this section before giving to students)
# ============================================================

def solution():
    values = [42, '99', 3.14, True, None, '007', False]

    # Print each value with its type
    for v in values:
        print(f"{v!r:<10} → {type(v).__name__}")

    # Count integers — exclude booleans using explicit check
    int_count = sum(
        1 for v in values
        if isinstance(v, int) and not isinstance(v, bool)
    )
    print(f"\nInteger count (excl. bool): {int_count}")

if __name__ == "__main__":
    # solution()
    values = [42, '99', 3.14, True, None, '007', False]
    for v in values:
        print(f"{v!r:<10} → {type(v).__name__}")

        int_count = sum(
        1 for v in values
        if isinstance(v, int) and not isinstance(v, bool)

        )
        print(f"\nInteger count (excl. bool): {int_count}")   
