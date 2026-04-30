# ============================================================
# Exercise 3 — FizzBuzz Pipeline                       [EASY]
# Topic: Loops
# ============================================================
#
# TASK:
# Generate numbers 1–30 and build a list of tuples (number, label)
# where label is:
#   'Fizz'     if divisible by 3
#   'Buzz'     if divisible by 5
#   'FizzBuzz' if divisible by both
#   str(n)     otherwise
#
# Then print ONLY the rows where the label is not a plain number.
#
# HINT:
# Build the list with a list comprehension using a helper function.
# Use % for modulo. Filter with another comprehension.
# str.isdigit() returns True if all characters are digits.
# ============================================================

# --- YOUR CODE HERE ---


# ============================================================
# SOLUTION
# ============================================================

def solution():
    def label(n: int) -> str:
        if n % 15 == 0: return 'FizzBuzz'
        if n % 3  == 0: return 'Fizz'
        if n % 5  == 0: return 'Buzz'
        return str(n)

    rows = [(n, label(n)) for n in range(1, 31)]

    # Filter out plain numbers
    special = [(n, lbl) for n, lbl in rows if not lbl.isdigit()]

    for n, lbl in special:
        print(f"{n:>2}  →  {lbl}")

if __name__ == "__main__":
    solution()
