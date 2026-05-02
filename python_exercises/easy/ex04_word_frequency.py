# ============================================================
# Exercise 4 — Word Frequency Counter                  [EASY]
# Topic: Loops
# ============================================================
#
# TASK:
# Given the text string below, write a function word_freq(text)
# that returns a dictionary of word frequencies
# (case-insensitive, stripped of punctuation).
# Then print the top 5 most common words.
#
# HINT:
# Use re.findall(r"[a-z]+", text.lower()) to extract words cleanly.
# Use collections.Counter to count them.
# Sort with: sorted(freq.items(), key=lambda x: x[1], reverse=True)
# ============================================================

# ============================================================
# Exercise 4 — Word Frequency Counter                  [EASY]
# Topic: Loops
# ============================================================

import re
from collections import Counter

text = """Data engineering involves collecting, processing,
and storing data. Engineers abhinav write pipelines that move
data from sources to destinations. Data quality abhinav matters."""

def word_freq(text: str) -> dict:
    words = re.findall(r"[a-z]+", text.lower())
    return dict(Counter(words))

if __name__ == "__main__":
    freq = word_freq(text)
    top5 = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:5]

    print("Top 5 words:")
    for word, count in top5:
        print(f"  {word:<15} {count}")