# ============================================================
# Exercise 11 — REST API Data Fetcher                [MEDIUM]
# Topic: APIs
# ============================================================
#
# REQUIRES: pip install requests
#
# TASK:
# Fetch all posts from: https://jsonplaceholder.typicode.com/posts
# Then:
#   1. Extract only: userId, id, title from each post
#   2. Save the result to "posts.csv"
#   3. Print the count of saved posts and a preview of the first 2
#
# HINT:
# Use requests.get(url, timeout=10) and call .raise_for_status()
# to automatically raise an exception on HTTP errors.
# Use csv.DictWriter with fieldnames=["userId","id","title"].
# Call writer.writeheader() before writer.writerows().
# ============================================================

# ============================================================
# Exercise 11 — REST API Data Fetcher                [MEDIUM]
# Topic: APIs
# ============================================================

import requests
import csv

def fetch_posts(url: str) -> list:
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

def extract_fields(posts: list) -> list:
    return [
        {"userId": p["userId"], "id": p["id"], "title": p["title"]}
        for p in posts
    ]

def write_csv(records: list, path: str):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["userId", "id", "title"])
        writer.writeheader()
        writer.writerows(records)

if __name__ == "__main__":
    try:
        posts   = fetch_posts("https://jsonplaceholder.typicode.com/posts")
        records = extract_fields(posts)
        write_csv(records, "posts.csv")
        print(f"Saved {len(records)} posts to posts.csv")
        print(records[:2])
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")