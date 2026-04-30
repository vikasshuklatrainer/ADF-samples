# ============================================================
# Exercise 14 — Async Batch API Caller                 [HARD]
# Topic: APIs
# ============================================================
#
# REQUIRES: pip install aiohttp requests
#
# TASK:
# Fetch posts 1–10 from https://jsonplaceholder.typicode.com/posts/{id}
# Implement TWO versions and compare their speed:
#   1. Synchronous  — using requests, fetch one at a time
#   2. Asynchronous — using asyncio + aiohttp, fetch all concurrently
#
# Print:
#   Sync:  X.XXs  (10 posts)
#   Async: X.XXs  (10 posts)
#   Speedup: X.Xx
#
# HINT:
# For async:
#   - Define: async def fetch_one(session, post_id)
#   - Use:    async with session.get(url) as resp: return await resp.json()
#   - Run all: await asyncio.gather(*[fetch_one(session, i) for i in IDS])
#   - Wrap in: async with aiohttp.ClientSession() as session:
#   - Run with: asyncio.run(fetch_all_async())
# ============================================================

import asyncio, aiohttp, time, requests

BASE = "https://jsonplaceholder.typicode.com/posts/"
IDS  = range(1, 11)

# --- YOUR CODE HERE ---

# Sync version:
def fetch_all_sync():
    pass

# Async version:
async def fetch_all_async():
    pass


# Benchmark
t0 = time.perf_counter()
posts_sync = fetch_all_sync()
t_sync = time.perf_counter() - t0

t0 = time.perf_counter()
posts_async = asyncio.run(fetch_all_async())
t_async = time.perf_counter() - t0

print(f"Sync:  {t_sync:.2f}s  ({len(posts_sync)} posts)")
print(f"Async: {t_async:.2f}s  ({len(posts_async)} posts)")
print(f"Speedup: {t_sync/t_async:.1f}x")


# ============================================================
# SOLUTION
# ============================================================

def solution():
    async def fetch_one(session, post_id):
        async with session.get(f"{BASE}{post_id}") as resp:
            return await resp.json()

    async def fetch_all_async():
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_one(session, i) for i in IDS]
            return await asyncio.gather(*tasks)

    def fetch_all_sync():
        return [requests.get(f"{BASE}{i}").json() for i in IDS]

    t0 = time.perf_counter()
    posts_sync = fetch_all_sync()
    t_sync = time.perf_counter() - t0

    t0 = time.perf_counter()
    posts_async = asyncio.run(fetch_all_async())
    t_async = time.perf_counter() - t0

    print(f"Sync:  {t_sync:.2f}s  ({len(posts_sync)} posts)")
    print(f"Async: {t_async:.2f}s  ({len(posts_async)} posts)")
    print(f"Speedup: {t_sync/t_async:.1f}x")

if __name__ == "__main__":
    solution()
