# ============================================================
# Exercise 7 — Retry Decorator                        [MEDIUM]
# Topic: Error Handling
# ============================================================
#
# TASK:
# Write a @retry(max_attempts=3, delay=1) decorator that:
#   - Retries a function when it raises an exception
#   - Waits `delay` seconds between attempts
#   - Re-raises the last exception if all attempts fail
#   - Logs each failed attempt
#
# Apply it to the flaky_api_call() function that randomly fails.
#
# HINT:
# A decorator factory returns a decorator which returns a wrapper.
# Structure:
#   def retry(max_attempts, delay):
#       def decorator(func):
#           @functools.wraps(func)
#           def wrapper(*args, **kwargs):
#               ...
#           return wrapper
#       return decorator
#
# Use time.sleep(delay) between retries.
# Use functools.wraps to preserve function metadata.
# ============================================================

import time, random, functools, logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

# --- YOUR CODE HERE ---
def retry(max_attempts=3, delay=1):
    pass


# Apply your decorator here:
# @retry(max_attempts=3, delay=0)
def flaky_api_call():
    """Simulates an API that fails 70% of the time."""
    if random.random() < 0.7:
        raise ConnectionError("API timeout")
    return {"status": "ok", "records": 42}


try:
    result = flaky_api_call()
    print("Success:", result)
except ConnectionError:
    print("All attempts failed — pipeline aborted.")


# ============================================================
# SOLUTION
# ============================================================

def solution():
    def retry(max_attempts=3, delay=1):
        """Decorator: retry on exception up to max_attempts times."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                last_exc = None
                for attempt in range(1, max_attempts + 1):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        last_exc = e
                        logging.info(f"Attempt {attempt} failed: {e}")
                        if attempt < max_attempts:
                            time.sleep(delay)
                raise last_exc
            return wrapper
        return decorator

    @retry(max_attempts=3, delay=0)
    def flaky_api_call():
        if random.random() < 0.7:
            raise ConnectionError("API timeout")
        return {"status": "ok", "records": 42}

    try:
        result = flaky_api_call()
        print("Success:", result)
    except ConnectionError:
        print("All attempts failed — pipeline aborted.")

if __name__ == "__main__":
    solution()
