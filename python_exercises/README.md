# Python for Data Engineering — Exercise Pack
## 15 Hands-on Exercises with Full Solutions

---

## Structure

```
python_exercises/
├── README.md
├── easy/
│   ├── ex01_type_detective.py
│   ├── ex02_schema_validator.py
│   ├── ex03_fizzbuzz_pipeline.py
│   ├── ex04_word_frequency.py
│   └── ex05_set_operations.py
├── medium/
│   ├── ex06_safe_converter.py
│   ├── ex07_retry_decorator.py
│   ├── ex08_csv_aggregator.py
│   ├── ex09_json_transformer.py
│   ├── ex10_parquet_inspector.py
│   ├── ex11_api_fetcher.py
│   └── ex12_cli_runner.py
└── hard/
    ├── ex13_custom_exceptions.py
    ├── ex14_async_api.py
    └── ex15_full_etl.py
```

---

## Exercise Overview

| # | Title | Level | Topic |
|---|-------|-------|-------|
| 1 | Type Detective | Easy | Data Types |
| 2 | Schema Validator | Easy | Data Types |
| 3 | FizzBuzz Pipeline | Easy | Loops |
| 4 | Word Frequency Counter | Easy | Loops |
| 5 | Set Operations on Column Names | Easy | Data Types |
| 6 | Safe Type Converter with Logging | Medium | Error Handling |
| 7 | Retry Decorator | Medium | Error Handling |
| 8 | CSV Reader and Aggregator | Medium | File Handling |
| 9 | JSON Transformer | Medium | File Handling |
| 10 | Parquet Schema Inspector | Medium | File Handling |
| 11 | REST API Data Fetcher | Medium | APIs |
| 12 | CLI Pipeline Runner | Medium | Automation |
| 13 | Pipeline with Custom Exceptions | Hard | Error Handling |
| 14 | Async Batch API Caller | Hard | APIs |
| 15 | Full ETL Automation Script | Hard | Automation |

---

## How to Use Each File

Each `.py` file follows this structure:

1. **Header** — exercise number, title, difficulty, topic
2. **Task description** — what you need to build
3. **Hint** — a nudge in the right direction
4. **Starter code** — function stubs for you to fill in
5. **Solution** — wrapped in a `solution()` function at the bottom

To attempt an exercise:
- Write your code in the `# --- YOUR CODE HERE ---` section
- Run the file: `python ex01_type_detective.py`

To reveal the solution:
- Scroll to the bottom of the file, or
- Call `solution()` directly

---

## Setup

### Install dependencies

```bash
pip install requests aiohttp pyarrow pandas
```

### Python version
Requires Python 3.8 or later (3.10+ recommended for match statements).

---

## Recommended Order

**Week 1 — Python Foundations**
- Ex 1, 2, 5 (data types)
- Ex 3, 4 (loops)

**Week 2 — Resilient Code**
- Ex 6 (safe converter + logging)
- Ex 7 (retry decorator)
- Ex 13 (custom exceptions)

**Week 3 — File Handling**
- Ex 8 (CSV)
- Ex 9 (JSON)
- Ex 10 (Parquet)

**Week 4 — APIs & Automation**
- Ex 11 (REST API)
- Ex 12 (CLI runner)
- Ex 14 (async API)
- Ex 15 (full ETL — capstone)
