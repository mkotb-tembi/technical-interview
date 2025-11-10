# Technical Interview (Web Scraper ETL Pipeline)

This Python script implements a simple ETL pipeline for web scraping, including caching, rate limiting, and storing word counts. The ETL flow is divided into:

- **Extractor** → Fetches HTML from URLs.

- **Transformer** → Cleans HTML and counts word frequencies.

- **Loader** → Saves processed data in a key-value store.

- **LRU Cache** → Caches recent results to avoid repeated fetches.

- **Rate Limiter** → Prevents too many requests per user.

- **KV Store** → Stores results with optional expiry and automatic cleanup.

**Usage**

Run the script:

```py
python main.py
```

Example output:

```txt
[CACHE] Using cached data for https://example.com
[INFO] Processed https://www.python.org, 120 unique words
[WARN] Rate limit exceeded for guest
```

**Refactoring Task**

**Objective:** Refactor this ETL scraper code for production readiness, maintainability, and scalability.
