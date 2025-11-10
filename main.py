import requests, time, re, threading, random
from collections import OrderedDict, deque, Counter

class LRUCache:
    def __init__(self, size=5):
        self.size = size
        self.cache = OrderedDict()

    def get(self, key):
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return None

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.size:
            self.cache.popitem(last=False)

class KVStore:
    def __init__(self):
        self.data = {}
        threading.Thread(target=self.cleanup, daemon=True).start()

    def set(self, key, value, ttl=None):
        self.data[key] = (value, time.time() + ttl if ttl else None)

    def get(self, key):
        value, expiry = self.data.get(key, (None, None))
        if expiry and expiry < time.time():
            self.data.pop(key, None)
            return None
        return value

    def cleanup(self):
        while True:
            time.sleep(1)
            now = time.time()
            expired = [k for k, (_, e) in self.data.items() if e and e < now]
            for k in expired:
                self.data.pop(k, None)

class Extractor:
    def fetch(self, url):
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                return r.text
            return ""
        except Exception as e:
            print(f"[ERROR] Failed to fetch {url}: {e}")
            return ""

class Transformer:
    def clean_text(self, html):
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def word_count(self, text):
        words = re.findall(r"\w+", text.lower())
        return Counter(words)

class Loader:
    def __init__(self, store):
        self.store = store

    def save(self, key, value):
        self.store.set(key, value, ttl=60)

class ScraperETL:
    def __init__(self):
        self.cache = LRUCache()
        self.extractor = Extractor()
        self.transformer = Transformer()
        self.store = KVStore()
        self.loader = Loader(self.store)

    def process(self, url):

        # Cache check
        cached = self.cache.get(url)
        if cached:
            print(f"[CACHE] Using cached data for {url}")
            return cached

        # Extract
        html = self.extractor.fetch(url)
        if not html:
            return None

        # Transform
        text = self.transformer.clean_text(html)
        counts = self.transformer.word_count(text)

        # Load
        self.loader.save(url, counts)
        self.cache.put(url, counts)
        print(f"[INFO] Processed {url}, {len(counts)} unique words")
        return counts

def main():
    etl = ScraperETL()
    urls = [
        "https://example.com",
        "https://www.python.org",
        "https://www.wikipedia.org",
        "https://example.com",
    ]
    for u in urls:
        result = etl.process(u)
        time.sleep(random.uniform(0.5, 1.5))

    print("\n[STORE CONTENTS]")
    for k, v in list(etl.store.data.items())[:3]:
        print(k, "=>", list(v[0].items())[:5])

if __name__ == "__main__":
    main()
