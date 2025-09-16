import os
import time
import unittest
from funkcache import SQLiteCache


class TestSQLiteCache(unittest.TestCase):
    DB_FILE = "test_cache.db"

    def setUp(self):
        if os.path.exists(self.DB_FILE):
            os.remove(self.DB_FILE)
        self.cache = SQLiteCache(self.DB_FILE, ttl_seconds=1)

    def tearDown(self):
        if os.path.exists(self.DB_FILE):
            os.remove(self.DB_FILE)

    def test_cache_hit_and_miss(self):
        calls = {"count": 0}

        @self.cache.cache(ttl_seconds=1)
        def add(a, b):
            calls["count"] += 1
            return a + b

        # First call → MISS
        self.assertEqual(add(2, 3), 5)
        self.assertEqual(calls["count"], 1)

        # Second call → HIT
        self.assertEqual(add(2, 3), 5)
        self.assertEqual(calls["count"], 1)

        # After expiration → MISS again
        time.sleep(1.2)
        self.assertEqual(add(2, 3), 5)
        self.assertEqual(calls["count"], 2)


if __name__ == "__main__":
    unittest.main()
