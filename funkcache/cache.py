#V3 (sqlite3 only, with logging)
import json
import hashlib
import sqlite3
import logging
from functools import wraps
from datetime import datetime, timedelta, timezone


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


class SQLiteCache:
    def __init__(self, db_path="funkcache_cache_manager.db", ttl_seconds=60):
        """
        Initialize SQLite cache manager.

        Args:
            db_path (str): Path to SQLite DB file.
            ttl_seconds (int): Default expiration time (in seconds).
        """
        self.db_path = db_path
        self.default_ttl = ttl_seconds

        # Create table if not exists
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS function_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    func_name TEXT NOT NULL,
                    key TEXT NOT NULL UNIQUE,
                    hash_val TEXT NOT NULL,
                    args TEXT,
                    kwargs TEXT,
                    result TEXT,
                    expires_at TEXT
                )
            """)
            conn.commit()

    def _make_key(self, func_name, args, kwargs, exp_time):
        raw = json.dumps(
            {"func": func_name, "args": args, "kwargs": kwargs, "exp_time": exp_time},
            sort_keys=True,
            default=str,
        )
        return hashlib.sha3_512(raw.encode()).hexdigest()

    def _make_hash(self, func_name, args, kwargs):
        raw = json.dumps(
            {"func": func_name, "args": args, "kwargs": kwargs},
            sort_keys=True,
            default=str,
        )
        return hashlib.sha3_512(raw.encode()).hexdigest()

    def cache(self, ttl_seconds=None):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                func_name = func.__qualname__

                # Handle bound methods: drop "self"
                call_args = args[1:] if args and hasattr(args[0], "__class__") else args

                args_json = json.dumps(call_args, default=str)
                kwargs_json = json.dumps(kwargs, default=str)

                now_utc = datetime.now(timezone.utc)
                ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl
                expires_at = now_utc + timedelta(seconds=ttl)

                key = self._make_key(func_name, args_json, kwargs_json, exp_time=expires_at.isoformat())
                hash_val = self._make_hash(func_name, args_json, kwargs_json)

                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()

                    # Remove expired entries for this hash
                    self.clear_expired_by_hash(hash_val, conn=conn)

                    # Check cache
                    cur.execute(
                        """SELECT result FROM function_cache
                           WHERE func_name=? AND hash_val=? AND expires_at > ?""",
                        (func_name, hash_val, now_utc.isoformat())
                    )
                    row = cur.fetchone()
                    if row:
                        logging.info(f"[CACHE HIT] {func_name}({call_args}, {kwargs})")
                        return json.loads(row["result"])

                    # Compute result
                    logging.info(f"[CACHE MISS] {func_name}({call_args}, {kwargs})")
                    result = func(*args, **kwargs)

                    # Insert/update cache
                    cur.execute(
                        """INSERT OR REPLACE INTO function_cache
                           (func_name, key, hash_val, args, kwargs, result, expires_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (
                            func_name,
                            key,
                            hash_val,
                            args_json,
                            kwargs_json,
                            json.dumps(result, default=str),
                            expires_at.isoformat(),
                        )
                    )
                    conn.commit()

                return result
            return wrapper
        return decorator

    def clear_cache(self, func_name=None):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            if func_name:
                cur.execute("DELETE FROM function_cache WHERE func_name=?", (func_name,))
                deleted = cur.rowcount
            else:
                cur.execute("DELETE FROM function_cache")
                deleted = cur.rowcount
            conn.commit()
        logging.info(f"[CACHE CLEARED] {deleted} entries removed")

    def clear_expired_cache(self):
        """Remove all expired entries from the cache."""
        now_utc = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM function_cache WHERE expires_at <= ?", (now_utc,))
            deleted = cur.rowcount
            conn.commit()
        logging.info(f"[EXPIRED CLEARED] {deleted} expired entries removed")

    def clear_expired_by_hash(self, hash_val, conn=None):
        now_utc = datetime.now(timezone.utc).isoformat()
        own_conn = False
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            own_conn = True
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM function_cache WHERE hash_val=? AND expires_at <= ?",
            (hash_val, now_utc)
        )
        deleted = cur.rowcount
        if own_conn:
            conn.commit()
            conn.close()
        logging.info(f"[EXPIRED HASH CLEARED] {deleted} entries removed for hash {hash_val}")
s