import json
import pickle
import hashlib
import sqlite3
import logging
import threading
from functools import wraps
from datetime import datetime, timedelta, timezone


class SQLiteCache:
    def __init__(self, db_path="funkcache_cache_manager.db", ttl_seconds=60,
                 use_json=False, debug=False):
        """
        Initialize SQLite cache manager.

        Args:
            db_path (str): Path to SQLite DB file.
            ttl_seconds (int): Default expiration time (in seconds).
            use_json (bool): If True, use JSON for serialization, otherwise pickle.
            debug (bool): Enable logging if True.
        """
        self.db_path = db_path
        self.default_ttl = ttl_seconds
        self.use_json = use_json
        self.debug = debug

        # Configure logging
        log_level = logging.DEBUG if debug else logging.CRITICAL
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )

        # Thread safety
        self._lock = threading.Lock()
        self.conn_args = {"check_same_thread": False}

        # Init DB
        with sqlite3.connect(self.db_path, **self.conn_args) as conn:
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS function_cache (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        func_name TEXT NOT NULL,
                        key TEXT NOT NULL UNIQUE,
                        hash_val TEXT NOT NULL,
                        args TEXT,
                        kwargs TEXT,
                        result BLOB,
                        expires_at TEXT
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cache_lookup
                    ON function_cache (func_name, hash_val, expires_at)
                """)
                conn.commit()
            except Exception as e:
                logging.error(f"DB Init failed: {e}")

    # ---------------- Serialization ----------------
    def _serialize(self, obj):
        try:
            if self.use_json:
                return json.dumps(obj, default=str).encode("utf-8")
            return pickle.dumps(obj)
        except Exception as e:
            logging.error(f"Serialization failed: {e}")
            raise

    def _deserialize(self, data):
        try:
            if self.use_json:
                return json.loads(data.decode("utf-8"))
            return pickle.loads(data)
        except Exception as e:
            logging.error(f"Deserialization failed: {e}")
            raise

    # ---------------- Key/Hash ----------------
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

    # ---------------- Decorator ----------------
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

                with self._lock, sqlite3.connect(self.db_path, **self.conn_args) as conn:
                    try:
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
                            logging.debug(f"[CACHE HIT] {func_name}({call_args}, {kwargs})")
                            return self._deserialize(row["result"])

                        # Compute result
                        logging.debug(f"[CACHE MISS] {func_name}({call_args}, {kwargs})")
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
                                self._serialize(result),
                                expires_at.isoformat(),
                            )
                        )
                        conn.commit()

                        return result
                    except Exception as e:
                        logging.error(f"Cache operation failed: {e}")
                        return func(*args, **kwargs)  # fallback
            return wrapper
        return decorator

    # ---------------- Cache Management ----------------
    def clear_cache(self, func_name=None):
        with self._lock, sqlite3.connect(self.db_path, **self.conn_args) as conn:
            try:
                cur = conn.cursor()
                if func_name:
                    cur.execute("DELETE FROM function_cache WHERE func_name=?", (func_name,))
                else:
                    cur.execute("DELETE FROM function_cache")
                deleted = cur.rowcount
                conn.commit()
                logging.debug(f"[CACHE CLEARED] {deleted} entries removed")
            except Exception as e:
                logging.error(f"Clear cache failed: {e}")

    def clear_expired_cache(self):
        now_utc = datetime.now(timezone.utc).isoformat()
        with self._lock, sqlite3.connect(self.db_path, **self.conn_args) as conn:
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM function_cache WHERE expires_at <= ?", (now_utc,))
                deleted = cur.rowcount
                conn.commit()
                logging.debug(f"[EXPIRED CLEARED] {deleted} expired entries removed")
            except Exception as e:
                logging.error(f"Clear expired cache failed: {e}")

    def clear_expired_by_hash(self, hash_val, conn=None):
        now_utc = datetime.now(timezone.utc).isoformat()
        own_conn = False
        try:
            if conn is None:
                conn = sqlite3.connect(self.db_path, **self.conn_args)
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
            logging.debug(f"[EXPIRED HASH CLEARED] {deleted} entries removed for hash {hash_val}")
        except Exception as e:
            logging.error(f"Clear expired by hash failed: {e}")
