
# funkcache 😎

**funkcache** is a lightweight SQLite-based caching manager with TTL (time-to-live) support.  
It lets you cache results of function calls, persist them in a local SQLite database,  
and automatically expire them after a configurable duration.

---

## Installation
```bash
pip install funkcache
````

---

## Quick Example

```python
from funkcache import SQLiteCache

# Create cache manager (stored in functions_v2.db, default TTL = 10s)
cache_manager = SQLiteCache("functions_v2.db", ttl_seconds=10)

# Free function
@cache_manager.cache(ttl_seconds=5)  # override default TTL → 5s
def add(a, b):
    print("Executing add...")
    return a + b

# Class methods
class Calculator:
    @cache_manager.cache()  # uses default TTL = 10s
    def multiply(self, a, b):
        print("Executing Calculator.multiply...")
        return a * b

class AdvancedCalculator:
    @cache_manager.cache()
    def multiply(self, a, b):
        print("Executing AdvancedCalculator.multiply...")
        return a * b + 1

# Free function calls
print(add(2, 3))   # MISS → executes, caches result
print(add(2, 3))   # HIT  → returns cached result

# Class methods (different cache keys!)
c1 = Calculator()
print(c1.multiply(2, 4))  # MISS
print(c1.multiply(2, 4))  # HIT

c2 = AdvancedCalculator()
print(c2.multiply(2, 4))  # MISS (different class → different key)
```

---

## Cache Management

```python
# Remove all expired entries
cache_manager.clear_expired_cache()

# Remove all cache entries
cache_manager.clear_cache()

# Remove cache entries for a specific function
cache_manager.clear_cache("add")
```

---

## Features

* ✅ SQLite backend (no external dependencies)
* ✅ TTL-based caching (per-function or default)
* ✅ Works with free functions and class methods
* ✅ Logging support for cache hits/misses/clears
* ✅ Explicit cache clearing (`clear_cache`, `clear_expired_cache`)

---


