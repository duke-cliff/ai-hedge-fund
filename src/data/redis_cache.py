import redis
import json
from typing import Any, Optional

class RedisCache:
    """Redis cache for API responses."""

    def __init__(self, redis_host='10.0.0.50', redis_port=56379, redis_db=0, ttl_seconds=86400):
        self.client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        self.ttl = ttl_seconds  # Optional: time-to-live for each key in seconds

    # def _merge_data(self, existing: list[dict] | None, new_data: list[dict], key_field: str) -> list[dict]:
    #     """Merge existing and new data, avoiding duplicates based on a key field."""
    #     if not existing:
    #         return new_data

    #     # Create a set of existing keys for O(1) lookup
    #     existing_keys = {item[key_field] for item in existing}

    #     # Only add items that don't exist yet
    #     merged = existing.copy()
    #     merged.extend([item for item in new_data if item[key_field] not in existing_keys])
    #     return merged

    def _key(self, category: str, ticker: str) -> str:
        return f"{category}:{ticker.upper()}"
    
    def _merge_data(self, existing: Optional[list[dict]], new_data: list[dict], key_field: str) -> list[dict]:
        if not existing:
            return new_data
        existing_keys = {item[key_field] for item in existing}
        merged = existing.copy()
        merged.extend([item for item in new_data if item[key_field] not in existing_keys])
        return merged
    
    def _get(self, category: str, ticker: str) -> Optional[list[dict[str, Any]]]:
        data = self.client.get(self._key(category, ticker))
        return json.loads(data) if data else None
    
    def _set(self, category: str, ticker: str, data: list[dict[str, Any]], ttl=None):
        if ttl is None:
            ttl = self.ttl
        self.client.set(self._key(category, ticker), json.dumps(data), ex=ttl)

    def get_prices(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached price data if available."""
        return self._get("prices", ticker)

    def set_prices(self, ticker: str, data: list[dict[str, Any]]):
        existing = self.get_prices(ticker)
        merged = self._merge_data(existing, data, key_field="time")
        self._set("prices", ticker, merged)

    # Financial metrics
    def get_financial_metrics(self, ticker: str) -> Optional[list[dict[str, Any]]]:
        return self._get("financial_metrics", ticker)

    def set_financial_metrics(self, ticker: str, data: list[dict[str, Any]]):
        existing = self.get_financial_metrics(ticker)
        merged = self._merge_data(existing, data, key_field="report_period")
        self._set("financial_metrics", ticker, merged)

    # Line items
    def get_line_items(self, ticker: str) -> Optional[list[dict[str, Any]]]:
        return self._get("line_items", ticker)

    def set_line_items(self, ticker: str, data: list[dict[str, Any]]):
        existing = self.get_line_items(ticker)
        merged = self._merge_data(existing, data, key_field="report_period")
        self._set("line_items", ticker, merged, ttl=2592000) #30days

    # Insider trades
    def get_insider_trades(self, ticker: str) -> Optional[list[dict[str, Any]]]:
        return self._get("insider_trades", ticker)

    def set_insider_trades(self, ticker: str, data: list[dict[str, Any]]):
        existing = self.get_insider_trades(ticker)
        merged = self._merge_data(existing, data, key_field="filing_date")
        self._set("insider_trades", ticker, merged, ttl=2592000) #30days

    # Company news
    def get_company_news(self, ticker: str) -> Optional[list[dict[str, Any]]]:
        return self._get("company_news", ticker)

    def set_company_news(self, ticker: str, data: list[dict[str, Any]]):
        existing = self.get_company_news(ticker)
        merged = self._merge_data(existing, data, key_field="date")
        self._set("company_news", ticker, merged)


# Global cache instance
_cache = RedisCache()


def get_cache() -> RedisCache:
    """Get the global cache instance."""
    return _cache
