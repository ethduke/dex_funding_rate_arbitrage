import os
import time
import types
import unittest


os.environ.setdefault("BACKPACK_API_SECRET", "test-backpack-secret")
os.environ.setdefault("BACKPACK_API_KEY", "test-api-key")
os.environ.setdefault("HYPERLIQUID_API_PRIVATE_KEY", "test-hyperliquid-private-key")
os.environ.setdefault("HYPERLIQUID_ADDRESS", "test-hyperliquid-address")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "test-lighter-private-key")

from model.exchanges.hyperliquid import HyperliquidExchange
from model.exchanges.lighter import LighterExchange


class FakeHyperliquidInfo:
    def __init__(self):
        self.meta_calls = 0
        self.all_mids_calls = 0

    def meta(self):
        self.meta_calls += 1
        return {"universe": [{"name": "BTC", "szDecimals": 5}]}

    def all_mids(self):
        self.all_mids_calls += 1
        return {"BTC": "100000"}


class MarketMetadataCacheTests(unittest.TestCase):
    def test_hyperliquid_reuses_cached_meta_and_prices(self):
        exchange = HyperliquidExchange.__new__(HyperliquidExchange)
        exchange.info = FakeHyperliquidInfo()
        exchange._meta_cache = None
        exchange._meta_cache_ts = 0.0
        exchange._all_mids_cache = None
        exchange._all_mids_cache_ts = 0.0

        self.assertEqual(exchange.get_sz_decimals("BTC"), 5)
        self.assertEqual(exchange.get_sz_decimals("BTC"), 5)
        self.assertEqual(exchange.info.meta_calls, 1)

        self.assertEqual(exchange.get_price_from_api("BTC", 200000), 2.0)
        self.assertEqual(exchange.get_price_from_api("BTC", 100000), 1.0)
        self.assertEqual(exchange.info.all_mids_calls, 1)

    def test_hyperliquid_refresh_clears_cached_metadata(self):
        exchange = HyperliquidExchange.__new__(HyperliquidExchange)
        exchange.info = FakeHyperliquidInfo()
        exchange._meta_cache = {"universe": []}
        exchange._meta_cache_ts = time.time()
        exchange._all_mids_cache = {"BTC": "1"}
        exchange._all_mids_cache_ts = time.time()

        exchange.refresh_market_metadata()

        self.assertIsNone(exchange._meta_cache)
        self.assertEqual(exchange._meta_cache_ts, 0.0)
        self.assertIsNone(exchange._all_mids_cache)
        self.assertEqual(exchange._all_mids_cache_ts, 0.0)

    def test_lighter_refreshes_expired_market_mapping(self):
        exchange = LighterExchange.__new__(LighterExchange)
        exchange._market_mapping_cache = {1: "OLD"}
        exchange._market_mapping_cache_ts = 1.0
        exchange._market_decimals_cache = {}
        exchange._market_decimals_cache_ts = {}
        calls = {"count": 0}

        async def fake_fetch_market_info(self):
            calls["count"] += 1
            self._market_mapping_cache = {2: "BTC"}
            self._market_mapping_cache_ts = time.time()
            return self._market_mapping_cache

        exchange._fetch_market_info = types.MethodType(fake_fetch_market_info, exchange)

        import asyncio

        mapping = asyncio.run(exchange.refresh_market_mapping())

        self.assertEqual(mapping, {2: "BTC"})
        self.assertEqual(calls["count"], 1)

    def test_lighter_reuses_fresh_market_decimals(self):
        exchange = LighterExchange.__new__(LighterExchange)
        exchange._market_decimals_cache = {
            1: {"sizeDecimal": 6, "priceDecimal": 2}
        }
        exchange._market_decimals_cache_ts = {1: time.time()}
        exchange._market_mapping_cache = {1: "BTC"}
        exchange._market_mapping_cache_ts = time.time()

        import asyncio

        decimals = asyncio.run(exchange._get_market_decimals(1))

        self.assertEqual(decimals, {"sizeDecimal": 6, "priceDecimal": 2})


if __name__ == "__main__":
    unittest.main()
