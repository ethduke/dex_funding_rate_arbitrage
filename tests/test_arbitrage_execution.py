import asyncio
import os
import unittest


os.environ.setdefault("BACKPACK_API_SECRET", "test-backpack-secret")
os.environ.setdefault("BACKPACK_API_KEY", "test-api-key")
os.environ.setdefault("HYPERLIQUID_API_PRIVATE_KEY", "0x" + "1" * 64)
os.environ.setdefault("HYPERLIQUID_ADDRESS", "test-hyperliquid-address")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "test-lighter-private-key")

from model.core.arbitrage_engine import FundingArbitrageEngine


class FakeSyncExchange:
    def __init__(self):
        self.calls = []

    def open_long(self, asset, amount):
        self.calls.append(("open_long", asset, amount))
        return {"status": "ok", "success": True}

    def open_short(self, asset, amount):
        self.calls.append(("open_short", asset, amount))
        return {"status": "ok", "success": True}

    def close_position(self, asset):
        self.calls.append(("close_position", asset))
        return {"status": "ok", "success": True}


class FakeAsyncExchange:
    def __init__(self):
        self.calls = []

    async def open_long(self, asset, amount):
        self.calls.append(("open_long", asset, amount))
        return {"status": "ok", "success": True}

    async def open_short(self, asset, amount):
        self.calls.append(("open_short", asset, amount))
        return {"status": "ok", "success": True}

    async def close_position(self, asset):
        self.calls.append(("close_position", asset))
        return {"status": "ok", "success": True}


class FakeFailingAsyncExchange(FakeAsyncExchange):
    async def open_short(self, asset, amount):
        self.calls.append(("open_short", asset, amount))
        return {"status": "error", "error": "rejected"}


class ArbitrageExecutionTests(unittest.TestCase):
    def test_tradexyz_pair_routes_sync_and_async_order_calls(self):
        engine = FundingArbitrageEngine.__new__(FundingArbitrageEngine)
        tradexyz = FakeSyncExchange()
        lighter = FakeAsyncExchange()
        engine.exchanges = {
            "TradeXYZ": tradexyz,
            "Lighter": lighter,
        }

        result, _, _, long_success, short_success = asyncio.run(
            engine._execute_tradexyz_pair(
                asset="TSLA",
                long_exchange="TradeXYZ",
                short_exchange="Lighter",
                position_size_usd=5,
            )
        )

        self.assertTrue(long_success)
        self.assertTrue(short_success)
        self.assertEqual(result["long_exchange"], "TradeXYZ")
        self.assertEqual(result["short_exchange"], "Lighter")
        self.assertEqual(tradexyz.calls, [("open_long", "TSLA", 5)])
        self.assertEqual(lighter.calls, [("open_short", "TSLA", 5)])

    def test_tradexyz_pair_rolls_back_long_when_short_fails(self):
        engine = FundingArbitrageEngine.__new__(FundingArbitrageEngine)
        tradexyz = FakeSyncExchange()
        lighter = FakeFailingAsyncExchange()
        engine.exchanges = {
            "TradeXYZ": tradexyz,
            "Lighter": lighter,
        }

        result, _, _, long_success, short_success = asyncio.run(
            engine._execute_tradexyz_pair(
                asset="TSLA",
                long_exchange="TradeXYZ",
                short_exchange="Lighter",
                position_size_usd=5,
            )
        )

        self.assertIsNone(result)
        self.assertTrue(long_success)
        self.assertFalse(short_success)
        self.assertEqual(
            tradexyz.calls,
            [("open_long", "TSLA", 5), ("close_position", "TSLA")],
        )
        self.assertEqual(lighter.calls, [("open_short", "TSLA", 5)])


if __name__ == "__main__":
    unittest.main()
