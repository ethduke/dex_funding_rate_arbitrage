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
    def __init__(self, available_usd=10):
        self.calls = []
        self.available_usd = available_usd

    def open_long(self, asset, amount):
        self.calls.append(("open_long", asset, amount))
        return {"status": "ok", "success": True}

    def open_short(self, asset, amount):
        self.calls.append(("open_short", asset, amount))
        return {"status": "ok", "success": True}

    def close_position(self, asset):
        self.calls.append(("close_position", asset))
        return {"status": "ok", "success": True}

    def get_market_data(self, asset):
        self.calls.append(("get_market_data", asset))
        return {"price": 123.45}

    def get_available_usd(self, asset=None):
        self.calls.append(("get_available_usd", asset))
        return self.available_usd


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


class FakeLighterExecutionExchange(FakeAsyncExchange):
    signer_client = True

    async def _ensure_account_initialized(self):
        self.calls.append(("_ensure_account_initialized",))

    async def get_real_balance(self):
        self.calls.append(("get_real_balance",))
        return {"free_collateral": "10", "total_asset_value": "10"}


class FakeFailingAsyncExchange(FakeAsyncExchange):
    async def open_short(self, asset, amount):
        self.calls.append(("open_short", asset, amount))
        return {"status": "error", "error": "rejected"}


class FakeFundingExchange:
    def __init__(self):
        self.calls = []

    def get_funding_rates(self):
        self.calls.append(("get_funding_rates",))
        return {"raw": True}

    def process_funding_rates(self, raw_data):
        self.calls.append(("process_funding_rates", raw_data))
        return {"TSLA": {"rate": 0.0002}}


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

    def test_cleanup_positions_closes_tradexyz_successful_leg(self):
        engine = FundingArbitrageEngine.__new__(FundingArbitrageEngine)
        tradexyz = FakeSyncExchange()
        engine.exchanges = {"TradeXYZ": tradexyz}

        engine._cleanup_positions(
            asset="TSLA",
            long_exchange="TradeXYZ",
            short_exchange="Lighter",
            long_success=True,
            short_success=False,
        )

        self.assertEqual(tradexyz.calls, [("close_position", "TSLA")])

    def test_close_exchange_position_supports_tradexyz(self):
        engine = FundingArbitrageEngine.__new__(FundingArbitrageEngine)
        tradexyz = FakeSyncExchange()
        stats = {"exit_prices": {"TradeXYZ": None}}

        result = asyncio.run(
            engine._close_exchange_position(
                exchange_name="TradeXYZ",
                exchange_obj=tradexyz,
                asset_name="TSLA",
                stats_dict=stats,
            )
        )

        self.assertTrue(result)
        self.assertEqual(
            tradexyz.calls,
            [("close_position", "TSLA"), ("get_market_data", "TSLA")],
        )
        self.assertEqual(stats["exit_prices"]["TradeXYZ"], 123.45)

    def test_poll_funding_rates_emits_tradexyz_updates(self):
        async def run_test():
            engine = FundingArbitrageEngine.__new__(FundingArbitrageEngine)
            tradexyz = FakeFundingExchange()
            queue = asyncio.Queue()
            task = asyncio.create_task(
                engine._poll_funding_rates(
                    backpack=None,
                    hyperliquid=None,
                    lighter=None,
                    asset="TSLA",
                    funding_rate_queue=queue,
                    tradexyz=tradexyz,
                )
            )

            try:
                exchange, rate, _ = await asyncio.wait_for(queue.get(), timeout=1)
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            return tradexyz, exchange, rate

        tradexyz, exchange, rate = asyncio.run(run_test())

        self.assertEqual(exchange, "TradeXYZ")
        self.assertEqual(rate, 0.0002)
        self.assertEqual(
            tradexyz.calls,
            [("get_funding_rates",), ("process_funding_rates", {"raw": True})],
        )

    def test_validate_exchange_balances_checks_tradexyz_available_usd(self):
        engine = FundingArbitrageEngine.__new__(FundingArbitrageEngine)
        tradexyz = FakeSyncExchange(available_usd=6)
        engine.exchanges = {"TradeXYZ": tradexyz}
        engine.position_size = 5

        balances = asyncio.run(engine._validate_exchange_balances())

        self.assertEqual(balances, {"TradeXYZ": True})
        self.assertEqual(tradexyz.calls, [("get_available_usd", None)])

    def test_execute_tradexyz_pair_requires_tradexyz_balance(self):
        engine = FundingArbitrageEngine.__new__(FundingArbitrageEngine)
        engine.position_size = 5
        tradexyz = FakeSyncExchange(available_usd=10)
        lighter = FakeLighterExecutionExchange()
        engine.exchanges = {
            "TradeXYZ": tradexyz,
            "Lighter": lighter,
        }

        result = asyncio.run(
            engine._execute_arbitrage({
                "asset": "TSLA",
                "long_exchange": "TradeXYZ",
                "short_exchange": "Lighter",
            })
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["long_exchange"], "TradeXYZ")
        self.assertEqual(result["short_exchange"], "Lighter")
        self.assertIn(("get_available_usd", "TSLA"), tradexyz.calls)


if __name__ == "__main__":
    unittest.main()
