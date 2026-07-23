import os
import unittest


os.environ.setdefault("BACKPACK_API_SECRET", "test-backpack-secret")
os.environ.setdefault("BACKPACK_API_KEY", "test-api-key")
os.environ.setdefault("HYPERLIQUID_API_PRIVATE_KEY", "test-hyperliquid-private-key")
os.environ.setdefault("HYPERLIQUID_ADDRESS", "test-hyperliquid-address")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "test-lighter-private-key")

from model.core.arbitrage_engine import FundingArbitrageEngine
from model.exchanges.lighter import LighterExchange


class FundingResponse:
    def __init__(self, payments, next_cursor=None):
        self.position_fundings = payments
        self.next_cursor = next_cursor


class ModernAccountApi:
    def __init__(self):
        self.calls = []

    async def position_funding(
        self,
        account_index,
        limit,
        authorization,
        market_ids=None,
        cursor=None,
        start_timestamp=None,
        end_timestamp=None,
    ):
        self.calls.append({
            "market_ids": market_ids,
            "cursor": cursor,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
        })
        if cursor is None:
            return FundingResponse([
                {
                    "timestamp": 200,
                    "market_id": 7,
                    "funding_id": 2,
                    "change": "-0.25",
                    "discount": "0",
                    "rate": "0.001",
                    "position_size": "10",
                    "position_side": "long",
                }
            ], next_cursor="page-2")
        return FundingResponse([
            {
                "timestamp": 100,
                "market_id": 8,
                "funding_id": 1,
                "change": "0.10",
                "discount": "0",
                "rate": "-0.002",
                "position_size": "5",
                "position_side": "short",
            }
        ])


class LegacyAccountApi:
    def __init__(self):
        self.market_ids = []

    async def position_funding(
        self,
        account_index,
        limit,
        authorization,
        market_id=None,
        cursor=None,
        start_timestamp=None,
        end_timestamp=None,
    ):
        self.market_ids.append(market_id)
        return FundingResponse([])


class FakeLighter:
    async def _get_market_id(self, asset):
        return 7

    async def get_position_funding_payments(
        self,
        market_ids,
        start_timestamp=None,
        end_timestamp=None,
    ):
        return [{"change": "0.15"}, {"change": "-0.04"}]


class LighterPositionFundingTests(unittest.IsolatedAsyncioTestCase):
    def _exchange(self, account_api):
        exchange = LighterExchange.__new__(LighterExchange)
        exchange.account_api = account_api
        exchange.account_index = 5725
        exchange._get_auth_token = lambda: _async_value("test-auth")
        return exchange

    async def test_uses_batched_market_ids_and_paginates(self):
        account_api = ModernAccountApi()
        exchange = self._exchange(account_api)

        payments = await exchange.get_position_funding_payments(
            market_ids=[7, 8],
            start_timestamp=10,
            end_timestamp=300,
        )

        self.assertEqual([call["market_ids"] for call in account_api.calls], ["7,8", "7,8"])
        self.assertEqual([call["cursor"] for call in account_api.calls], [None, "page-2"])
        self.assertEqual([payment["timestamp"] for payment in payments], [100, 200])
        self.assertEqual([payment["change"] for payment in payments], [0.10, -0.25])

    async def test_falls_back_to_one_request_per_market_for_current_sdk(self):
        account_api = LegacyAccountApi()
        exchange = self._exchange(account_api)

        payments = await exchange.get_position_funding_payments(market_ids=[7, 8, 7])

        self.assertEqual(payments, [])
        self.assertEqual(account_api.market_ids, [7, 8])

    async def test_engine_uses_signed_funding_changes(self):
        engine = FundingArbitrageEngine.__new__(FundingArbitrageEngine)
        stats = {"funding_payments": {"Lighter": 0.0}}

        refreshed = await engine._refresh_lighter_funding_payments(
            FakeLighter(),
            "TSLA",
            1000,
            stats,
        )

        self.assertTrue(refreshed)
        self.assertAlmostEqual(stats["funding_payments"]["Lighter"], 0.11)


async def _async_value(value):
    return value


if __name__ == "__main__":
    unittest.main()
