import os
import unittest
from unittest.mock import patch
from types import SimpleNamespace


os.environ.setdefault("BACKPACK_API_SECRET", "test-backpack-secret")
os.environ.setdefault("BACKPACK_API_KEY", "test-api-key")
os.environ.setdefault("HYPERLIQUID_API_PRIVATE_KEY", "test-hyperliquid-private-key")
os.environ.setdefault("HYPERLIQUID_ADDRESS", "test-hyperliquid-address")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "test-lighter-private-key")

from model.exchanges.backpack import BackpackExchange
from model.exchanges.base import BaseExchange
from model.exchanges.hyperliquid import HyperliquidExchange
from model.exchanges.lighter import LighterExchange
from model.exchanges.normalized import BalanceSnapshot, FundingRate, OrderResult, Position


class DummyExchange(BaseExchange):
    def get_funding_rates(self):
        return {}

    def process_funding_rates(self, raw_data):
        return {}

    def get_positions(self):
        return []

    def place_market_order(self, symbol, side, quantity=None, quote_quantity=None, reduce_only=False):
        return {}

    def close_position(self, symbol):
        return {}

    def open_long(self, asset, amount):
        return {}

    def open_short(self, asset, amount):
        return {}

    def format_symbol(self, asset):
        return asset

    def subscribe_to_funding_updates(self, callback):
        return None

    def get_available_usd(self, asset=None):
        return 12.5


class NormalizedModelTests(unittest.TestCase):
    def test_dataclasses_omit_empty_optional_fields(self):
        self.assertEqual(
            FundingRate(asset="BTC", exchange="X", rate=0.1).to_dict(),
            {"asset": "BTC", "exchange": "X", "rate": 0.1},
        )
        self.assertEqual(
            BalanceSnapshot(exchange="X", available_usd=5).to_dict(),
            {"exchange": "X", "available_usd": 5},
        )
        self.assertEqual(
            Position(asset="ETH", exchange="X", size=-2).to_dict(),
            {"asset": "ETH", "exchange": "X", "symbol": "ETH", "size": -2},
        )
        self.assertEqual(
            OrderResult(exchange="X", success=True, asset="SOL").to_dict()["status"],
            "ok",
        )

    def test_base_exchange_balance_snapshot_uses_available_usd(self):
        snapshot = DummyExchange().get_balance_snapshot()

        self.assertEqual(snapshot.exchange, "Dummy")
        self.assertEqual(snapshot.available_usd, 12.5)


class ExchangeNormalizationTests(unittest.TestCase):
    def test_backpack_funding_and_position_normalization(self):
        exchange = BackpackExchange(use_ws=False)

        rates = exchange.process_funding_rates([
            {
                "symbol": "BTC_USDC_PERP",
                "fundingRate": "0.0001",
                "nextFundingTimestamp": 123,
                "markPrice": "100000",
                "indexPrice": "99990",
            }
        ])
        position = exchange._normalize_position({
            "symbol": "ETH_USDC_PERP",
            "netQuantity": "-0.5",
            "avgEntryPrice": "3000",
        })
        order = exchange._normalize_order_result(
            {"id": "abc", "avgFillPrice": "42"},
            symbol="SOL_USDC_PERP",
            side="Bid",
            size=10,
        )

        self.assertEqual(rates["BTC"]["asset"], "BTC")
        self.assertEqual(rates["BTC"]["exchange"], "Backpack")
        self.assertEqual(rates["BTC"]["rate"], 0.0001)
        self.assertEqual(position["asset"], "ETH")
        self.assertEqual(position["side"], "short")
        self.assertTrue(order["success"])
        self.assertEqual(order["order_id"], "abc")

    def test_hyperliquid_funding_position_and_order_normalization(self):
        exchange = HyperliquidExchange.__new__(HyperliquidExchange)

        rates = exchange.process_funding_rates([
            [
                "BTC",
                [
                    ["BinPerp", {"fundingRate": "0.1"}],
                    [
                        "HlPerp",
                        {
                            "fundingRate": "-0.0002",
                            "nextFundingTime": 456,
                            "fundingIntervalHours": 1,
                        },
                    ],
                ],
            ]
        ])
        position = exchange._normalize_position({
            "position": {
                "coin": "ETH",
                "szi": "0.25",
                "entryPx": "3000",
                "unrealizedPnl": "1.5",
            }
        })
        order = exchange._normalize_order_result(
            {"status": "ok"},
            asset="ETH",
            side="BUY",
            size=0.25,
        )

        self.assertEqual(rates["BTC"]["exchange"], "Hyperliquid")
        self.assertEqual(rates["BTC"]["funding_interval_hours"], 1.0)
        self.assertEqual(position["coin"], "ETH")
        self.assertEqual(position["side"], "long")
        self.assertEqual(position["entry_price"], 3000.0)
        self.assertTrue(order["success"])

    def test_hyperliquid_order_normalization_detects_nested_rejection(self):
        exchange = HyperliquidExchange.__new__(HyperliquidExchange)

        order = exchange._normalize_order_result(
            {
                "status": "ok",
                "response": {
                    "type": "order",
                    "data": {
                        "statuses": [
                            {"error": "Insufficient margin to place order"},
                        ],
                    },
                },
            },
            asset="xyz:DRAM",
            side="SELL",
            size=0.25,
        )

        self.assertFalse(order["success"])
        self.assertEqual(order["status"], "error")
        self.assertEqual(order["error"], "Insufficient margin to place order")

    def test_hyperliquid_order_normalization_accepts_nested_fills(self):
        exchange = HyperliquidExchange.__new__(HyperliquidExchange)

        order = exchange._normalize_order_result(
            {
                "status": "ok",
                "response": {
                    "type": "order",
                    "data": {
                        "statuses": [
                            {"filled": {"totalSz": "0.25", "avgPx": "80.0"}},
                        ],
                    },
                },
            },
            asset="xyz:DRAM",
            side="SELL",
            size=0.25,
        )

        self.assertTrue(order["success"])

    def test_lighter_funding_and_trade_normalization(self):
        exchange = LighterExchange.__new__(LighterExchange)
        exchange.account_index = 7

        rates = exchange.process_funding_rates({
            "SOL": {
                "rate": "0.0003",
                "next_funding_time": 789,
                "mark_price": "150",
                "index_price": "151",
            }
        })
        trade = exchange.normalize_trade({
            "trade_id": 1,
            "market_id": 2,
            "size": "3",
            "price": "10",
            "ask_account_id": 7,
            "bid_account_id": 8,
            "is_maker_ask": True,
            "maker_fee": "0.01",
        })

        self.assertEqual(rates["SOL"]["exchange"], "Lighter")
        self.assertEqual(rates["SOL"]["mark_price"], 150.0)
        self.assertEqual(trade["side"], "SELL")
        self.assertEqual(trade["role"], "maker")
        self.assertEqual(trade["fee"], "0.01")

    def test_lighter_funding_rates_ignore_external_venue_rows(self):
        exchange = LighterExchange.__new__(LighterExchange)
        exchange.api_client = object()

        async def fake_symbol_by_market_id(market_id):
            return "AXS"

        exchange._get_symbol_by_market_id = fake_symbol_by_market_id

        class FakeFundingApi:
            def __init__(self, api_client):
                self.api_client = api_client

            async def funding_rates(self):
                return SimpleNamespace(funding_rates=[
                    SimpleNamespace(
                        market_id=131,
                        exchange="binance",
                        symbol="AXS",
                        rate=-0.0054767,
                    ),
                    SimpleNamespace(
                        market_id=131,
                        exchange="lighter",
                        symbol="AXS",
                        rate=-0.001696,
                    ),
                ])

        import asyncio

        with patch("model.exchanges.lighter.lighter.FundingApi", FakeFundingApi):
            raw_rates = asyncio.run(exchange.get_funding_rates())

        self.assertEqual(raw_rates["AXS"]["exchange"], "lighter")
        self.assertEqual(raw_rates["AXS"]["rate"], -0.001696)


if __name__ == "__main__":
    unittest.main()
