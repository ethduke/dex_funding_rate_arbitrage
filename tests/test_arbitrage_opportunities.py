import os
import unittest
from unittest.mock import patch


os.environ.setdefault("BACKPACK_API_SECRET", "test-backpack-secret")
os.environ.setdefault("BACKPACK_API_KEY", "test-api-key")
os.environ.setdefault("HYPERLIQUID_API_PRIVATE_KEY", "test-hyperliquid-private-key")
os.environ.setdefault("HYPERLIQUID_ADDRESS", "test-hyperliquid-address")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "test-lighter-private-key")

from model.core.arbitrage_engine import FundingArbitrageEngine, resolve_exchange_classes
from model.core import arbitrage_engine as arbitrage_engine_module


class ArbitrageOpportunityTests(unittest.TestCase):
    def setUp(self):
        self.engine = FundingArbitrageEngine.__new__(FundingArbitrageEngine)

    def test_uses_rate_spread_for_same_sign_positive_rates(self):
        opportunities = self.engine.find_arbitrage_opportunities(
            {
                "A": {"BTC": {"rate": 0.0002}},
                "B": {"BTC": {"rate": 0.0001}},
            },
            min_diff=0.00005,
        )

        self.assertEqual(len(opportunities), 1)
        self.assertAlmostEqual(opportunities[0]["potential_profit"], 0.0001)
        self.assertEqual(opportunities[0]["long_exchange"], "B")
        self.assertEqual(opportunities[0]["short_exchange"], "A")

    def test_uses_rate_spread_for_same_sign_negative_rates(self):
        opportunities = self.engine.find_arbitrage_opportunities(
            {
                "A": {"ETH": {"rate": -0.0002}},
                "B": {"ETH": {"rate": -0.0001}},
            },
            min_diff=0.00005,
        )

        self.assertEqual(len(opportunities), 1)
        self.assertAlmostEqual(opportunities[0]["potential_profit"], 0.0001)
        self.assertEqual(opportunities[0]["long_exchange"], "A")
        self.assertEqual(opportunities[0]["short_exchange"], "B")

    def test_cross_sign_rates_use_full_spread(self):
        opportunities = self.engine.find_arbitrage_opportunities(
            {
                "A": {"SOL": {"rate": -0.0002}},
                "B": {"SOL": {"rate": 0.0001}},
            },
            min_diff=0.00005,
        )

        self.assertEqual(len(opportunities), 1)
        self.assertAlmostEqual(opportunities[0]["potential_profit"], 0.0003)
        self.assertEqual(opportunities[0]["long_exchange"], "A")
        self.assertEqual(opportunities[0]["short_exchange"], "B")

    def test_filters_spreads_below_threshold(self):
        opportunities = self.engine.find_arbitrage_opportunities(
            {
                "A": {"BTC": {"rate": 0.0002}},
                "B": {"BTC": {"rate": 0.00019}},
            },
            min_diff=0.00005,
        )

        self.assertEqual(opportunities, [])

    def test_uses_configured_comparison_pairs_only(self):
        self.engine.comparison_pairs = [("Lighter", "TradeXYZ")]

        opportunities = self.engine.find_arbitrage_opportunities(
            {
                "Lighter": {
                    "TSLA": {"rate": 0.0003},
                    "BTC": {"rate": 0.0003},
                },
                "TradeXYZ": {
                    "TSLA": {"rate": 0.0001},
                },
                "Hyperliquid": {
                    "BTC": {"rate": 0.0},
                    "TSLA": {"rate": 0.0},
                },
            },
            min_diff=0.00005,
        )

        self.assertEqual(len(opportunities), 1)
        self.assertEqual(opportunities[0]["asset"], "TSLA")
        self.assertEqual(opportunities[0]["exchanges"], ["Lighter", "TradeXYZ"])

    def test_configured_pairs_skip_disabled_exchanges(self):
        self.engine.comparison_pairs = [("Lighter", "TradeXYZ"), ("Lighter", "Hyperliquid")]

        opportunities = self.engine.find_arbitrage_opportunities(
            {
                "Lighter": {"TSLA": {"rate": 0.0003}},
                "TradeXYZ": {"TSLA": {"rate": 0.0001}},
            },
            min_diff=0.00005,
        )

        self.assertEqual(len(opportunities), 1)
        self.assertEqual(opportunities[0]["exchanges"], ["Lighter", "TradeXYZ"])

    def test_resolves_configured_exchange_classes(self):
        classes = resolve_exchange_classes(["Lighter", "TradeXYZ"])

        self.assertEqual(
            [exchange_class.__name__ for exchange_class in classes],
            ["LighterExchange", "TradeXYZExchange"],
        )

    def test_rejects_unknown_exchange_names(self):
        with self.assertRaises(ValueError):
            resolve_exchange_classes(["Lighter", "Nope"])

    def test_default_exchange_set_includes_tradexyz(self):
        class BackpackExchange:
            def __init__(self, use_ws=True):
                self.use_ws = use_ws

        class HyperliquidExchange:
            pass

        class LighterExchange:
            def __init__(self, use_ws=True):
                self.use_ws = use_ws

        class TradeXYZExchange:
            pass

        exchange_classes = {
            "Backpack": BackpackExchange,
            "Hyperliquid": HyperliquidExchange,
            "Lighter": LighterExchange,
            "TradeXYZ": TradeXYZExchange,
        }
        with patch.object(arbitrage_engine_module, "EXCHANGE_CLASSES", exchange_classes):
            engine = FundingArbitrageEngine(
                min_rate_difference=0.0001,
                position_size=5,
                min_hold_time_seconds=0,
                magnitude_reduction_threshold=0.5,
                check_interval_minutes=1,
                use_ws=False,
            )

        self.assertEqual(
            set(engine.exchanges),
            {"Backpack", "Hyperliquid", "Lighter", "TradeXYZ"},
        )


if __name__ == "__main__":
    unittest.main()
