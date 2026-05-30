import os
import unittest


os.environ.setdefault("BACKPACK_API_SECRET", "test-backpack-secret")
os.environ.setdefault("BACKPACK_API_KEY", "test-api-key")
os.environ.setdefault("HYPERLIQUID_API_PRIVATE_KEY", "test-hyperliquid-private-key")
os.environ.setdefault("HYPERLIQUID_ADDRESS", "test-hyperliquid-address")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "test-lighter-private-key")

from model.core.arbitrage_engine import FundingArbitrageEngine


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


if __name__ == "__main__":
    unittest.main()
