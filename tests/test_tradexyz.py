import unittest

from model.exchanges.tradexyz import TradeXYZExchange


class TradeXYZExchangeTests(unittest.TestCase):
    def test_processes_meta_and_asset_contexts_as_funding_rates(self):
        exchange = TradeXYZExchange.__new__(TradeXYZExchange)

        result = exchange.process_funding_rates([
            {
                "universe": [
                    {"name": "xyz:XYZ100", "szDecimals": 4},
                    {"name": "xyz:TSLA", "szDecimals": 3},
                ]
            },
            [
                {
                    "funding": "-0.0000281351",
                    "markPx": "30283.0",
                    "oraclePx": "30308.0",
                },
                {
                    "funding": "0.0000125",
                    "markPx": "399.18",
                    "oraclePx": "399.64",
                },
            ],
        ])

        self.assertEqual(set(result), {"xyz:XYZ100", "xyz:TSLA"})
        self.assertEqual(result["xyz:XYZ100"]["exchange"], "TradeXYZ")
        self.assertAlmostEqual(result["xyz:XYZ100"]["rate"], -0.0000281351)
        self.assertEqual(result["xyz:XYZ100"]["mark_price"], 30283.0)
        self.assertEqual(result["xyz:TSLA"]["index_price"], 399.64)

    def test_ignores_error_payloads(self):
        exchange = TradeXYZExchange.__new__(TradeXYZExchange)

        self.assertEqual(exchange.process_funding_rates({"error": "boom"}), {})


if __name__ == "__main__":
    unittest.main()
