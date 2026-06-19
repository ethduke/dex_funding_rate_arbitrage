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

    def test_positions_use_xyz_user_state(self):
        class FakeInfo:
            def __init__(self):
                self.calls = []

            def user_state(self, address, dex=""):
                self.calls.append((address, dex))
                return {
                    "assetPositions": [
                        {"position": {"coin": "xyz:TSLA", "szi": "1.5", "entryPx": "400"}}
                    ]
                }

        exchange = TradeXYZExchange.__new__(TradeXYZExchange)
        exchange.info = FakeInfo()
        exchange.address = "0xabc"

        positions = exchange.get_positions()

        self.assertEqual(exchange.info.calls, [("0xabc", "xyz")])
        self.assertEqual(positions[0]["exchange"], "TradeXYZ")
        self.assertEqual(positions[0]["asset"], "xyz:TSLA")
        self.assertEqual(positions[0]["side"], "long")

    def test_balance_uses_xyz_user_state(self):
        class FakeInfo:
            def __init__(self):
                self.calls = []

            def user_state(self, address, dex=""):
                self.calls.append((address, dex))
                return {
                    "withdrawable": "12.5",
                    "crossMarginSummary": {"accountValue": "15.0"},
                    "assetPositions": [],
                }

        exchange = TradeXYZExchange.__new__(TradeXYZExchange)
        exchange.info = FakeInfo()
        exchange.address = "0xabc"

        balance = exchange.get_balance_snapshot()

        self.assertEqual(exchange.info.calls, [("0xabc", "xyz")])
        self.assertEqual(balance.exchange, "TradeXYZ")
        self.assertEqual(balance.available_usd, 12.5)
        self.assertEqual(balance.total_usd, 15.0)


if __name__ == "__main__":
    unittest.main()
