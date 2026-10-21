"""Replay public captures through the real SDK and bot; never contact an API."""

import asyncio
from decimal import Decimal
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("BACKPACK_API_SECRET", "test-backpack-secret")
os.environ.setdefault("BACKPACK_API_KEY", "test-api-key")
os.environ.setdefault("HYPERLIQUID_API_PRIVATE_KEY", "test-hyperliquid-private-key")

import lighter
from lighter.rest import RESTResponse
from model.core.arbitrage_engine import FundingArbitrageEngine
from model.exchanges.lighter import LighterExchange
from model.exchanges.lighter_rhc import LighterRHCExchange
from model.exchanges.lighter_funding import (
    finite_funding_rate, rest_funding_to_hourly, ws_funding_to_hourly,
)


CAPTURE = json.loads((Path(__file__).parent / "fixtures/lighter_funding_units.json").read_text())


class FundingConversionTests(unittest.TestCase):
    def test_source_specific_units_positive_negative_and_zero(self):
        for rest, ws, hourly in (("0.000096", "0.0012", 0.000012),
                                  ("-0.000072", "-0.0009", -0.000009),
                                  (0, "0.0000", 0.0)):
            with self.subTest(rest=rest):
                self.assertEqual(rest_funding_to_hourly(rest), hourly)
                self.assertEqual(ws_funding_to_hourly(ws), hourly)
                self.assertEqual(finite_funding_rate(hourly), hourly)

    def test_rejects_missing_boolean_nonfinite_and_malformed_rates(self):
        for value in (None, True, False, "", "invalid", {}, [], float("nan"),
                      float("inf"), "-Infinity", "NaN", "1e999999999"):
            for convert in (rest_funding_to_hourly, ws_funding_to_hourly, finite_funding_rate):
                with self.subTest(value=value, convert=convert.__name__):
                    with self.assertRaises(ValueError):
                        convert(value)

    def test_captured_rest_and_current_ws_agree_for_every_market(self):
        for venue, count in (("Lighter", 213), ("LighterRHC", 58)):
            markets = CAPTURE["venues"][venue]["markets"]
            self.assertEqual(len(markets), count)
            for market in markets:
                with self.subTest(venue=venue, symbol=market["websocket"]["symbol"]):
                    current = ws_funding_to_hourly(market["websocket"]["current_funding_rate"])
                    for source in ("rest_before", "rest_after"):
                        self.assertAlmostEqual(rest_funding_to_hourly(market[source]["rate"]),
                                               current, delta=1e-18)

    def test_hourly_rate_annualizes_without_eightfold_inflation(self):
        self.assertAlmostEqual(rest_funding_to_hourly(0.000096) * 8760 * 100, 10.512)
        self.assertAlmostEqual(0.000096 * 8760 * 100, 84.096)


class LighterFundingPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def replay_rest(self, venue):
        capture = CAPTURE["venues"][venue]
        cls = LighterExchange if venue == "Lighter" else LighterRHCExchange
        exchange = cls.__new__(cls)
        client = lighter.ApiClient(lighter.Configuration(host="https://example.invalid"))
        self.addAsyncCleanup(client.close)
        exchange.api_client = client
        symbols = {m["rest_before"]["market_id"]: m["websocket"]["symbol"] for m in capture["markets"]}
        exchange._get_symbol_by_market_id = AsyncMock(side_effect=lambda market_id: symbols[market_id])
        rows = [m["rest_before"] for m in capture["markets"]]
        # Same symbol from the cross-venue aggregator must never override Lighter.
        rows.append({"market_id": 1, "exchange": "binance", "symbol": "BTC", "rate": 0.99})
        response = RESTResponse(SimpleNamespace(
            status=200, reason="captured public funding response",
            headers={"content-type": "application/json"},
            read=AsyncMock(return_value=json.dumps({"code": 200, "funding_rates": rows}).encode()),
        ))
        with patch.object(client.rest_client, "request", AsyncMock(return_value=response)) as request:
            rates = await exchange.get_funding_rates()
        request.assert_awaited_once()
        self.assertEqual(request.call_args.args[0], "GET")
        self.assertTrue(request.call_args.args[1].endswith("/api/v1/funding-rates"))
        self.assertNotIn("authorization", {k.lower() for k in request.call_args.kwargs["headers"]})
        self.assertEqual(exchange._get_symbol_by_market_id.await_count, len(capture["markets"]))
        return exchange, rates

    async def check_adapter(self, venue):
        exchange, rates = await self.replay_rest(venue)
        processed = exchange.process_funding_rates(rates)
        self.assertEqual(len(processed), len(CAPTURE["venues"][venue]["markets"]))
        for market in CAPTURE["venues"][venue]["markets"]:
            stat = market["websocket"]
            symbol = stat["symbol"]
            with self.subTest(venue=venue, symbol=symbol):
                self.assertAlmostEqual(processed[symbol]["rate"],
                                       ws_funding_to_hourly(stat["current_funding_rate"]), delta=1e-18)
                self.assertEqual(processed[symbol]["rate"], rates[symbol]["rate"])
                self.assertEqual(processed[symbol]["funding_interval_hours"], 1)
                self.assertEqual(processed[symbol]["exchange"], venue)
                self.assertEqual(processed[symbol]["raw"]["raw_rate"], market["rest_before"]["rate"])
                self.assertEqual(processed[symbol]["raw"]["raw_funding_interval_hours"], 8)

    async def test_core_real_sdk_to_normalized_rate_divides_only_once(self):
        await self.check_adapter("Lighter")

    async def test_rhc_real_sdk_to_normalized_rate_divides_only_once(self):
        await self.check_adapter("LighterRHC")

    async def test_processor_rejects_invalid_rates_without_fabricating_zero(self):
        exchange = LighterExchange.__new__(LighterExchange)
        result = exchange.process_funding_rates({
            "MISSING": {}, "NULL": {"rate": None}, "NAN": {"rate": "NaN"},
            "INF": {"rate": float("inf")}, "BOOL": {"rate": True},
            "ZERO": {"rate": 0}, "HOURLY": {"rate": Decimal("0.000012")},
        })
        self.assertEqual(set(result), {"ZERO", "HOURLY"})
        self.assertEqual(result["ZERO"]["rate"], 0)
        self.assertEqual(result["HOURLY"]["rate"], 0.000012)

    async def test_invalid_rest_rows_are_skipped_but_zero_is_kept(self):
        exchange = LighterExchange.__new__(LighterExchange)
        exchange.api_client = object()
        exchange._get_symbol_by_market_id = AsyncMock(side_effect=lambda mid: str(mid))
        rows = [SimpleNamespace(exchange="lighter", market_id=i, symbol=str(i), rate=rate)
                for i, rate in enumerate((None, "NaN", True, 0, "-0.000072"))]
        with patch("model.exchanges.lighter.lighter.FundingApi") as api:
            api.return_value.funding_rates = AsyncMock(return_value=SimpleNamespace(funding_rates=rows))
            rates = await exchange.get_funding_rates()
        self.assertEqual(set(rates), {"3", "4"})
        self.assertEqual(rates["3"]["rate"], 0)
        self.assertEqual(rates["4"]["rate"], -0.000009)

    async def test_ws_uses_upcoming_estimate_not_last_paid_rate(self):
        queue = asyncio.Queue()
        btc = next(m["websocket"] for m in CAPTURE["venues"]["Lighter"]["markets"]
                   if m["websocket"]["symbol"] == "BTC")
        self.assertNotEqual(btc["current_funding_rate"], btc["funding_rate"])
        await FundingArbitrageEngine._queue_lighter_ws_funding(btc, "BTC", queue)
        venue, rate, timestamp = queue.get_nowait()
        self.assertEqual((venue, rate), ("Lighter", 0.000012))
        self.assertGreater(timestamp, 0)

    async def test_ws_preserves_negative_and_zero_rates(self):
        queue = asyncio.Queue()
        for raw, expected in (("-0.0009", -0.000009), ("0", 0)):
            await FundingArbitrageEngine._queue_lighter_ws_funding(
                {"symbol": "BTC", "current_funding_rate": raw}, "BTC", queue)
            self.assertEqual(queue.get_nowait()[1], expected)

    async def test_ws_ignores_other_markets_invalid_or_missing_current_estimate(self):
        queue = asyncio.Queue()
        for message in (None, {}, {"symbol": "BTC", "funding_rate": "0.0012"},
                        {"symbol": "ETH", "current_funding_rate": "0.0012"}):
            await FundingArbitrageEngine._queue_lighter_ws_funding(message, "BTC", queue)
        for value in (None, "NaN", "bad", True, "Infinity", "1e999999999"):
            await FundingArbitrageEngine._queue_lighter_ws_funding(
                {"symbol": "BTC", "current_funding_rate": value}, "BTC", queue)
        self.assertTrue(queue.empty())

    async def test_rest_poll_and_ws_queue_have_same_hourly_units(self):
        engine = FundingArbitrageEngine.__new__(FundingArbitrageEngine)
        exchange, rates = await self.replay_rest("Lighter")
        exchange.get_funding_rates = AsyncMock(return_value=rates)
        queue = asyncio.Queue()
        with patch("model.core.arbitrage_engine.asyncio.sleep", side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):
                await engine._poll_funding_rates(None, None, exchange, "BTC", queue,
                                                monitored_exchanges={"Lighter"})
        rest = queue.get_nowait()
        await engine._queue_lighter_ws_funding(
            {"symbol": "BTC", "current_funding_rate": "0.0012"}, "BTC", queue)
        ws = queue.get_nowait()
        self.assertEqual(rest[0], ws[0])
        self.assertAlmostEqual(rest[1], ws[1], delta=1e-18)
        self.assertAlmostEqual(rest[1], 0.000012, delta=1e-18)

    async def test_fixed_units_choose_correct_long_short_and_spread(self):
        exchange, rates = await self.replay_rest("Lighter")
        engine = FundingArbitrageEngine.__new__(FundingArbitrageEngine)
        opportunities = engine.find_arbitrage_opportunities({
            "Lighter": exchange.process_funding_rates(rates),
            "Hyperliquid": {"BTC": {"rate": 0.0000125, "funding_interval_hours": 1}},
        }, min_diff=0.0000001)
        self.assertEqual(len(opportunities), 1)
        opportunity = opportunities[0]
        self.assertEqual((opportunity["long_exchange"], opportunity["short_exchange"]),
                         ("Lighter", "Hyperliquid"))
        self.assertAlmostEqual(opportunity["potential_profit"], 0.0000005, delta=1e-18)
        self.assertAlmostEqual(opportunity["daily_profit_rate"], 0.000012, delta=1e-18)
