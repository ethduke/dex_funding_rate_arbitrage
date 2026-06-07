import os
import unittest


os.environ.setdefault("BACKPACK_API_SECRET", "test-backpack-secret")
os.environ.setdefault("BACKPACK_API_KEY", "test-api-key")
os.environ.setdefault("HYPERLIQUID_API_PRIVATE_KEY", "test-hyperliquid-private-key")
os.environ.setdefault("HYPERLIQUID_ADDRESS", "test-hyperliquid-address")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "test-lighter-private-key")

from model.exchanges import lighter as lighter_module
from model.exchanges.lighter import LighterExchange


class SelfTradeSigner:
    ORDER_TYPE_MARKET = 1
    ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
    DEFAULT_IOC_EXPIRY = 0

    def __init__(self):
        self.best_price_call = None
        self.create_order_call = None

    async def get_best_price(self, market_index, is_ask):
        self.best_price_call = {"market_index": market_index, "is_ask": is_ask}
        return 100_00

    async def create_order(
        self,
        market_index,
        client_order_index,
        base_amount,
        price,
        is_ask,
        order_type,
        time_in_force,
        reduce_only=False,
        trigger_price=0,
        order_expiry=-1,
        *,
        integrator_account_index: int = 0,
        integrator_taker_fee: int = 0,
        integrator_maker_fee: int = 0,
        self_trade_behavior_mode: int = 0,
        self_trade_equality_mode: int = 0,
        skip_nonce: int = 0,
        nonce: int = -1,
        api_key_index: int = 255,
    ):
        self.create_order_call = {
            "market_index": market_index,
            "client_order_index": client_order_index,
            "base_amount": base_amount,
            "price": price,
            "is_ask": is_ask,
            "order_type": order_type,
            "time_in_force": time_in_force,
            "reduce_only": reduce_only,
            "order_expiry": order_expiry,
            "self_trade_behavior_mode": self_trade_behavior_mode,
            "self_trade_equality_mode": self_trade_equality_mode,
            "api_key_index": api_key_index,
        }
        return {"tx": "created"}, {"code": 200}, None


class LegacySigner:
    def __init__(self):
        self.limited_slippage_call = None

    async def create_market_order_limited_slippage(
        self,
        market_index,
        client_order_index,
        base_amount,
        max_slippage,
        is_ask,
        reduce_only=False,
        *,
        api_key_index=255,
    ):
        self.limited_slippage_call = {
            "market_index": market_index,
            "client_order_index": client_order_index,
            "base_amount": base_amount,
            "max_slippage": max_slippage,
            "is_ask": is_ask,
            "reduce_only": reduce_only,
            "api_key_index": api_key_index,
        }
        return {"tx": "created"}, {"code": 200}, None


class LighterSelfTradeOrderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._old_config = {
            "LIGHTER_MAX_SLIPPAGE": getattr(lighter_module.CONFIG, "LIGHTER_MAX_SLIPPAGE", None),
            "LIGHTER_SELF_TRADE_BEHAVIOR_MODE": lighter_module.CONFIG.LIGHTER_SELF_TRADE_BEHAVIOR_MODE,
            "LIGHTER_SELF_TRADE_EQUALITY_MODE": lighter_module.CONFIG.LIGHTER_SELF_TRADE_EQUALITY_MODE,
            "LIGHTER_API_KEY_INDEX": lighter_module.CONFIG.LIGHTER_API_KEY_INDEX,
        }

    def tearDown(self):
        for key, value in self._old_config.items():
            if value is None and hasattr(lighter_module.CONFIG, key):
                delattr(lighter_module.CONFIG, key)
            else:
                setattr(lighter_module.CONFIG, key, value)

    def _exchange_with_signer(self, signer):
        exchange = LighterExchange.__new__(LighterExchange)
        exchange.signer_client = signer
        exchange._ensure_account_initialized = lambda: _async_none()
        exchange._get_market_id = lambda symbol: _async_value(42)
        exchange._get_market_decimals = lambda market_id: _async_value({"sizeDecimal": 3, "priceDecimal": 2})
        exchange._get_estimated_price = lambda symbol: _async_value(100.0)
        exchange._get_next_client_order_index = lambda: 777
        return exchange

    async def test_market_order_passes_self_trade_modes_with_slippage_price(self):
        lighter_module.CONFIG.LIGHTER_MAX_SLIPPAGE = 0.05
        lighter_module.CONFIG.LIGHTER_SELF_TRADE_BEHAVIOR_MODE = 2
        lighter_module.CONFIG.LIGHTER_SELF_TRADE_EQUALITY_MODE = 1
        lighter_module.CONFIG.LIGHTER_API_KEY_INDEX = 9
        signer = SelfTradeSigner()
        exchange = self._exchange_with_signer(signer)

        result = await exchange._place_market_order_async("BTC", "BUY", quote_quantity=10.0)

        self.assertTrue(result["success"])
        self.assertEqual(signer.best_price_call, {"market_index": 42, "is_ask": False})
        self.assertEqual(signer.create_order_call["base_amount"], 100)
        self.assertEqual(signer.create_order_call["price"], 10500)
        self.assertEqual(signer.create_order_call["order_type"], signer.ORDER_TYPE_MARKET)
        self.assertEqual(
            signer.create_order_call["time_in_force"],
            signer.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
        )
        self.assertEqual(signer.create_order_call["order_expiry"], signer.DEFAULT_IOC_EXPIRY)
        self.assertEqual(signer.create_order_call["self_trade_behavior_mode"], 2)
        self.assertEqual(signer.create_order_call["self_trade_equality_mode"], 1)
        self.assertEqual(signer.create_order_call["api_key_index"], 9)

    async def test_market_order_falls_back_for_legacy_sdk(self):
        lighter_module.CONFIG.LIGHTER_MAX_SLIPPAGE = 0.02
        lighter_module.CONFIG.LIGHTER_API_KEY_INDEX = 4
        signer = LegacySigner()
        exchange = self._exchange_with_signer(signer)

        result = await exchange._place_market_order_async("ETH", "SELL", quantity=0.25, reduce_only=True)

        self.assertTrue(result["success"])
        self.assertEqual(signer.limited_slippage_call["market_index"], 42)
        self.assertEqual(signer.limited_slippage_call["client_order_index"], 777)
        self.assertEqual(signer.limited_slippage_call["base_amount"], 250)
        self.assertEqual(signer.limited_slippage_call["max_slippage"], 0.02)
        self.assertTrue(signer.limited_slippage_call["is_ask"])
        self.assertTrue(signer.limited_slippage_call["reduce_only"])
        self.assertEqual(signer.limited_slippage_call["api_key_index"], 4)


async def _async_none():
    return None


async def _async_value(value):
    return value


if __name__ == "__main__":
    unittest.main()
