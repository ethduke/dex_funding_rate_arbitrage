import os
import unittest


os.environ.setdefault("BACKPACK_API_SECRET", "test-backpack-secret")
os.environ.setdefault("BACKPACK_API_KEY", "test-api-key")
os.environ.setdefault("HYPERLIQUID_API_PRIVATE_KEY", "test-hyperliquid-private-key")
os.environ.setdefault("HYPERLIQUID_ADDRESS", "test-hyperliquid-address")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "test-lighter-private-key")

from model.exchanges import lighter as lighter_module
from model.exchanges.lighter import LighterExchange


class FakeSigner:
    def __init__(self, auth_error=None):
        self.auth_error = auth_error
        self.closed = False

    def create_auth_token_with_expiry(self, api_key_index):
        return "test-auth", self.auth_error

    async def close(self):
        self.closed = True


class FakeAccountApi:
    def __init__(self, maker_only_indexes=None, error=None):
        self.maker_only_indexes = maker_only_indexes or []
        self.error = error

    async def get_maker_only_api_keys(self, authorization, account_index):
        if self.error:
            raise self.error
        return type(
            "MakerOnlyKeys",
            (),
            {"api_key_indexes": self.maker_only_indexes},
        )()


class LighterMakerOnlyKeyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_api_key_index = lighter_module.CONFIG.LIGHTER_API_KEY_INDEX
        lighter_module.CONFIG.LIGHTER_API_KEY_INDEX = 9

    def tearDown(self):
        lighter_module.CONFIG.LIGHTER_API_KEY_INDEX = self.old_api_key_index

    def _exchange(self, indexes=None, error=None):
        exchange = LighterExchange.__new__(LighterExchange)
        exchange.signer_client = FakeSigner()
        exchange.account_api = FakeAccountApi(indexes, error)
        return exchange

    async def test_disables_market_orders_for_maker_only_key(self):
        exchange = self._exchange(indexes=[4, 9])
        signer = exchange.signer_client

        compatible = await exchange._ensure_market_order_key_compatible()

        self.assertFalse(compatible)
        self.assertTrue(signer.closed)
        self.assertIsNone(exchange.signer_client)

    async def test_keeps_market_orders_for_regular_key(self):
        exchange = self._exchange(indexes=[4, 5])
        signer = exchange.signer_client

        compatible = await exchange._ensure_market_order_key_compatible()

        self.assertTrue(compatible)
        self.assertFalse(signer.closed)
        self.assertIs(exchange.signer_client, signer)

    async def test_status_check_failure_does_not_disable_signer(self):
        exchange = self._exchange(error=RuntimeError("temporary API failure"))
        signer = exchange.signer_client

        compatible = await exchange._ensure_market_order_key_compatible()

        self.assertTrue(compatible)
        self.assertFalse(signer.closed)
        self.assertIs(exchange.signer_client, signer)


if __name__ == "__main__":
    unittest.main()
