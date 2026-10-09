from lighter.endpoint_profiles import ROBINHOOD, ROBINHOOD_TESTNET

from model.exchanges.lighter import LighterExchange


class LighterRHCExchange(LighterExchange):
    """Independent Lighter venue on Robinhood Chain with USDG collateral."""

    exchange_name = "LighterRHC"
    config_prefix = "LIGHTER_RHC"
    default_api_url = ROBINHOOD.api_url
    collateral_asset = "USDG"
    market_mapping_path = "data/lighter_rhc_markets.json"

    def __init__(self, use_ws=False, order_book_ids=None):
        url = self._config("API_URL", self.default_api_url).rstrip("/")
        if url not in {ROBINHOOD.api_url, ROBINHOOD_TESTNET.api_url}:
            raise ValueError("LIGHTER_RHC_API_URL must use an official RHC mainnet or testnet endpoint")
        super().__init__(use_ws=use_ws, order_book_ids=order_book_ids)
