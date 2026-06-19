from typing import Dict, List

from model.exchanges.hyperliquid import HyperliquidExchange
from model.exchanges.normalized import FundingRate, to_float


class TradeXYZExchange(HyperliquidExchange):
    """Trade.xyz HIP-3 perp DEX adapter backed by Hyperliquid APIs."""

    exchange_name = "TradeXYZ"
    dex = "xyz"
    perp_dexs = ("", "xyz")
    symbol_prefix = "xyz:"

    def get_funding_rates(self) -> List:
        """Get current Trade.xyz funding and market contexts from Hyperliquid."""
        try:
            return self.info.post("/info", {"type": "metaAndAssetCtxs", "dex": self.dex})
        except Exception as e:
            return {"error": f"Unexpected error: {str(e)}"}

    def process_funding_rates(self, raw_data: List) -> Dict[str, Dict]:
        """Convert Trade.xyz asset contexts to the shared funding-rate shape."""
        if isinstance(raw_data, dict) and raw_data.get("error"):
            return {}
        if not isinstance(raw_data, list) or len(raw_data) < 2:
            return {}

        meta, asset_contexts = raw_data[0], raw_data[1]
        markets = meta.get("universe", []) if isinstance(meta, dict) else []
        result = {}

        for market, context in zip(markets, asset_contexts):
            if not isinstance(market, dict) or not isinstance(context, dict):
                continue

            symbol = market.get("name")
            if not symbol:
                continue

            asset = self.normalize_asset_symbol(symbol)
            result[asset] = FundingRate(
                asset=asset,
                exchange=self.exchange_name,
                rate=to_float(context.get("funding")),
                mark_price=to_float(context.get("markPx"), None),
                index_price=to_float(context.get("oraclePx"), None),
                raw={"market": market, "context": context},
            ).to_dict()

        return result
