from typing import Dict, List, Optional

from model.exchanges.hyperliquid import HyperliquidExchange
from model.exchanges.normalized import BalanceSnapshot, FundingRate, to_float


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

    def get_balance_snapshot(self, asset: Optional[str] = None) -> BalanceSnapshot:
        """Return Trade.xyz collateral availability, including unified-account spot USDC."""
        balance = super().get_balance_snapshot(asset)
        if balance.available_usd > 0:
            return balance

        try:
            abstraction = self.info.post(
                "/info",
                {"type": "userAbstraction", "user": self.address},
            )
            if abstraction != "unifiedAccount":
                return balance

            spot_state = self.info.post(
                "/info",
                {"type": "spotClearinghouseState", "user": self.address},
            )
            available = self._extract_unified_usdc_available(spot_state)
            if available <= 0:
                return balance

            raw = dict(balance.raw or {})
            raw.update({
                "userAbstraction": abstraction,
                "spotClearinghouseState": spot_state,
            })
            return BalanceSnapshot(
                exchange=self.exchange_name,
                available_usd=available,
                total_usd=available,
                account_id=self.address,
                positions_count=balance.positions_count,
                raw=raw,
            )
        except Exception:
            return balance

    @staticmethod
    def _extract_unified_usdc_available(spot_state: Dict) -> float:
        if not isinstance(spot_state, dict):
            return 0.0

        token_availability = spot_state.get("tokenToAvailableAfterMaintenance")
        if isinstance(token_availability, list):
            for item in token_availability:
                if isinstance(item, list) and len(item) >= 2 and item[0] == 0:
                    return to_float(item[1])

        for balance in spot_state.get("balances", []):
            if not isinstance(balance, dict) or balance.get("coin") != "USDC":
                continue
            total = to_float(balance.get("total"))
            hold = to_float(balance.get("hold"))
            return max(total - hold, 0.0)

        return 0.0
