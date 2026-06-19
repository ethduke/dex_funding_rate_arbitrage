from model.exchanges.hyperliquid import HyperliquidExchange


class TradeXYZExchange(HyperliquidExchange):
    """Trade.xyz HIP-3 perp DEX adapter backed by Hyperliquid APIs."""

    exchange_name = "TradeXYZ"
    dex = "xyz"
    symbol_prefix = "xyz:"
