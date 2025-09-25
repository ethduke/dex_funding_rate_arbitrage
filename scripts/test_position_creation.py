#!/usr/bin/env python3
"""
Ad-hoc tester to attempt a LONG on Backpack and SHORT on Hyperliquid
for a given asset and USD size, with clear balance diagnostics and dry-run mode.

Configure the parameters below and run directly:
  python3 scripts/test_position_creation.py
"""

import sys

from utils.logger import setup_logger
from model.exchanges.backpack import BackpackExchange
from model.exchanges.hyperliquid import HyperliquidExchange


logger = setup_logger("test_position_creation")


def get_hyperliquid_balances(hl: HyperliquidExchange):
    try:
        us = hl.info.user_state(hl.address)
        cms = us.get("crossMarginSummary", {}) if isinstance(us, dict) else {}
        withdrawable = float(cms.get("withdrawable", 0)) if cms else 0.0
        equity = float(cms.get("accountValue", 0)) if cms else 0.0
        return withdrawable, equity
    except Exception as e:
        logger.warning(f"Could not fetch Hyperliquid balances: {e}")
        return 0.0, 0.0


def print_diagnostics(asset: str, size: float, bp: BackpackExchange, hl: HyperliquidExchange):
    logger.info(f"Planned trade: LONG on Backpack, SHORT on Hyperliquid | asset={asset}, size=${size}")

    # Backpack: show available balance if possible
    try:
        bp_bal = bp.get_balances()
        available = None
        items = []
        if isinstance(bp_bal, dict):
            items = bp_bal.get("data") or bp_bal.get("balances") or bp_bal.get("assets") or []
        elif isinstance(bp_bal, list):
            items = bp_bal
        for item in items:
            sym = (item.get("symbol") or item.get("asset") or "").upper()
            if sym in ("USDC", "USD"):
                available = float(item.get("available", item.get("free", 0)))
                break
        if available is not None:
            logger.info(f"Backpack available: ${available:.2f}")
        else:
            logger.info("Backpack balance: unavailable (no USDC entry found)")
    except Exception as e:
        logger.warning(f"Backpack balances fetch failed: {e}")

    # Hyperliquid: show withdrawable/equity
    w, eq = get_hyperliquid_balances(hl)
    logger.info(f"Hyperliquid balances -> withdrawable=${w:.2f}, equity=${eq:.2f}")


def execute(asset: str, size: float, bp: BackpackExchange, hl: HyperliquidExchange) -> int:
    # Open LONG on Backpack
    try:
        logger.info(f"Opening LONG on Backpack for {asset} size ${size}")
        res_long = bp.open_long(asset, size)
        logger.info(f"Backpack open_long result: {res_long}")
        if isinstance(res_long, dict) and res_long.get("error"):
            logger.error("Backpack LONG failed")
            return 1
    except Exception as e:
        logger.error(f"Backpack LONG exception: {e}")
        return 1

    # Open SHORT on Hyperliquid
    try:
        logger.info(f"Opening SHORT on Hyperliquid for {asset} size ${size}")
        res_short = hl.open_short(asset, size)
        logger.info(f"Hyperliquid open_short result: {res_short}")
        if isinstance(res_short, dict) and res_short.get("status") == "error":
            logger.error("Hyperliquid SHORT failed; attempting to close Backpack LONG")
            try:
                bp.close_asset_position(asset)
            except Exception as ce:
                logger.error(f"Cleanup failed closing Backpack LONG: {ce}")
            return 1
    except Exception as e:
        logger.error(f"Hyperliquid SHORT exception: {e}; attempting to close Backpack LONG")
        try:
            bp.close_asset_position(asset)
        except Exception as ce:
            logger.error(f"Cleanup failed closing Backpack LONG: {ce}")
        return 1

    logger.info("Both legs placed successfully (subject to exchange responses)")
    return 0


def close_both(asset: str, bp: BackpackExchange, hl: HyperliquidExchange):
    try:
        logger.info(f"Closing Backpack position for {asset}")
        bp.close_asset_position(asset)
    except Exception as e:
        logger.warning(f"Backpack close exception: {e}")
    try:
        logger.info(f"Closing Hyperliquid position for {asset}")
        hl.close_position(asset)
    except Exception as e:
        logger.warning(f"Hyperliquid close exception: {e}")


def main():
    # ========= Configurable parameters =========
    ASSET = "0G"          # asset symbol to trade
    SIZE_USD = 50.0        # USD notional per leg
    EXECUTE = True         # set True to place orders, False to only print diagnostics
    CLOSE_AFTER = False    # set True to attempt closing both legs after placement
    # ===========================================

    # Initialize clients
    bp = BackpackExchange(use_ws=False)
    hl = HyperliquidExchange()

    # Diagnostics
    print_diagnostics(ASSET, SIZE_USD, bp, hl)

    if not EXECUTE:
        logger.info("Diagnostics complete (EXECUTE=False).")
        return 0

    rc = execute(ASSET, SIZE_USD, bp, hl)

    if CLOSE_AFTER:
        close_both(ASSET, bp, hl)

    return rc


if __name__ == "__main__":
    sys.exit(main())


