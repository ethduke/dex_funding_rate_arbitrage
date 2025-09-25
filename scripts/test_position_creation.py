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

    # Backpack: show raw balances and summarize
    try:
        bp_bal = bp.get_balances()
        try:
            import json
            raw = json.dumps(bp_bal, indent=2) if not isinstance(bp_bal, str) else bp_bal
            logger.info(f"Backpack balances raw (trimmed):\n{raw[:2000]}")
        except Exception:
            logger.info(f"Backpack balances raw (unserializable): {type(bp_bal)}")

        # Summarize entries in common shapes
        if isinstance(bp_bal, dict):
            items = bp_bal.get("data") or bp_bal.get("balances") or bp_bal.get("assets")
            if isinstance(items, list):
                for it in items:
                    sym = (it.get("symbol") or it.get("asset") or it.get("currency") or "").upper()
                    available = it.get("available") or it.get("free")
                    locked = it.get("locked") or it.get("inOrder")
                    staked = it.get("staked")
                    if sym:
                        logger.info(f"Backpack item: {sym} available={available} locked={locked} staked={staked}")
            else:
                for sym, it in bp_bal.items():
                    if isinstance(it, dict):
                        available = it.get("available")
                        locked = it.get("locked")
                        staked = it.get("staked")
                        logger.info(f"Backpack entry: {sym} available={available} locked={locked} staked={staked}")
        elif isinstance(bp_bal, list):
            for it in bp_bal:
                logger.info(f"Backpack list entry: {it}")
        else:
            logger.info(f"Backpack balances unexpected type: {type(bp_bal)} -> {bp_bal}")
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
    ASSET = "0G"              # asset symbol to trade
    SIZE_USD = 50.0            # USD notional per leg
    EXECUTE = True             # set True to place orders, False to only print diagnostics
    CLOSE_AFTER = False        # set True to attempt closing both legs after placement
    DO_BACKPACK_LONG = True    # control which legs to place (HL disabled per request)
    DO_HYPERLIQUID_SHORT = False
    # ===========================================

    # Initialize clients
    bp = BackpackExchange(use_ws=False)
    hl = HyperliquidExchange()

    # Diagnostics
    print_diagnostics(ASSET, SIZE_USD, bp, hl)

    if not EXECUTE:
        logger.info("Diagnostics complete (EXECUTE=False).")
        return 0

    # Selective execution
    rc = 0
    if DO_BACKPACK_LONG and DO_HYPERLIQUID_SHORT:
        rc = execute(ASSET, SIZE_USD, bp, hl)
    elif DO_HYPERLIQUID_SHORT and not DO_BACKPACK_LONG:
        try:
            logger.info(f"Opening SHORT on Hyperliquid for {ASSET} size ${SIZE_USD}")
            res_short = hl.open_short(ASSET, SIZE_USD)
            logger.info(f"Hyperliquid open_short result: {res_short}")
            if isinstance(res_short, dict) and res_short.get("status") in ("error", "err"):
                rc = 1
        except Exception as e:
            logger.error(f"Hyperliquid SHORT exception: {e}")
            rc = 1
    elif DO_BACKPACK_LONG and not DO_HYPERLIQUID_SHORT:
        try:
            logger.info(f"Opening LONG on Backpack for {ASSET} size ${SIZE_USD}")
            res_long = bp.open_long(ASSET, SIZE_USD)
            logger.info(f"Backpack open_long result: {res_long}")
            if isinstance(res_long, dict) and res_long.get("error"):
                rc = 1
        except Exception as e:
            logger.error(f"Backpack LONG exception: {e}")
            rc = 1

    if CLOSE_AFTER and DO_BACKPACK_LONG and DO_HYPERLIQUID_SHORT:
        close_both(ASSET, bp, hl)

    return rc


if __name__ == "__main__":
    sys.exit(main())


