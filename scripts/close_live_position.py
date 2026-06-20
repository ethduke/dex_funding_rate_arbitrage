import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from model.exchanges.lighter import LighterExchange
from model.exchanges.tradexyz import TradeXYZExchange
from model.exchanges.normalized import to_float


def json_default(value: Any) -> str:
    return str(value)


def print_json(label: str, value: Any) -> None:
    print(f"\n--- {label} ---")
    print(json.dumps(value, indent=2, sort_keys=True, default=json_default))


def signed_lighter_size(position: Any) -> float:
    raw_size = abs(to_float(getattr(position, "position", 0)))
    sign = getattr(position, "sign", None)
    if sign is None:
        return to_float(getattr(position, "position", 0))
    return raw_size if int(sign) > 0 else -raw_size


async def find_lighter_position(exchange: LighterExchange, asset: str) -> Optional[Any]:
    await exchange._ensure_account_initialized()
    account_details = await exchange.account_api.account(by="index", value=str(exchange.account_index))
    if not getattr(account_details, "accounts", None):
        return None

    for position in account_details.accounts[0].positions:
        if getattr(position, "symbol", None) == asset and to_float(getattr(position, "position", 0)) != 0:
            return position
    return None


async def close_lighter(asset: str, polls: int, poll_delay: float) -> bool:
    exchange = LighterExchange(use_ws=False)
    try:
        position = await find_lighter_position(exchange, asset)
        if not position:
            print_json("lighter_before", [])
            return True

        size = signed_lighter_size(position)
        quantity = abs(to_float(getattr(position, "position", 0)))
        side = "SELL" if size > 0 else "BUY"
        print_json(
            "lighter_before",
            {
                "symbol": getattr(position, "symbol", None),
                "position": getattr(position, "position", None),
                "sign": getattr(position, "sign", None),
                "signed_size": size,
                "close_side": side,
                "quantity": quantity,
            },
        )

        result = await exchange.place_market_order(
            symbol=asset,
            side=side,
            quantity=quantity,
            reduce_only=True,
        )
        print_json("lighter_close_result", result)
        if not result.get("success"):
            return False

        for index in range(polls):
            await asyncio.sleep(poll_delay)
            remaining = await find_lighter_position(exchange, asset)
            print_json(f"lighter_after_poll_{index}", [] if not remaining else {
                "symbol": getattr(remaining, "symbol", None),
                "position": getattr(remaining, "position", None),
                "sign": getattr(remaining, "sign", None),
                "signed_size": signed_lighter_size(remaining),
            })
            if not remaining:
                return True
        return False
    finally:
        await exchange.close()


def active_tradexyz_positions(exchange: TradeXYZExchange, asset: str):
    positions = []
    for position in exchange.get_positions():
        try:
            size = float(position.get("size") or position.get("szi") or 0)
        except (TypeError, ValueError):
            size = 0.0
        if position.get("asset") == asset and abs(size) > 0:
            positions.append(position)
    return positions


def close_tradexyz(asset: str, polls: int, poll_delay: float) -> bool:
    exchange = TradeXYZExchange()
    before = active_tradexyz_positions(exchange, asset)
    print_json("tradexyz_before", before)
    if not before:
        return True

    result = exchange.close_position(asset)
    print_json("tradexyz_close_result", result)
    if not result.get("success"):
        return False

    for index in range(polls):
        time.sleep(poll_delay)
        remaining = active_tradexyz_positions(exchange, asset)
        print_json(f"tradexyz_after_poll_{index}", remaining)
        if not remaining:
            return True
    return False


async def main() -> int:
    parser = argparse.ArgumentParser(description="Close a live arbitrage position on supported venues.")
    parser.add_argument("--asset", default="AMD")
    parser.add_argument("--exchange", action="append", choices=["Lighter", "TradeXYZ"], help="Can be repeated; defaults to both")
    parser.add_argument("--polls", type=int, default=8)
    parser.add_argument("--poll-delay", type=float, default=2.0)
    args = parser.parse_args()

    exchanges = args.exchange or ["TradeXYZ", "Lighter"]
    results = {}

    if "TradeXYZ" in exchanges:
        results["TradeXYZ"] = close_tradexyz(args.asset, args.polls, args.poll_delay)
    if "Lighter" in exchanges:
        results["Lighter"] = await close_lighter(args.asset, args.polls, args.poll_delay)

    print_json("summary", results)
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(exit_code)
