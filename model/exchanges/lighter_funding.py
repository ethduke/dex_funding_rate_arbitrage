"""Lighter source units converted to fractional funding per hour.

The public REST /funding-rates aggregator quotes eight-hour-equivalent fractions.
market_stats.current_funding_rate quotes hourly percentage points instead.
Settlement cadence alone must never be used to infer either source's units.
"""

from decimal import Decimal, DecimalException
import math


def finite_funding_rate(value) -> float:
    """Validate an already-normalized hourly fraction without rescaling it."""
    return _scaled_rate(value, Decimal(1))


def _scaled_rate(value, divisor: Decimal) -> float:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("Funding rate must be a finite number")
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            raise ValueError("Funding rate must be a finite number")
        result = float(number / divisor)
    except (DecimalException, OverflowError):
        raise ValueError("Funding rate must be a finite number") from None
    if not math.isfinite(result):
        raise ValueError("Funding rate must be a finite number")
    return result


def rest_funding_to_hourly(value) -> float:
    """REST eight-hour-equivalent fraction -> hourly fraction, exactly once."""
    return _scaled_rate(value, Decimal(8))


def ws_funding_to_hourly(value) -> float:
    """market_stats hourly percentage points -> hourly fraction (no /8)."""
    return _scaled_rate(value, Decimal(100))
