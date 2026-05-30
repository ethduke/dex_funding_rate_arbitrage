from dataclasses import dataclass, field
from typing import Any, Dict, Optional


def to_float(value: Any, default: float = 0.0) -> float:
    """Convert exchange numeric strings to float without leaking exceptions."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _without_none(data: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in data.items() if value is not None}


@dataclass(frozen=True)
class FundingRate:
    asset: str
    exchange: str
    rate: float
    next_funding_time: Optional[int] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    funding_interval_hours: Optional[float] = None
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        return _without_none({
            "asset": self.asset,
            "exchange": self.exchange,
            "rate": self.rate,
            "next_funding_time": self.next_funding_time,
            "mark_price": self.mark_price,
            "index_price": self.index_price,
            "funding_interval_hours": self.funding_interval_hours,
            "raw": self.raw or None,
        })


@dataclass(frozen=True)
class BalanceSnapshot:
    exchange: str
    available_usd: float
    total_usd: Optional[float] = None
    account_id: Optional[Any] = None
    positions_count: Optional[int] = None
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        return _without_none({
            "exchange": self.exchange,
            "available_usd": self.available_usd,
            "total_usd": self.total_usd,
            "account_id": self.account_id,
            "positions_count": self.positions_count,
            "raw": self.raw or None,
        })


@dataclass(frozen=True)
class Position:
    asset: str
    exchange: str
    size: float
    side: Optional[str] = None
    symbol: Optional[str] = None
    entry_price: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    realized_pnl: Optional[float] = None
    market_id: Optional[Any] = None
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        return _without_none({
            "asset": self.asset,
            "exchange": self.exchange,
            "symbol": self.symbol or self.asset,
            "size": self.size,
            "side": self.side,
            "entry_price": self.entry_price,
            "unrealized_pnl": self.unrealized_pnl,
            "realized_pnl": self.realized_pnl,
            "market_id": self.market_id,
            "raw": self.raw or None,
        })


@dataclass(frozen=True)
class OrderResult:
    exchange: str
    success: bool
    asset: Optional[str] = None
    side: Optional[str] = None
    size: Optional[float] = None
    price: Optional[float] = None
    order_id: Optional[Any] = None
    error: Optional[str] = None
    message: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        status = "ok" if self.success else "error"
        return _without_none({
            "exchange": self.exchange,
            "status": status,
            "success": self.success,
            "asset": self.asset,
            "side": self.side,
            "size": self.size,
            "price": self.price,
            "order_id": self.order_id,
            "error": self.error,
            "message": self.message,
            "raw": self.raw or None,
        })
