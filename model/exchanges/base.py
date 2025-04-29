from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Callable

class BaseExchange(ABC):
    """Base class for all exchange implementations."""
    
    @abstractmethod
    def get_funding_rates(self) -> Dict:
        """Get current funding rates from the exchange."""
        pass

    @abstractmethod
    def process_funding_rates(self, raw_data: Dict) -> Dict[str, Dict]:
        """Process raw funding rates into a normalized format."""
        pass
    
    @abstractmethod
    def get_positions(self) -> List[Dict]:
        """Get current positions."""
        pass
    
    @abstractmethod
    def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Optional[float] = None,
        quote_quantity: Optional[float] = None,
        reduce_only: bool = False
    ) -> Dict:
        """Place a market order."""
        pass
    
    @abstractmethod
    def close_position(self, symbol: str) -> Dict:
        """Close position for a specific symbol."""
        pass
    
    @abstractmethod
    def open_long(self, asset: str, amount: float) -> Dict:
        """Open a long position."""
        pass
    
    @abstractmethod
    def open_short(self, asset: str, amount: float) -> Dict:
        """Open a short position."""
        pass
    
    @abstractmethod
    def format_symbol(self, asset: str) -> str:
        """Format asset name to exchange-specific symbol format."""
        pass
    
    @abstractmethod
    def subscribe_to_funding_updates(self, callback: Callable) -> Any:
        """Subscribe to funding rate updates."""
        pass
