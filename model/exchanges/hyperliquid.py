from typing import Dict, List, Callable, Optional, Any
import eth_account
import logging
import requests
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

from utils.config import CONFIG
from model.exchanges.base import BaseExchange

logger = logging.getLogger(__name__)

class HyperliquidExchange(BaseExchange):
    def __init__(self):
        """Initialize Hyperliquid client using the official SDK."""

        self.base_url = CONFIG.get("HYPERLIQUID_API_URL")
        
        private_key = CONFIG.get("HYPERLIQUID_API_PRIVATE_KEY")
        if not private_key:
            raise ValueError("No secret_key found in config.json")

        self.account = eth_account.Account.from_key(private_key)
        self.address = CONFIG.get("HYPERLIQUID_ADDRESS") or self.account.address
        
        self.info = Info(self.base_url, skip_ws=False)
        self.exchange = Exchange(self.account, self.base_url, account_address=self.address)
        
        print(f"Initialized client with address: {self.address}")
    
    def get_funding_rates(self) -> Dict:
        """Get predicted funding rates from the Hyperliquid API."""
        try:
            headers = {
                "Content-Type": "application/json"
            }
            
            payload = {
                "type": "predictedFundings"
            }
            
            response = requests.post(
                f"{self.base_url}/info",
                headers=headers,
                json=payload,
                timeout=10  
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get funding rates. Status code: {response.status_code}")
                return {}
                
            data = response.json()
            return data
            
        except requests.Timeout:
            logger.error("Request to Hyperliquid API timed out")
            return {}
        except requests.RequestException as e:
            logger.error(f"Request to Hyperliquid API failed: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"Error getting funding rates: {e}")
            return {}
    
    def get_positions(self) -> List[Dict]:
        """Get current positions."""
        try:
            user_state = self.info.user_state(self.address)
            return user_state.get("assetPositions", [])
        except Exception as e:
            print(f"Error getting positions: {e}")
            return []
    
    def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Optional[float] = None,
        quote_quantity: Optional[float] = None,
        reduce_only: bool = False
    ) -> Dict:
        """Place a market order using the exact function signature from the example."""
        try:
            # Convert side to is_buy
            is_buy = side.lower() == "bid"
            
            # Use quantity or quote_quantity
            size = quantity if quantity is not None else quote_quantity
            
            if size is None:
                return {"status": "error", "message": "Either quantity or quote_quantity must be provided"}
            
            slippage = 0.01  # 1% slippage tolerance
            order_result = self.exchange.market_open(
                symbol,      
                is_buy,  
                size,      
                None,      
                slippage  
            )
            return order_result
        except Exception as e:
            print(f"Error placing market order: {e}")
            return {"status": "error", "message": str(e)}
    
    def close_position(self, symbol: str) -> Dict:
        """Close position for a specific coin."""
        try:
            # Use the exact function signature from the example
            order_result = self.exchange.market_close(symbol)
            return order_result
        except Exception as e:
            print(f"Error closing position: {e}")
            return {"status": "error", "message": str(e)}
    
    def open_long(self, asset: str, amount: float) -> Dict:
        """Open a long position."""
        return self.place_market_order(
            symbol=asset,
            side="bid",
            quantity=amount
        )
    
    def open_short(self, asset: str, amount: float) -> Dict:
        """Open a short position."""
        return self.place_market_order(
            symbol=asset,
            side="ask",
            quantity=amount
        )

    def format_symbol(self, asset: str) -> str:
        """Format asset name to exchange-specific symbol format."""
        return asset  # Hyperliquid doesn't need special formatting

    def subscribe_to_funding_updates(self, callback: Callable) -> Any:
        """Subscribe to funding rate updates."""
        # Simple parse for funding events
        def funding_callback(data):
            print(f"Funding received: {data}")
            if callback:
                callback(data)
                
        subscription = {"type": "userFundings", "user": self.address}
        return self.info.subscribe(subscription, funding_callback)
    
    def process_funding_rates(self, hl_funding: List) -> Dict[str, Dict]:
        """Convert Hyperliquid funding rates to a normalized funding rate dict."""
        result = {}
        for asset_data in hl_funding:
            asset = asset_data[0]
            for venue_data in asset_data[1]:
                if venue_data[0] == "HlPerp":  # Only use Hyperliquid's own rate
                    result[asset] = {
                        "rate": float(venue_data[1].get("fundingRate", "0")),
                        "next_funding_time": venue_data[1].get("nextFundingTime", 0),
                        "exchange": "Hyperliquid"
                    }
        return result