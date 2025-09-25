from typing import Dict, List, Callable, Optional, Any
import eth_account
import logging
import requests
import math
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from utils.config import CONFIG
from model.exchanges.base import BaseExchange
from utils.logger import setup_logger

logger = setup_logger(__name__)

class HyperliquidExchange(BaseExchange):
    def __init__(self):
        """Initialize Hyperliquid client using the official SDK."""

        # Configure logging for Hyperliquid SDK
        logging.getLogger("hyperliquid").setLevel(logging.WARNING)
        logging.getLogger("websocket").setLevel(logging.WARNING)
        logging.getLogger("websockets").setLevel(logging.WARNING)
        
        self.base_url = CONFIG.get("HYPERLIQUID_API_URL")
        
        private_key = CONFIG.get("HYPERLIQUID_API_PRIVATE_KEY")
        if not private_key:
            raise ValueError("No secret_key found in config.json")

        self.account = eth_account.Account.from_key(private_key)
        self.address = CONFIG.get("HYPERLIQUID_ADDRESS") or self.account.address
        
        self.info = Info(self.base_url, skip_ws=False)
        self.exchange = Exchange(self.account, self.base_url, account_address=self.address)
            
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
                f"{CONFIG.get('HYPERLIQUID_API_URL_INFO')}",
                headers=headers,
                json=payload,
                timeout=10  
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get funding rates. Status code: {response.status_code}")
                return {"error": f"API error: status code {response.status_code}", "response": response.text}
                
            data = response.json()
            return data
            
        except requests.Timeout:
            logger.error("Request to Hyperliquid API timed out")
            return {"error": "Request timed out"}
        except requests.RequestException as e:
            logger.error(f"Request to Hyperliquid API failed: {str(e)}")
            return {"error": f"Request failed: {str(e)}"}
        except Exception as e:
            logger.error(f"Error getting funding rates: {e}")
            return {"error": f"Unexpected error: {str(e)}"}
    
    def get_positions(self) -> List[Dict]:
        """Get current positions."""
        try:
            user_state = self.info.user_state(self.address)
            return user_state.get("assetPositions", [])
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []
    
    def get_market_data(self, asset: str) -> Dict:
        """Get current market data for an asset.
        
        Returns a dict with price and metadata or empty dict if not found.
        """
        try:
            # Most direct way to get market data
            response = requests.post(
                f"{CONFIG.get('HYPERLIQUID_API_URL_INFO')}",
                headers={"Content-Type": "application/json"},
                json={"type": "marketData"},
                timeout=5
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get market data. Status code: {response.status_code}")
                return {}
                
            market_data = response.json()
            
            # Also get universe metadata for szDecimals
            meta_response = requests.post(
                f"{CONFIG.get('HYPERLIQUID_API_URL_INFO')}",
                headers={"Content-Type": "application/json"},
                json={"type": "meta"},
                timeout=5
            )
            
            asset_info = {}
            if meta_response.status_code == 200:
                meta_data = meta_response.json()
                for item in meta_data.get("universe", []):
                    if item.get("name") == asset:
                        asset_info = item
                        break
            
            # Find the asset in market data
            for item in market_data:
                if item.get("coin") == asset:
                    # Combine with metadata
                    return {
                        "price": float(item.get("markPx", 0)),
                        "szDecimals": asset_info.get("szDecimals", 2)
                    }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting market data: {e}")
            return {}
    
    def get_price_from_api(self, asset: str, usd_amount: float) -> float:
        try:
            meta_response = requests.post(
                f"{CONFIG.get('HYPERLIQUID_API_URL_INFO')}",
                headers={"Content-Type": "application/json"},
                json={"type": "allMids"},
                timeout=5
            )
            if meta_response.status_code == 200:
                all_prices = meta_response.json()
                price = float(all_prices.get(asset))
                # Convert USD amount to token amount
                token_amount = usd_amount / price
                
                # Get proper decimal precision for this asset
                sz_decimals = self.get_sz_decimals(asset)
                
                # Round to the appropriate decimal places to avoid precision errors
                token_amount = math.floor(token_amount * (10 ** sz_decimals)) / (10 ** sz_decimals)
                
                return token_amount
        except Exception as e:
            logger.error(f"Error in fallback price lookup: {str(e)}")
        
        return 0

    def usd_to_token_size(self, asset: str, usd_amount: float) -> float:
        """Convert USD amount to token size with proper decimal precision."""
        return self.get_price_from_api(asset, usd_amount)
    
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
            
            # Handle precision errors by rounding size to appropriate decimals
            if size > 0:
                sz_decimals = self.get_sz_decimals(symbol)
                size = math.floor(size * (10 ** sz_decimals)) / (10 ** sz_decimals)
                
                # If size becomes 0 after rounding, return error
                if size == 0:
                    return {"status": "error", "message": f"Size too small for {symbol} after precision adjustment"}
            
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
            logger.error(f"Error placing market order: {e}")
            return {"status": "error", "message": str(e)}
    
    def close_position(self, symbol: str) -> Dict:
        """Close position for a specific coin."""
        try:
            # Use the exact function signature from the example
            order_result = self.exchange.market_close(symbol)
            return order_result
        except Exception as e:
            logger.error(f"Error closing position: {e}")
            return {"status": "error", "message": str(e)}
    
    def get_sz_decimals(self, asset: str) -> int:
        """Get the size decimals for an asset from the universe metadata."""
        try:
            response = requests.post(
                f"{CONFIG.get('HYPERLIQUID_API_URL_INFO')}",
                headers={"Content-Type": "application/json"},
                json={"type": "meta"},
                timeout=5
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get asset metadata. Status code: {response.status_code}")
                return 2  # Default to 2 decimals
                
            data = response.json()
            
            # Find the asset in the universe
            for asset_meta in data.get("universe", []):
                if asset_meta.get("name") == asset:
                    return asset_meta.get("szDecimals", 2)
            
            return 2  # Default to 2 decimals if asset not found
            
        except Exception as e:
            logger.error(f"Error getting size decimals: {e}")
            return 2  # Default to 2 decimals on error
    
    def open_long(self, asset: str, usd_amount: float) -> Dict:
        """Open a long position with USD amount."""
        token_amount = self.usd_to_token_size(asset, usd_amount)
        if token_amount <= 0:
            return {"status": "error", "message": f"Invalid conversion for {asset}"}
            
        return self.place_market_order(
            symbol=asset,
            side="bid",
            quantity=token_amount
        )
    
    def open_short(self, asset: str, usd_amount: float) -> Dict:
        """Open a short position with USD amount."""
        token_amount = self.usd_to_token_size(asset, usd_amount)
        if token_amount <= 0:
            return {"status": "error", "message": f"Invalid conversion for {asset}"}
            
        return self.place_market_order(
            symbol=asset,
            side="ask",
            quantity=token_amount
        )

    def format_symbol(self, asset: str) -> str:
        """Format asset name to exchange-specific symbol format."""
        return asset  # Hyperliquid doesn't need special formatting

    def subscribe_to_funding_updates(self, callback: Callable) -> Any:
        """Subscribe to funding rate updates."""
        # Simple parse for funding events
        def funding_callback(data):
            if callback:
                callback(data)
                
        subscription = {"type": "userFundings", "user": self.address}
        # Configure logging for this subscription
        logging.getLogger("hyperliquid").setLevel(logging.WARNING)
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