import base64
import time
from typing import Dict, Optional, Any, List, Callable
import urllib.parse
import requests
import nacl.signing
from utils.config import CONFIG
from model.exchanges.base import BaseExchange
from utils.logger import setup_logger
from model.exchanges.backpack_ws import BackpackWebSocketClient
import json

logger = setup_logger(__name__)

class BackpackExchange(BaseExchange):
    def __init__(self, use_ws: bool = False):
        proxy_url = CONFIG.get('PROXY_URL')
        self.proxies = {
            "http": proxy_url
        } if proxy_url else None
        
        # Initialize WebSocket client if use_ws is True
        self.use_ws = use_ws
        self.ws_client = BackpackWebSocketClient() if use_ws else None
    
    async def initialize_ws(self):
        """Initialize WebSocket connection if WS is enabled."""
        if self.use_ws and self.ws_client:
            await self.ws_client.connect()
            return self.ws_client.is_connected()
        return False
        
    async def close_ws(self):
        """Close the WebSocket connection if it exists."""
        if self.use_ws and self.ws_client:
            await self.ws_client.disconnect()
    
    def sign_request(self, instruction_type: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """Sign a request for Backpack API."""
        private_key_b64 = CONFIG.get("BACKPACK_PRIVATE_KEY")
        private_key = base64.b64decode(private_key_b64)
        signing_key = nacl.signing.SigningKey(private_key)
        
        # Set parameters for signing
        timestamp = int(time.time() * 1000)
        message = f"instruction={instruction_type}"
        
        # Add sorted parameters if provided
        if params:
            sorted_params = sorted(params.items(), key=lambda x: x[0])
            
            # Convert values to strings, ensuring booleans are lowercase "true"/"false"
            encoded_params = []
            for k, v in sorted_params:
                if isinstance(v, bool):
                    # Convert boolean to lowercase string as expected by API signature
                    encoded_params.append((k, str(v).lower())) 
                else:
                    # Ensure all other values are strings
                    encoded_params.append((k, str(v)))
            
            params_str = urllib.parse.urlencode(encoded_params)
            message += f"&{params_str}"
        
        window = CONFIG.get("BACKPACK_DEFAULT_WINDOW")
        message += f"&timestamp={timestamp}&window={window}"
        
        # Log the exact message being signed for debugging
        logger.debug(f"Message to sign: {message}")
        
        signature = signing_key.sign(message.encode())
        
        # Return headers
        return {
            "X-API-Key": CONFIG.get("BACKPACK_PUBLIC_KEY"),
            "X-Signature": base64.b64encode(signature.signature).decode(),
            "X-Timestamp": str(timestamp),
            "X-Window": str(window)
        }
    
    def get_funding_history(
        self,
        subaccount_id: Optional[int] = None,
        symbol: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        sort_direction: str = "Desc"
    ) -> Dict:
        """Get funding payment history for futures."""
        # Prepare query parameters
        params = {}
        if subaccount_id is not None:
            params["subaccountId"] = subaccount_id
        if symbol:
            params["symbol"] = symbol
        if limit != 100:
            params["limit"] = limit
        if offset != 0:
            params["offset"] = offset
        if sort_direction != "Desc":
            params["sortDirection"] = sort_direction
        
        # Sign the request
        headers = self.sign_request("fundingHistoryQueryAll")
        
        # Make the request
        url = f"{CONFIG.BACKPACK_API_URL_HISTORY_FUNDING}"
        if params:
            url += f"?{urllib.parse.urlencode(params)}"
        
        response = requests.get(url, headers=headers, proxies=self.proxies)
        return response.json()
    
    def get_funding_rates(self) -> Dict:
        """Get mark prices, index prices and funding rates."""
        return self.get_mark_prices()
    
    def get_mark_prices(self, symbol: Optional[str] = None) -> Dict:
        """Get mark prices, index prices and funding rates."""
        url = f"{CONFIG.BACKPACK_API_URL_MARK_PRICES}"
        
        # Add symbol parameter if provided
        if symbol:
            url += f"?symbol={symbol}_USDC_PERP"
        
        try:
            response = requests.get(url, proxies=self.proxies, timeout=10)  # Add 10 second timeout
            
            # Check response status and content before parsing JSON
            if response.status_code != 200:
                logger.error(f"API error: status code {response.status_code}, response: {response.text}")
                return {"error": f"API error: status code {response.status_code}", "response": response.text}
            
            if not response.text:
                logger.error("Empty response from API")
                return {"error": "Empty response from API", "status_code": response.status_code}
            
            # Try to parse JSON, handle decode errors explicitly
            try:
                data = response.json()
                logger.debug(f"Mark prices response: {data}")
                return data
            except json.JSONDecodeError as json_err:
                logger.error(f"JSON decode error: {json_err}, response text: {response.text}")
                return {"error": f"JSON decode error: {str(json_err)}", "response": response.text}
                
        except requests.Timeout:
            logger.error("Request to Backpack API timed out")
            return {"error": "Request timed out"}
        except requests.RequestException as e:
            logger.error(f"Request to Backpack API failed: {str(e)}")
            return {"error": f"Request failed: {str(e)}"}
        except Exception as e:
            logger.error(f"Unexpected error in get_mark_prices: {str(e)}")
            return {"error": f"Unexpected error: {str(e)}"}
    
    def get_positions(self) -> List[Dict]:
        """Get current positions."""
        headers = self.sign_request("positionQuery")
        url = f"{CONFIG.BACKPACK_API_URL_POSITION}"
        
        response = requests.get(url, headers=headers, proxies=self.proxies)
        
        # Check response status and content before parsing JSON
        if response.status_code != 200:
            return {"error": f"API error: status code {response.status_code}", "response": response.text}
        
        if not response.text:
            return {"error": "Empty response from API", "status_code": response.status_code}
            
        try:
            return response.json()
        except Exception as e:
            return {"error": f"Failed to parse JSON response: {str(e)}", "response": response.text}
    
    def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Optional[float] = None,
        quote_quantity: Optional[float] = None,
        reduce_only: bool = False,
        client_id: Optional[int] = None
    ) -> Dict:
        """Place a market order."""
        if not quantity and not quote_quantity:
            raise ValueError("Either quantity or quote_quantity must be provided")
        
        # Only add the suffix if it's not already there
        if not symbol.endswith("_USDC_PERP"):
            symbol = f"{symbol}_USDC_PERP"

        # Prepare request body
        payload = {
            "symbol": symbol,
            "side": side,
            "orderType": "Market",
            "reduceOnly": reduce_only
        }
        
        # Add either quantity or quoteQuantity
        if quantity:
            payload["quantity"] = str(quantity)
        if quote_quantity:
            payload["quoteQuantity"] = str(quote_quantity)
        
        # Add client ID if provided
        if client_id is not None:
            payload["clientId"] = client_id
            
        # Sign the request
        headers = self.sign_request("orderExecute", params=payload)
        headers["Content-Type"] = "application/json"
        
        # Make the request
        url = f"{CONFIG.BACKPACK_API_URL_ORDER}"
        
        response = requests.post(url, headers=headers, json=payload, proxies=self.proxies)
        
        # Check response status and content before parsing JSON
        if response.status_code != 200:
            return {"error": f"API error: status code {response.status_code}", "response": response.text}
        
        if not response.text:
            return {"error": "Empty response from API", "status_code": response.status_code}
            
        try:
            return response.json()
        except Exception as e:
            return {"error": f"Failed to parse JSON response: {str(e)}", "response": response.text}
    
    def close_position(
        self,
        symbol: str,
        position_size: Optional[float] = None,
        client_id: Optional[int] = None
    ) -> Dict:
        """Close an existing position."""
        logger.debug(f"Attempting to close position for {symbol}, position_size={position_size}")
        
        if position_size is None:
            # Get positions to find the size
            positions = self.get_positions()
            
            if not isinstance(positions, list):
                logger.error(f"Failed to get positions: {positions}")
                return {"error": "Failed to get positions", "details": positions}
                
            logger.debug(f"Current positions: {positions}")
            position_found = False
            
            for position in positions:
                if position.get("symbol") == symbol:
                    position_found = True
                    
                    # Check multiple possible fields for position size
                    if "netQuantity" in position:
                        position_size = float(position.get("netQuantity", "0"))
                        logger.debug(f"Using netQuantity field for position size: {position_size}")
                    elif "positionSize" in position:
                        position_size = float(position.get("positionSize", "0"))
                        logger.debug(f"Using positionSize field for position size: {position_size}")
                    else:
                        # Log all available keys to help diagnose
                        logger.warning(f"Could not find size field. Available keys: {list(position.keys())}")
                        position_size = 0
                    
                    logger.debug(f"Found position for {symbol} with size {position_size}")
                    break
            
            if not position_found:
                logger.warning(f"No position found for symbol {symbol}")
                return {"error": "No position found for symbol", "symbol": symbol}
                
            if position_size is None or position_size == 0:
                logger.warning(f"Position found for {symbol} but size is 0, nothing to close")
                return {"error": "No position found for symbol", "symbol": symbol}
        
        # Determine side based on position direction
        side = "Ask" if position_size > 0 else "Bid"
        
        # Use absolute value for quantity
        quantity = abs(position_size)
        
        logger.debug(f"Placing order to close position: symbol={symbol}, side={side}, quantity={quantity}")
        
        return self.place_market_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
            reduce_only=True,
            client_id=client_id
        )
    
    def close_all_positions(self) -> Dict[str, Any]:
        """Close all open positions."""
        positions = self.get_positions()
        
        results = {}
        for position in positions:
            symbol = position.get("symbol")
            size = float(position.get("positionSize", "0"))
            
            # Skip if position size is 0
            if size == 0:
                continue
                
            # Close the position
            result = self.close_position(symbol, size)
            results[symbol] = result
        
        return results
    
    def format_symbol(self, asset: str) -> str:
        """Format asset name to exchange-specific symbol format."""
        return f"{asset}_USDC_PERP"
    
    def open_long(self, asset: str, amount_usd: float) -> Dict:
        """Open a long position for the specified asset."""
        symbol = self.format_symbol(asset)
        return self.place_market_order(
            symbol=symbol,
            side="Bid",
            quote_quantity=amount_usd
        )
    
    def open_short(self, asset: str, amount_usd: float) -> Dict:
        """Open a short position for the specified asset."""
        symbol = self.format_symbol(asset)
        return self.place_market_order(
            symbol=symbol,
            side="Ask",
            quote_quantity=amount_usd
        )
    
    def close_asset_position(self, asset: str) -> Optional[Dict]:
        """Close position for a specific asset."""
        symbol = self.format_symbol(asset)
        
        # Get positions
        positions = self.get_positions()
        logger.info(f"Positions: {positions}")
        
        # First check if positions is a list (as expected)
        if not isinstance(positions, list):
            logger.error(f"Expected positions to be a list, got {type(positions)}: {positions}")
            return positions  # Return the error
        
        for position in positions:
            if position.get("symbol") == symbol:
                # Check multiple possible fields for position size
                size = None
                if "netQuantity" in position:
                    size = float(position.get("netQuantity", "0"))
                    logger.info(f"Using netQuantity field for position size: {size}")
                elif "positionSize" in position:
                    size = float(position.get("positionSize", "0"))
                    logger.info(f"Using positionSize field for position size: {size}")
                else:
                    # Log all available keys to help diagnose
                    logger.warning(f"Could not find size field. Available keys: {list(position.keys())}")
                    size = 0
                
                if size != 0:
                    logger.info(f"Found position for {symbol} with size {size}, closing...")
                    return self.close_position(symbol, size)
                else:
                    logger.warning(f"Position found for {symbol} but size is 0, nothing to close")
        
        logger.warning(f"No position found for {symbol}")
        return None
        
    def subscribe_to_funding_updates(self, callback: Callable) -> Any:
        """Subscribe to funding rate updates using WebSocket."""
        if not self.use_ws or not self.ws_client:
            logger.warning("WebSocket subscription requested but WebSocket is not enabled. Set use_ws=True when initializing.")
            return None
            
        # Create an async subscription to the mark price stream
        async def subscribe_async():
            if not self.ws_client.is_connected():
                logger.info("WebSocket not connected, connecting now...")
                await self.ws_client.connect()
                
            # Subscribe to the mark price stream
            stream_name = "markPrice" # Appropriate stream name for funding/mark price updates
            await self.ws_client.subscribe(stream_name, callback)
            return True
            
        # Return the coroutine for the caller to schedule
        return subscribe_async()

    def process_funding_rates(self, mark_prices: List[Dict]) -> Dict[str, Dict]:
        """Convert Backpack mark prices to a normalized funding rate dict."""
        result = {}
        
        # Check if we received an error response
        if isinstance(mark_prices, dict) and "error" in mark_prices:
            logger.error(f"Error in mark prices data: {mark_prices}")
            return result
            
        # Check if we received a valid list of market data
        if not isinstance(mark_prices, list):
            logger.error(f"Unexpected format for mark_prices: {type(mark_prices)}, content: {mark_prices}")
            return result
            
        for item in mark_prices:
            try:
                symbol = item.get("symbol", "").split("_")[0]  
                result[symbol] = {
                    "rate": float(item.get("fundingRate", "0")),
                    "next_funding_time": item.get("nextFundingTimestamp", 0),
                    "exchange": "Backpack",
                    "mark_price": float(item.get("markPrice", "0")),
                    "index_price": float(item.get("indexPrice", "0"))
                }
            except Exception as e:
                logger.error(f"Error processing mark price item: {e}, item: {item}")
                
        return result 
