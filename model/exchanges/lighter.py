import lighter
from model.exchanges.base import BaseExchange
from model.exchanges.lighter_ws import LighterWebSocketClient
from typing import Dict, List, Optional, Any, Callable, Tuple
from utils.config import CONFIG
from utils.logger import setup_logger

logger = setup_logger(__name__)

class LighterExchange(BaseExchange):
    def __init__(self, use_ws: bool = False, order_book_ids: List[int] = None):
        super().__init__()
        
        # Initialize API client
        self.api_client = lighter.ApiClient(
            configuration=lighter.Configuration(host=CONFIG.LIGHTER_API_URL)
        )
        self.account_api = lighter.AccountApi(self.api_client)

        # Initialize WebSocket client if use_ws is True
        self.use_ws = use_ws
        if use_ws:
            self.ws_client = LighterWebSocketClient(
                order_book_ids=order_book_ids or [0, 1], 
                account_ids=[1, 2]
            )
        else:
            self.ws_client = None
        
        # Store latest order books
        self._order_books = {}
        
        # Initialize credentials
        self.account_index = None
        self.signer_client = None
        self.transaction_api = None
        self.api_private_key = None
        self.api_public_key = None
        
        # Note: Account initialization will be done lazily when needed
        # This follows the same pattern as BackpackExchange

    async def initialize_ws(self):
        """Initialize WebSocket connection if WS is enabled."""
        if self.use_ws and self.ws_client:
            # Ensure market mapping is loaded before initializing WebSocket
            await self._ensure_market_mapping_loaded()
            
            # Update WebSocket client with market mapping
            if hasattr(self, '_market_mapping_cache'):
                self.ws_client.set_market_mapping(self._market_mapping_cache)
            
            await self.ws_client.connect()
            return self.ws_client.is_connected()
        return False
        
    async def close_ws(self):
        """Close the WebSocket connection if it exists."""
        if self.use_ws and self.ws_client:
            await self.ws_client.disconnect()
    
    async def _ensure_account_initialized(self):
        """Ensure account is initialized (lazy initialization)"""
        if self.account_index is None:
            await self._initialize_account()
    
    async def _initialize_account(self):
        """Initialize account for Lighter exchange using mainnet"""
        try:
            # Get configuration
            BASE_URL = CONFIG.LIGHTER_API_URL
            API_KEY_INDEX = CONFIG.LIGHTER_API_KEY_INDEX
            
            logger.info(f"Initializing Lighter for mainnet: {BASE_URL}")
            logger.info(f"API Key Index: {API_KEY_INDEX}")

            # Get account information using the account API
            # First, get account details to find the L1 address
            account_response = await self.account_api.account(by="index", value="1")  # Start with index 1
            
            if not hasattr(account_response, 'accounts') or not account_response.accounts:
                raise Exception("No account found with index 1")
            
            # Get the L1 address from the first account
            account = account_response.accounts[0]
            l1_address = account.l1_address
            
            logger.info(f"Found L1 address: {l1_address}")
            
            # Now get account by L1 address to get the correct account index
            try:
                response = await self.account_api.accounts_by_l1_address(l1_address=l1_address)
            except lighter.ApiException as e:
                if e.data.message == "account not found":
                    logger.error(f"Account not found for {l1_address}")
                    raise Exception(f"Account not found for {l1_address}. Please ensure your account exists on Lighter mainnet.")
                else:
                    raise e

            if len(response.sub_accounts) > 1:
                logger.warning(f"Found multiple account indexes: {len(response.sub_accounts)}")
                for sub_account in response.sub_accounts:
                    logger.info(f"Account index: {sub_account.index}")
                # Use the first account index instead of raising an exception
                self.account_index = response.sub_accounts[0].index
                logger.info(f"Using first account index: {self.account_index}")
            else:
                self.account_index = response.sub_accounts[0].index
                logger.info(f"Account index: {self.account_index}")

            # Get account details using the correct account index
            account_details = await self.account_api.account(by="index", value=str(self.account_index))
            logger.info(f"Account details retrieved successfully")
            
            # Log account information
            if hasattr(account_details, 'accounts') and account_details.accounts:
                account = account_details.accounts[0]
                logger.info(f"Account Type: {account.account_type}")
                logger.info(f"Collateral: {account.collateral}")
                logger.info(f"Total Asset Value: {account.total_asset_value}")
                logger.info(f"Number of positions: {len(account.positions)}")
            
            # Initialize TransactionApi for future use
            self.transaction_api = lighter.TransactionApi(self.api_client)
            
            logger.info("✅ Lighter initialization successful!")

        except Exception as e:
            logger.error(f"Failed to initialize Lighter account: {e}")
            raise

    async def get_funding_rates(self) -> Dict:
        """Get current funding rates from Lighter exchange via FundingApi."""
        try:
            # Create funding API instance
            funding_api = lighter.FundingApi(self.api_client)
            
            # Get funding rates
            funding_rates_response = await funding_api.funding_rates()
            
            # Process funding rates into normalized format
            result = {}
            if hasattr(funding_rates_response, 'funding_rates') and funding_rates_response.funding_rates:
                for rate in funding_rates_response.funding_rates:
                    try:
                        market_id = rate.market_id
                        symbol = await self._get_symbol_by_market_id(market_id)
                        result[symbol] = {
                            "rate": float(rate.rate) if rate.rate else 0,
                            "exchange": rate.exchange,
                            "symbol": rate.symbol,
                            "market_id": market_id
                        }
                    except Exception as e:
                        logger.error(f"Error processing funding rate for market {rate.market_id}: {e}")
            
            logger.info(f"Retrieved funding rates for {len(result)} markets")
            return result
            
        except Exception as e:
            logger.error(f"Failed to get funding rates from Lighter: {e}")
            return {"error": f"Failed to get funding rates: {str(e)}"}

    def process_funding_rates(self, raw_data: Dict) -> Dict[str, Dict]:
        """Process raw funding rates into a normalized format."""
        result = {}
        
        try:
            # Check if we received an error response
            if isinstance(raw_data, dict) and "error" in raw_data:
                logger.error(f"Error in funding rates data: {raw_data}")
                return result
                
            # Process the raw data into normalized format
            # This should match the format used by other exchanges
            for symbol, data in raw_data.items():
                try:
                    result[symbol] = {
                        "rate": float(data.get("rate", 0)),
                        "next_funding_time": data.get("next_funding_time", 0),
                        "exchange": "Lighter",
                        "mark_price": float(data.get("mark_price", 0)),
                        "index_price": float(data.get("index_price", 0))
                    }
                except Exception as e:
                    logger.error(f"Error processing funding rate for {symbol}: {e}")
                    
        except Exception as e:
            logger.error(f"Error processing funding rates: {e}")
            
        return result

    async def get_positions(self) -> List[Dict]:
        """Get current positions from Lighter."""
        try:
            await self._ensure_account_initialized()
            
            # Get account details which include positions
            account_details = await self.account_api.account(by="index", value=str(self.account_index))
            
            positions = []
            if hasattr(account_details, 'accounts') and account_details.accounts:
                account = account_details.accounts[0]
                for position in account.positions:
                    if float(position.position) != 0:  # Only include non-zero positions
                        positions.append({
                            "symbol": position.symbol,
                            "size": float(position.position),
                            "side": "long" if float(position.position) > 0 else "short",
                            "entry_price": float(position.avg_entry_price),
                            "unrealized_pnl": float(position.unrealized_pnl),
                            "realized_pnl": float(position.realized_pnl),
                            "market_id": position.market_id
                        })
            
            return positions
            
        except Exception as e:
            logger.error(f"Failed to get positions from Lighter: {e}")
            return []

    async def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Optional[float] = None,
        quote_quantity: Optional[float] = None,
        reduce_only: bool = False
    ) -> Dict:
        """Place a market order on Lighter."""
        try:
            await self._ensure_account_initialized()
            
            if not self.signer_client:
                return {"error": "SignerClient not initialized. Cannot place orders."}
                
            # Get next nonce
            next_nonce = await self.transaction_api.next_nonce(
                account_index=self.account_index,
                api_key_index=CONFIG.LIGHTER_API_KEY_INDEX
            )
            
            # Convert symbol to market_id
            market_id = await self._get_market_id(symbol)
            
            # Convert side to Lighter format
            order_side = lighter.ORDER_SIDE_BUY if side.upper() == "BUY" else lighter.ORDER_SIDE_SELL
            
            # Convert quantity to base_amount (smallest unit)
            base_amount = int(quantity * 1e6) if quantity else 0  # Adjust precision as needed
            
            # Sign the order
            signed_tx = self.signer_client.sign_create_order(
                market_id=market_id,
                side=order_side,
                order_type=lighter.ORDER_TYPE_MARKET,
                time_in_force=lighter.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                base_amount=base_amount,
                price=0,  # Market order doesn't need price
                client_order_index=self._get_next_client_order_index(),
                nonce=next_nonce
            )
            
            # Send the transaction
            response = await self.transaction_api.send_tx(signed_tx)
            return response
            
        except Exception as e:
            logger.error(f"Failed to place market order: {e}")
            return {"error": f"Failed to place market order: {str(e)}"}

    async def close_position(self, symbol: str) -> Dict:
        """Close position for a specific symbol."""
        try:
            await self._ensure_account_initialized()
            
            # Get current positions
            positions = await self.get_positions()
            
            # Find the position for this symbol
            position_size = None
            for position in positions:
                if position.get("symbol") == symbol:
                    position_size = position.get("size", 0)
                    break
            
            if position_size is None or position_size == 0:
                return {"error": f"No position found for {symbol}"}
            
            # Determine side based on position direction
            side = "SELL" if position_size > 0 else "BUY"
            
            # Use absolute value for quantity
            quantity = abs(position_size)
            
            return await self.place_market_order(
                symbol=symbol,
                side=side,
                quantity=quantity
            )
            
        except Exception as e:
            logger.error(f"Failed to close position for {symbol}: {e}")
            return {"error": f"Failed to close position: {str(e)}"}

    async def open_long(self, asset: str, amount: float) -> Dict:
        """Open a long position."""
        return await self.place_market_order(symbol=asset, side="BUY", quantity=amount)

    async def open_short(self, asset: str, amount: float) -> Dict:
        """Open a short position."""
        return await self.place_market_order(symbol=asset, side="SELL", quantity=amount)

    def format_symbol(self, asset: str) -> str:
        """Format asset symbol for Lighter."""
        return f"{asset}-USD"

    def subscribe_to_funding_updates(self, callback: Callable) -> Any:
        """Subscribe to funding rate updates."""
        if not self.use_ws or not self.ws_client:
            logger.warning("WebSocket subscription requested but WebSocket is not enabled. Set use_ws=True when initializing.")
            return None
            
        # Implementation for funding rate subscription via WebSocket
        # This would need to be implemented based on Lighter's WebSocket API
        logger.info("Funding rate subscription not yet implemented for Lighter")
        return None

    async def get_account(self):
        """Get account information"""
        try:
            await self._ensure_account_initialized()
            return await self.account_api.account(by="index", value=str(self.account_index))
        except Exception as e:
            logger.error(f"Failed to get account: {e}")
            raise

    async def get_account_by_l1_address(self, l1_address: str):
        """Get account by L1 address"""
        try:
            return await self.account_api.accounts_by_l1_address(l1_address=l1_address)
        except Exception as e:
            logger.error(f"Failed to get account by L1 address: {e}")
            raise

    async def get_account_limits(self):
        """Get account limits"""
        try:
            await self._ensure_account_initialized()
            return await self.account_api.account_limits(account_index=self.account_index)
        except Exception as e:
            logger.error(f"Failed to get account limits: {e}")
            raise

    async def get_pnl(self, resolution: str = "1h", count_back: int = 24):
        """Get PnL data"""
        try:
            await self._ensure_account_initialized()
            import time
            current_time = int(time.time() * 1000)  # Current time in milliseconds
            start_time = current_time - (24 * 60 * 60 * 1000)  # 24 hours ago
            
            return await self.account_api.pnl(
                by="index",
                value=str(self.account_index),
                resolution=resolution,
                start_timestamp=start_time,
                end_timestamp=current_time,
                count_back=count_back
            )
        except Exception as e:
            logger.error(f"Failed to get PnL: {e}")
            raise

    async def get_public_pools(self, filter: str = "all", limit: int = 10, index: int = 0):
        """Get public pools"""
        try:
            return await self.account_api.public_pools(filter=filter, limit=limit, index=index)
        except Exception as e:
            logger.error(f"Failed to get public pools: {e}")
            raise

    async def close(self):
        """Close all connections"""
        try:
            if hasattr(self, 'signer_client') and self.signer_client:
                await self.signer_client.close()
            if hasattr(self, 'api_client') and self.api_client:
                await self.api_client.close()
            if self.ws_client:
                await self.ws_client.close()
        except Exception as e:
            logger.error(f"Error closing connections: {e}")

    def get_order_book(self, market_id: str) -> Dict:
        """Get order book for a market"""
        # Implementation for getting order book
        pass

    async def get_all_markets(self) -> Dict[int, str]:
        """Get all available markets with their IDs and symbols"""
        return await self._fetch_market_info()

    def get_market_count(self) -> int:
        """Get the total number of markets"""
        markets = self._get_cached_market_mapping()
        return len(markets)

    def list_all_markets(self) -> List[Tuple[int, str]]:
        """List all markets as (market_id, symbol) tuples"""
        mapping = self._get_cached_market_mapping()
        return sorted(mapping.items())

    async def _ensure_market_mapping_loaded(self):
        """Ensure market mapping is loaded from API"""
        if not self._get_cached_market_mapping():
            await self._fetch_market_info()

    def _get_cached_market_mapping(self) -> Dict[int, str]:
        """Get cached market mapping or fetch if not available"""
        if not hasattr(self, '_market_mapping_cache'):
            # Initialize with empty cache - will be populated on first API call
            self._market_mapping_cache = {}
        return self._market_mapping_cache

    async def _fetch_market_info(self) -> Dict[int, str]:
        """Dynamically fetch market information from the API"""
        try:
            # Create funding API instance
            funding_api = lighter.FundingApi(self.api_client)
            
            # Get funding rates to discover all markets
            funding_rates_response = await funding_api.funding_rates()
            
            # Extract unique market information
            markets = {}
            if hasattr(funding_rates_response, 'funding_rates') and funding_rates_response.funding_rates:
                for rate in funding_rates_response.funding_rates:
                    market_id = rate.market_id
                    symbol = rate.symbol
                    
                    if market_id not in markets:
                        markets[market_id] = symbol
            
            # Cache the results
            self._market_mapping_cache = markets
            
            logger.info(f"Discovered {len(markets)} markets from Lighter API")
            return markets
            
        except Exception as e:
            logger.error(f"Failed to fetch market info: {e}")
            # Return empty dict if API call fails
            return {}

    async def _get_symbol_by_market_id(self, market_id: int) -> str:
        """Get symbol name by market ID"""
        await self._ensure_market_mapping_loaded()
        mapping = self._get_cached_market_mapping()
        return mapping.get(market_id, f"UNKNOWN_{market_id}")

    async def _get_market_id(self, symbol: str) -> int:
        """Get market ID for a symbol"""
        await self._ensure_market_mapping_loaded()
        mapping = self._get_cached_market_mapping()
        # Create reverse mapping
        reverse_mapping = {v: k for k, v in mapping.items()}
        return reverse_mapping.get(symbol.upper(), 1)  # Default to BTC (market 1) if not found

    def _get_next_client_order_index(self) -> int:
        """Get next client order index"""
        # Simple implementation - you might want to use a more sophisticated approach
        import time
        return int(time.time() * 1000)

