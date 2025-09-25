import lighter
from model.exchanges.base import BaseExchange
from model.exchanges.lighter_ws import LighterWebSocketClient
from typing import Dict, List, Optional, Any, Callable, Tuple
from utils.config import CONFIG
from utils.logger import setup_logger
from lighter import SignerClient

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
    
    def _is_valid_private_key(self, private_key: str) -> bool:
        """Validate private key format for Lighter."""
        try:
            # Remove '0x' prefix if present
            if private_key.startswith('0x'):
                private_key = private_key[2:]
            
            # Check if it's a placeholder value
            if private_key.lower() in ['your_private_key_here', 'placeholder', '']:
                return False
            
            # Must be valid hex
            int(private_key, 16)

            # Validate decoded byte length explicitly (SignerClient expects 32 bytes)
            try:
                key_bytes = bytes.fromhex(private_key)
            except ValueError:
                return False

            # SignerClient expects 40 bytes (80 hex chars)
            return len(key_bytes) == 40
            
        except (ValueError, TypeError):
            return False

    async def _initialize_account(self):
        """Initialize account for Lighter exchange using mainnet"""
        try:
            # Get configuration
            BASE_URL = CONFIG.LIGHTER_API_URL
            API_KEY_INDEX = CONFIG.LIGHTER_API_KEY_INDEX
            
            logger.info(f"Initializing Lighter for mainnet: {BASE_URL}")
            logger.info(f"API Key Index: {API_KEY_INDEX}")

            # Use the account index from config directly
            self.account_index = CONFIG.LIGHTER_ACCOUNT_INDEX
            logger.info(f"Using configured account index: {self.account_index}")
            
            # Get account details using the account index
            try:
                account_details = await self.account_api.account(by="index", value=str(self.account_index))
                logger.info(f"Account details retrieved successfully")
                
                # Log account information
                if hasattr(account_details, 'accounts') and account_details.accounts:
                    account = account_details.accounts[0]
                    logger.info(f"Account Type: {account.account_type}")
                    logger.info(f"Collateral: {account.collateral}")
                    logger.info(f"Total Asset Value: {account.total_asset_value}")
                    logger.info(f"Number of positions: {len(account.positions)}")
            except Exception as e:
                logger.warning(f"Could not retrieve account details (API compatibility issue): {e}")
                logger.info("Continuing with SignerClient initialization...")
            
            # Initialize TransactionApi for future use
            self.transaction_api = lighter.TransactionApi(self.api_client)
            
            # Initialize SignerClient for order placement
            # Note: This requires private key configuration
            try:
                # Get private key from environment (you'll need to set this)
                private_key = CONFIG.get('LIGHTER_PRIVATE_KEY')
                if private_key:
                    logger.info(f"Raw private key from config: {private_key[:10]}... (length: {len(private_key)})")
                    
                    # Validate private key format
                    if not self._is_valid_private_key(private_key):
                        logger.error("Invalid private key format - must be 40 bytes (80 hex characters)")
                        self.signer_client = None
                    else:
                        try:
                            # Use the private key directly (SignerClient expects 40 bytes)
                            self.signer_client = SignerClient(
                                url=CONFIG.LIGHTER_API_URL,
                                private_key=private_key,
                                account_index=CONFIG.LIGHTER_ACCOUNT_INDEX,
                                api_key_index=CONFIG.LIGHTER_API_KEY_INDEX
                            )
                            logger.info("Lighter SignerClient initialized successfully")
                        except Exception as e:
                            logger.error(f"SignerClient initialization failed: {e}")
                            self.signer_client = None
                else:
                    logger.warning("LIGHTER_PRIVATE_KEY not configured - order placement will be disabled")
                    self.signer_client = None
            except Exception as e:
                logger.warning(f"Could not initialize SignerClient: {e} - order placement will be disabled")
                self.signer_client = None
            
            logger.info("Lighter initialization successful!")

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
            
            # Convert symbol to market_id
            market_id = await self._get_market_id(symbol)
            
            # Get current price for the symbol
            current_price = await self._get_current_price(symbol)
            if not current_price or current_price <= 0:
                return {"error": f"Could not get current price for {symbol}"}
            
            # Determine the base amount and price based on dynamic estimate
            if quote_quantity is not None:
                # Convert USD to base units via dynamic price
                base_amount = int((quote_quantity / current_price) * self._get_base_scale(symbol))
            elif quantity is not None:
                # If quantity is provided, convert to base amount
                base_amount = int(quantity * self._get_base_scale(symbol))
            else:
                return {"error": "Either quantity or quote_quantity must be provided"}
            
            # Ensure base_amount is at least 1 (minimum order size)
            if base_amount < 1:
                base_amount = 1
            
            # Convert side to is_ask format
            is_ask = side.upper() == "SELL"
            
            # Calculate avg_execution_price in micro units (price * 1e6)
            avg_execution_price = int(current_price * 1e6)
            
            # Log order parameters for debugging
            logger.info(f"Order parameters:")
            logger.info(f"   Symbol: {symbol}, Side: {side}")
            logger.info(f"   Market ID: {market_id}")
            logger.info(f"   Current Price: ${current_price}")
            logger.info(f"   Base amount: {base_amount}")
            logger.info(f"   Avg execution price: {avg_execution_price}")
            logger.info(f"   Is ask: {is_ask}")
            logger.info(f"   Reduce only: {reduce_only}")
            
            # Create market order using the correct method
            tx = await self.signer_client.create_order(
                market_index=market_id,
                client_order_index=self._get_next_client_order_index(),
                base_amount=base_amount,
                price=avg_execution_price,
                is_ask=is_ask,
                order_type=lighter.SignerClient.ORDER_TYPE_MARKET,
                time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=reduce_only,
                order_expiry=lighter.SignerClient.DEFAULT_IOC_EXPIRY
            )
            
            logger.info(f"Lighter market order created: {tx}")
            return {"status": "success", "tx": tx}
            
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
                quantity=quantity,
                reduce_only=True
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
            
        # Set the callback for market stats updates
        self.ws_client.on_market_stats_update = callback
        logger.info("Lighter funding rate subscription configured")
        return True

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

    async def _get_current_price(self, symbol: str) -> Optional[float]:
        """Get current price for a symbol from WebSocket or fallback to API."""
        try:
            # Try WebSocket first if available
            if self.use_ws and self.ws_client and self.ws_client.is_connected():
                try:
                    market_id = await self._get_market_id(symbol)
                    latest_price = self.ws_client.get_latest_price(market_id)
                    if latest_price and latest_price > 0:
                        logger.debug(f"Got price from WebSocket for {symbol}: ${latest_price}")
                        return latest_price
                except Exception as e:
                    logger.debug(f"WebSocket price fetch failed for {symbol}: {e}")
            
            # Fallback: try to get from positions (entry price)
            try:
                balance = await self.get_real_balance()
                for position in balance.get("positions", []):
                    if position.get("symbol") == symbol:
                        entry_price = float(position.get("entry_price", 0))
                        if entry_price > 0:
                            logger.debug(f"Got price from position entry for {symbol}: ${entry_price}")
                            return entry_price
            except Exception as e:
                logger.debug(f"Position price fetch failed for {symbol}: {e}")
            
            # Last resort: use reasonable defaults based on symbol
            default_prices = {
                "BTC": 50000.0,
                "ETH": 3000.0,
                "SOL": 100.0,
                "DOGE": 0.08,
                "LINK": 15.0,
                "TRUMP": 0.5,
                "FARTCOIN": 0.001,
                "HYPE": 0.1,
                "KAITO": 0.05,
                "IP": 0.02,
                "YZY": 0.01,
                "ASTER": 0.005,
                "1000PEPE": 0.00001
            }
            
            default_price = default_prices.get(symbol.upper())
            if default_price:
                logger.warning(f"Using default price for {symbol}: ${default_price}")
                return default_price
            
            logger.error(f"Could not get current price for {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get current price for {symbol}: {e}")
            return None

    def _get_base_scale(self, symbol: str) -> int:
        """Return base unit scale per market.
        
        Different markets have different precision requirements.
        This should match the actual market configuration on Lighter.
        """
        # Conservative scaling based on typical market requirements
        scale_mapping = {
            "BTC": int(1e5),      # 100,000 units per BTC
            "ETH": int(1e6),      # 1,000,000 units per ETH  
            "SOL": int(1e6),      # 1,000,000 units per SOL
            "DOGE": int(1e6),     # 1,000,000 units per DOGE
            "LINK": int(1e6),     # 1,000,000 units per LINK
            "TRUMP": int(1e6),    # 1,000,000 units per TRUMP
            "FARTCOIN": int(1e6), # 1,000,000 units per FARTCOIN
            "HYPE": int(1e6),     # 1,000,000 units per HYPE
            "KAITO": int(1e6),    # 1,000,000 units per KAITO
            "IP": int(1e6),       # 1,000,000 units per IP
            "YZY": int(1e6),      # 1,000,000 units per YZY
            "ASTER": int(1e6),    # 1,000,000 units per ASTER
            "1000PEPE": int(1e6), # 1,000,000 units per 1000PEPE
        }
        return scale_mapping.get(symbol.upper(), int(1e6))  # Default to 1e6

