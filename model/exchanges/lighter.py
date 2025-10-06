import lighter
import json
import os
import time
import asyncio
from model.exchanges.base import BaseExchange
from model.exchanges.lighter_ws import LighterWebSocketClient
from typing import Dict, List, Optional, Any, Callable, Tuple
from utils.config import CONFIG
from utils.logger import setup_logger
from lighter import SignerClient
from datetime import datetime

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
        
        # Cache for market decimals (sizeDecimal, priceDecimal)
        self._market_decimals_cache = {}
        
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
            
            if self.ws_client.is_connected():
                logger.debug("Waiting 3 seconds for order book updates to initialize")
                await asyncio.sleep(3)
                logger.debug("Order book updates should now be available")
            
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
                    logger.debug(f"Account Type: {account.account_type}")
                    logger.debug(f"Collateral: {account.collateral}")
                    logger.debug(f"Total Asset Value: {account.total_asset_value}")
                    logger.debug(f"Number of positions: {len(account.positions)}")
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

    async def _place_market_order_async(
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
            
            # Get market decimals for proper scaling
            decimals = await self._get_market_decimals(market_id)
            size_decimal = decimals['sizeDecimal']
            price_decimal = decimals['priceDecimal']

            # Determine the base amount and price; abort if unavailable
            estimated_price = await self._get_estimated_price(symbol)
            if not estimated_price or estimated_price <= 0:
                logger.warning(f"Could not estimate price for {symbol}; aborting order placement")
                return {"error": f"No price available for {symbol}; order aborted"}

            # Determine the base amount using proper sizeDecimal scaling
            if quote_quantity is not None:
                # Convert USD to base units via dynamic price, then scale by sizeDecimal
                base_amount_raw = quote_quantity / estimated_price
                base_amount = int(base_amount_raw * (10 ** size_decimal))
            elif quantity is not None:
                # If quantity is provided, scale by sizeDecimal
                base_amount = int(quantity * (10 ** size_decimal))
            else:
                return {"error": "Either quantity or quote_quantity must be provided"}
            
            # Ensure base_amount is at least 1 (minimum order size)
            if base_amount < 1:
                base_amount = 1
            
            # Convert side to is_ask format
            is_ask = side.upper() == "SELL"
            
            # Use SDK helper to constrain execution by slippage instead of arbitrary bounds
            max_slippage = float(CONFIG.get("LIGHTER_MAX_SLIPPAGE", 0.01))

            # Log order parameters for debugging
            logger.info(f"🔍 Order parameters:")
            logger.info(f"   Symbol: {symbol}, Side: {side}")
            logger.info(f"   Market ID: {market_id}")
            logger.info(f"   Base amount: {base_amount}")
            logger.info(f"   Size decimal: {size_decimal}")
            logger.info(f"   Price decimal: {price_decimal}")
            logger.info(f"   Max slippage: {max_slippage}")
            logger.info(f"   Is ask: {is_ask}")
            logger.info(f"   Reduce only: {reduce_only}")
            
            # Use limited slippage market order
            created, api_resp, err = await self.signer_client.create_market_order_limited_slippage(
                market_index=market_id,
                client_order_index=self._get_next_client_order_index(),
                base_amount=base_amount,
                max_slippage=max_slippage,
                is_ask=is_ask,
                reduce_only=reduce_only,
            )

            tx = (created, api_resp, err)
            
            logger.info(f"Lighter market order created: {tx}")
            return {"status": "success", "tx": tx}
            
        except Exception as e:
            logger.error(f"Failed to place market order: {e}")
            return {"error": f"Failed to place market order: {str(e)}"}

    async def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Optional[float] = None,
        quote_quantity: Optional[float] = None,
        reduce_only: bool = False
    ) -> Dict:
        """Place a market order."""
        # Use the existing async implementation
        return await self._place_market_order_async(
            symbol, side, quantity, quote_quantity, reduce_only
        )

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

    async def refresh_market_mapping(self, force: bool = False) -> Dict[int, str]:
        """Refresh market mapping from API if empty or force=True, then return it."""
        if force or not self._get_cached_market_mapping():
            await self._fetch_market_info()
        return self._get_cached_market_mapping()

    def get_market_mapping(self) -> Dict[int, str]:
        """Return current cached mapping of market_id -> symbol."""
        return dict(self._get_cached_market_mapping())

    def get_reverse_market_mapping(self) -> Dict[str, int]:
        """Return reverse mapping of symbol -> market_id (upper-cased symbols)."""
        mapping = self._get_cached_market_mapping()
        return {symbol.upper(): mid for mid, symbol in mapping.items()}

    def save_market_mapping(self, path: str = "data/lighter_markets.json") -> bool:
        """Persist current market mapping to disk for warm start."""
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                json.dump(self._get_cached_market_mapping(), f, indent=2)
            logger.info(f"Saved Lighter market mapping to {path}")
            return True
        except Exception as e:
            logger.warning(f"Failed to save market mapping to {path}: {e}")
            return False

    def load_market_mapping(self, path: str = "data/lighter_markets.json") -> bool:
        """Load market mapping from disk if available (does not call API)."""
        try:
            if os.path.exists(path):
                with open(path, "r") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    # keys might be strings when loaded from json; coerce to int
                    self._market_mapping_cache = {int(k): v for k, v in data.items()}
                    logger.info(f"Loaded Lighter market mapping from {path} ({len(self._market_mapping_cache)} markets)")
                    return True
        except Exception as e:
            logger.warning(f"Failed to load market mapping from {path}: {e}")
        return False

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
        # Robust resolution with retries and suffix handling
        await self._ensure_market_mapping_loaded()
        reverse_mapping = self.get_reverse_market_mapping()

        # Normalize inputs
        sym = symbol.upper()
        # Try direct
        mid = reverse_mapping.get(sym)
        if mid is not None:
            return mid
        # Try adding/removing -USD
        if sym.endswith("-USD"):
            alt = sym[:-4]
            mid = reverse_mapping.get(alt)
            if mid is not None:
                return mid
        else:
            alt = f"{sym}-USD"
            mid = reverse_mapping.get(alt)
            if mid is not None:
                return mid

        # As a last resort, force refresh and try again once
        await self.refresh_market_mapping(force=True)
        reverse_mapping = self.get_reverse_market_mapping()
        mid = reverse_mapping.get(sym) or reverse_mapping.get(sym[:-4] if sym.endswith("-USD") else f"{sym}-USD")
        if mid is not None:
            return mid

        # Default to BTC if truly unknown to avoid crashes
        logger.warning(f"Unknown symbol '{symbol}', defaulting to BTC market (1)")
        return 1

    async def subscribe_symbols(self, symbols: List[str]) -> bool:
        """Ensure WS is subscribed to the markets backing the provided symbols.

        Converts symbols to market_ids using the current mapping, refreshing if needed,
        and asks the WS client to resubscribe to those order books.
        """
        try:
            if not self.use_ws or not self.ws_client:
                logger.warning("WebSocket not enabled; cannot subscribe symbols")
                return False

            market_ids: List[int] = []
            for sym in symbols:
                try:
                    mid = await self._get_market_id(sym)
                    if mid is not None and mid not in market_ids:
                        market_ids.append(mid)
                except Exception as e:
                    logger.debug(f"Failed to resolve market id for {sym}: {e}")

            if not market_ids:
                logger.warning("No market ids resolved for subscription")
                return False

            # Ask WS client to resubscribe
            if hasattr(self.ws_client, "resubscribe_order_books"):
                ok = await self.ws_client.resubscribe_order_books(market_ids)
                logger.info(f"WS resubscribe to markets {market_ids}: {ok}")
                return ok
            else:
                # Fallback: reconnect with new ids by resetting property if supported
                try:
                    await self.ws_client.disconnect()
                    self.ws_client.order_book_ids = market_ids
                    await self.ws_client.connect()
                    logger.info(f"WS reconnected with order_book_ids={market_ids}")
                    return True
                except Exception as e:
                    logger.error(f"Failed to resubscribe WS to {market_ids}: {e}")
                    return False
        except Exception as e:
            logger.error(f"subscribe_symbols error: {e}")
            return False

    def _get_next_client_order_index(self) -> int:
        """Get next client order index for order tracking."""
        if not hasattr(self, '_client_order_counter'):
            self._client_order_counter = 0
        self._client_order_counter += 1
        return self._client_order_counter

    async def get_current_price(self, symbol: str) -> Optional[float]:
        """Get current price for a symbol from WebSocket only."""
        try:
            # If WebSocket is enabled, try to get from market stats
            if self.use_ws and self.ws_client and self.ws_client.is_connected():
                try:
                    market_id = await self._get_market_id(symbol)
                    latest_price = self.ws_client.get_latest_price(market_id)
                    if latest_price:
                        return latest_price
                except:
                    pass
            
            logger.warning(f"No WebSocket price data available for {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get current price for {symbol}: {e}")
            return None

    async def _get_market_decimals(self, market_id: int) -> Dict[str, int]:
        """Get supported_price_decimals and supported_size_decimals for a market from Lighter API.
        
        Returns:
            Dict with 'sizeDecimal' and 'priceDecimal' keys
        """
        if market_id in self._market_decimals_cache:
            return self._market_decimals_cache[market_id]
        
        try:
            # Use Lighter OrderApi to get order book details
            order_api = lighter.OrderApi(self.api_client)
            response = await order_api.order_book_details(market_id=market_id)
            
            # Extract decimals from the response
            if hasattr(response, 'order_book_details') and response.order_book_details:
                for detail in response.order_book_details:
                    if detail.market_id == market_id:
                        decimals = {
                            'sizeDecimal': int(detail.size_decimals),
                            'priceDecimal': int(detail.price_decimals)
                        }
                        
                        # Cache the result
                        self._market_decimals_cache[market_id] = decimals
                        logger.debug(f"Fetched decimals for market {market_id}: {decimals}")
                        
                        return decimals
            
            # If not found in API response, raise an error
            raise ValueError(f"Market {market_id} not found in API response")
            
        except Exception as e:
            logger.error(f"Failed to fetch decimals for market {market_id}: {e}")
            raise

    async def _get_estimated_price(self, symbol: str) -> Optional[float]:
        """Estimate current price for symbol without hardcoded constants.

        Tries WS latest price, then position entry price.
        Returns price in USD.
        """
        # Try WebSocket latest price
        try:
            ws_price = await self.get_current_price(symbol)
            if ws_price and ws_price > 0:
                return float(ws_price)
        except Exception:
            pass

        # Try position entry price if any
        try:
            balance = await self.get_real_balance()
            for p in balance.get("positions", []):
                if p.get("symbol") == symbol and float(p.get("entry_price", 0)) > 0:
                    return float(p["entry_price"])
        except Exception:
            pass

        return None

    async def get_real_balance(self) -> Dict:
        """Get real account balance from Lighter."""
        try:
            await self._ensure_account_initialized()
            
            # Get account details
            account_details = await self.account_api.account(by="index", value=str(self.account_index))
            
            if hasattr(account_details, 'accounts') and account_details.accounts:
                account = account_details.accounts[0]
                # Simplified free collateral calculation
                free_collateral = float(
                    getattr(account, 'free_collateral', None)
                    or getattr(account, 'available_balance', None)
                    or getattr(account, 'collateral', 0.0) * (
                        1.0 if not getattr(account, 'positions', None) or all(float(pos.position) == 0 for pos in account.positions)
                        else 0.1
                    )
                )
                balance_info = {
                    'account_index': self.account_index,
                    'account_type': account.account_type,
                    'collateral': float(account.collateral) if hasattr(account, 'collateral') else 0.0,
                    'total_asset_value': float(account.total_asset_value) if hasattr(account, 'total_asset_value') else 0.0,
                    'free_collateral': free_collateral,
                    'positions_count': len(account.positions) if hasattr(account, 'positions') else 0,
                    'timestamp': datetime.now().isoformat()
                }
                
                # Extract positions if available
                if hasattr(account, 'positions') and account.positions:
                    balance_info['positions'] = []
                    for position in account.positions:
                        pos_info = {
                            'symbol': position.symbol,
                            'size': float(position.position),
                            'side': 'long' if float(position.position) > 0 else 'short',
                            'entry_price': float(position.avg_entry_price),
                            'unrealized_pnl': float(position.unrealized_pnl),
                            'market_id': position.market_id
                        }
                        balance_info['positions'].append(pos_info)
                
                logger.debug(f"Real balance fetched: ${balance_info['total_asset_value']:.2f} total, ${balance_info['free_collateral']:.2f} free")
                return balance_info
            else:
                logger.warning("No account details found")
                return {'error': 'No account details found'}
                
        except Exception as e:
            logger.error(f"Failed to get real balance: {e}")
            return {'error': f'Failed to get real balance: {str(e)}'}

    # Unified balance/min-notional
    def get_available_usd(self, asset: Optional[str] = None) -> float:
        # Synchronous wrapper that uses the last known call if available
        # For simplicity, return 0 if not retrievable synchronously.
        return 0.0

    def get_min_notional_usd(self, asset: str) -> float:
        # Conservative default: $1
        return 1.0

