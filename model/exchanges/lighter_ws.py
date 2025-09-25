import asyncio
import json
from typing import Callable, List, Dict, Optional
from utils.logger import setup_logger
import lighter
from lighter import WsClient
from websockets.client import connect as _ws_connect_async

logger = setup_logger(__name__)


class _PatchedWsClient(WsClient):
    """Subclass of Lighter WsClient that disables protocol-level pings.

    Lighter expects application-level ping/pong messages rather than
    WebSocket control frame pings. We set ping_interval=None so the client
    does not send periodic pings that the server will not answer.
    """

    async def run_async(self):
        ws = await _ws_connect_async(self.base_url, ping_interval=None, ping_timeout=None)
        self.ws = ws

        async for message in ws:
            await self.on_message_async(ws, message)


class LighterWebSocketClient:
    def __init__(self, account_ids: List[int] = None,
                 on_account_update: Callable = None, 
                 on_order_book_update: Callable = None, 
                 market_mapping: Dict[int, str] = None,
                 order_book_ids: List[int] = None):
        
        # Provide default subscriptions to avoid "No subscriptions provided" error
        if account_ids is None:
            account_ids = [1]  # Default account ID
        if order_book_ids is None:
            order_book_ids = [1, 2, 3]  # Default market IDs
        
        self.account_ids = account_ids
        self.order_book_ids = order_book_ids
        
        # Store latest market data
        self._market_stats = {}
        self._market_mapping = market_mapping or {}
        
        # Callbacks
        self.on_account_update = on_account_update or self._default_account_handler
        self.on_order_book_update = on_order_book_update or self._default_order_book_handler
        
        # Lighter SDK WsClient
        self.ws_client = None
        self.ws_task = None
        self._connected = False
        self._stop_event = asyncio.Event()

    async def connect(self):
        """Connect to the WebSocket server using Lighter SDK WsClient."""
        if self._connected:
            return True

        try:
            logger.info(f"🔌 Creating WebSocket client with account_ids: {self.account_ids}, order_book_ids: {self.order_book_ids}")
            
            # Create Lighter SDK WsClient with proper error handling
            # Use a patched client that handles app-level ping/pong and disables protocol pings
            self.ws_client = _PatchedWsClient(
                account_ids=self.account_ids,
                order_book_ids=self.order_book_ids,
                on_account_update=self._handle_account_update,
                on_order_book_update=self._handle_order_book_update
            )
            
            # Override the handle_unhandled_message method to prevent errors
            self.ws_client.handle_unhandled_message = self._handle_unhandled_message
            
            # Start the WebSocket client in a task to handle errors
            try:
                # Note: run_async() is blocking, so we'll run it in a task
                self.ws_task = asyncio.create_task(self._run_websocket())
                self._connected = True
                logger.info("✅ Lighter WebSocket connected successfully using SDK")
                
                # Wait a bit to see if connection is stable
                await asyncio.sleep(2)
                
                # Log WebSocket client state
                logger.info(f"🔍 WebSocket client state after connection:")
                logger.info(f"🔍 - Client exists: {self.ws_client is not None}")
                logger.info(f"🔍 - Account IDs: {getattr(self.ws_client, 'account_ids', 'N/A')}")
                logger.info(f"🔍 - Order book IDs: {getattr(self.ws_client, 'order_book_ids', 'N/A')}")
                
                return True
                
            except Exception as ws_error:
                logger.error(f"WebSocket task failed: {ws_error}")
                self._connected = False
                raise
            
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            self._connected = False
            raise

    async def _run_websocket(self):
        """Run the WebSocket client with proper error handling"""
        try:
            logger.info("🔌 Starting WebSocket client...")
            logger.info(f"🔌 Account IDs: {self.account_ids}")
            logger.info(f"🔌 Order book IDs: {self.order_book_ids}")
            await self.ws_client.run_async()
        except asyncio.CancelledError:
            logger.info("WebSocket task cancelled")
            raise
        except Exception as e:
            # Don't treat "Failed to fetch" as a critical error - it's common in WebSocket connections
            if "Failed to fetch" in str(e) or "Unhandled message" in str(e):
                logger.debug(f"WebSocket message handling: {e}")
                # Continue running - this is not a fatal error
                return
            else:
                logger.error(f"WebSocket runtime error: {e}")
                self._connected = False
                raise

    async def disconnect(self):
        """Disconnect the WebSocket connection."""
        try:
            # Cancel the WebSocket task if it exists
            if hasattr(self, 'ws_task') and self.ws_task:
                self.ws_task.cancel()
                try:
                    await self.ws_task
                except asyncio.CancelledError:
                    pass
                logger.info("WebSocket task cancelled")
            
            # Stop the WebSocket client
            if self.ws_client:
                if hasattr(self.ws_client, 'stop'):
                    self.ws_client.stop()
                self._connected = False
                logger.info("Lighter WebSocket disconnected")
                
        except Exception as e:
            logger.error(f"Error disconnecting WebSocket: {e}")

    def is_connected(self) -> bool:
        """Check if WebSocket is connected."""
        return self._connected

    def set_market_mapping(self, market_mapping: Dict[int, str]):
        """Set market mapping for symbol resolution."""
        self._market_mapping = market_mapping

    def get_market_stats(self, market_id: int) -> Dict:
        """Get market stats for a market."""
        # Convert market_id to string since _market_stats uses string keys
        market_key = str(market_id)
        return self._market_stats.get(market_key, {})

    def get_latest_price(self, market_id: int) -> Optional[float]:
        """Get latest price for a market."""
        # Convert market_id to string since _market_stats uses string keys
        market_key = str(market_id)
        market_stats = self._market_stats.get(market_key)
        if market_stats and 'mid_price' in market_stats:
            return market_stats['mid_price']
        return None

    def _handle_account_update(self, account_id: int, account: Dict):
        """Handle account updates from Lighter SDK."""
        try:
            # Call the callback
            if self.on_account_update:
                self.on_account_update(account_id, account)
                
        except Exception as e:
            logger.error(f"Error handling account update: {e}")

    def _handle_order_book_update(self, order_book_id: int, order_book):
        """Handle order book updates from Lighter SDK."""
        try:
            # Log order book updates at debug level to reduce spam
            logger.debug(f"🔔 ORDER BOOK UPDATE RECEIVED for market {order_book_id}")
            logger.debug(f"🔔 Data type: {type(order_book)}")
            logger.debug(f"🔔 Data content: {order_book}")
            
            # Debug: Log the raw order book data structure
            if order_book_id == 3:  # XRP
                logger.debug(f"🔍 DEBUG - Raw order book data for XRP (market 3):")
                logger.debug(f"🔍 DEBUG - Type: {type(order_book)}")
                logger.debug(f"🔍 DEBUG - Content: {order_book}")
                logger.debug(f"🔍 DEBUG - Has bids attr: {hasattr(order_book, 'bids')}")
                logger.debug(f"🔍 DEBUG - Has asks attr: {hasattr(order_book, 'asks')}")
                if hasattr(order_book, 'bids'):
                    logger.debug(f"🔍 DEBUG - Bids: {order_book.bids}")
                if hasattr(order_book, 'asks'):
                    logger.debug(f"🔍 DEBUG - Asks: {order_book.asks}")
            
            # Extract price from order book data
            mid_price = None
            
            # Handle different order book data formats
            if hasattr(order_book, 'bids') and hasattr(order_book, 'asks'):
                # Object with bids/asks attributes
                if order_book.bids and order_book.asks:
                    best_bid = self._extract_price(order_book.bids[0])
                    best_ask = self._extract_price(order_book.asks[0])
                    if best_bid and best_ask and best_bid > 0 and best_ask > 0:
                        mid_price = (best_bid + best_ask) / 2
                        if order_book_id == 3:  # XRP
                            logger.info(f"🔍 DEBUG - Extracted prices: bid={best_bid}, ask={best_ask}, mid={mid_price}")
            elif isinstance(order_book, dict) and 'bids' in order_book and 'asks' in order_book:
                # Dictionary format
                if order_book['bids'] and order_book['asks']:
                    best_bid = self._extract_price(order_book['bids'][0])
                    best_ask = self._extract_price(order_book['asks'][0])
                    if best_bid and best_ask and best_bid > 0 and best_ask > 0:
                        mid_price = (best_bid + best_ask) / 2
                        if order_book_id == 3:  # XRP
                            logger.info(f"🔍 DEBUG - Extracted prices from dict: bid={best_bid}, ask={best_ask}, mid={mid_price}")
            
            # Store the mid price if we successfully extracted it
            if mid_price:
                self._market_stats[str(order_book_id)] = {'mid_price': mid_price}
                # Only log XRP price updates
                if order_book_id == 3:  # XRP
                    logger.debug(f"📈 XRP price: ${mid_price:.6f}")
                    logger.debug(f"🔍 Stored in _market_stats: {self._market_stats}")
            else:
                if order_book_id == 3:  # XRP
                    logger.debug(f"🔍 No mid_price extracted for XRP (market 3)")
                    logger.debug(f"🔍 DEBUG - Order book structure analysis:")
                    logger.debug(f"🔍 DEBUG - Is dict: {isinstance(order_book, dict)}")
                    if isinstance(order_book, dict):
                        logger.debug(f"🔍 DEBUG - Dict keys: {list(order_book.keys())}")
                    logger.debug(f"🔍 DEBUG - Dir attributes: {[attr for attr in dir(order_book) if not attr.startswith('_')]}")
            
            # Call the callback
            if self.on_order_book_update:
                self.on_order_book_update(order_book_id, order_book)
                
        except Exception as e:
            logger.error(f"Error handling order book update for market {order_book_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    def _extract_price(self, price_data):
        """Extract price from various data formats."""
        try:
            if isinstance(price_data, (int, float)):
                return float(price_data)
            elif isinstance(price_data, str):
                return float(price_data)
            elif isinstance(price_data, dict):
                # Try common price field names
                for field in ['price', 'p', 'px', 'amount', 'size']:
                    if field in price_data:
                        return float(price_data[field])
                # If no price field found, try to convert the dict values
                if len(price_data) > 0:
                    first_value = list(price_data.values())[0]
                    return float(first_value)
            elif isinstance(price_data, list) and len(price_data) > 0:
                return float(price_data[0])
            elif hasattr(price_data, 'price'):
                return float(price_data.price)
            else:
                logger.debug(f"Unknown price data format: {type(price_data)} - {price_data}")
                return None
        except (ValueError, TypeError, IndexError) as e:
            logger.debug(f"Failed to extract price from {price_data}: {e}")
            return None

    def _default_account_handler(self, account_id: int, account: Dict):
        """Default account update handler."""
        logger.debug(f"Account update for account {account_id}")

    def _default_order_book_handler(self, order_book_id: int, order_book: Dict):
        """Default order book update handler."""
        logger.debug(f"Order book update for order book {order_book_id}")
    
    def _handle_unhandled_message(self, message):
        """Handle unhandled WebSocket messages gracefully."""
        try:
            # Respond to application-level ping with pong to satisfy Lighter WS policy
            if isinstance(message, dict) and message.get("type") == "ping":
                try:
                    if getattr(self.ws_client, "ws", None) is not None:
                        # Send app-level pong
                        send_coro = self.ws_client.ws.send(json.dumps({"type": "pong"}))
                        if asyncio.iscoroutine(send_coro):
                            # If ws.send is async (it is), await it safely in background
                            asyncio.create_task(send_coro)
                    return
                except Exception as ping_err:
                    logger.debug(f"Failed to respond to ping: {ping_err}")

            # Log unhandled messages at debug level to avoid spam
            if isinstance(message, dict) and 'error' in message:
                error_code = message['error'].get('code', 'unknown')
                error_msg = message['error'].get('message', 'unknown error')
                logger.debug(f"WebSocket error message: {error_code} - {error_msg}")
            else:
                logger.debug(f"Unhandled WebSocket message: {type(message)} - {message}")
        except Exception as e:
            logger.debug(f"Error handling unhandled message: {e}")

    async def subscribe_to_market_updates(self, market_ids: List[int], callback: Callable):
        """Subscribe to market updates for specific markets."""
        try:
            # Update account IDs to include new markets
            self.account_ids = list(set(self.account_ids + market_ids))
            
            if self.ws_client and hasattr(self.ws_client, 'subscribe_to_market_updates'):
                success = await self.ws_client.subscribe_to_market_updates(market_ids, callback)
                if success:
                    logger.info(f"Subscribed to market updates for markets: {market_ids}")
                    return True
                else:
                    logger.error(f"Failed to subscribe to market updates for markets: {market_ids}")
                    return False
            else:
                logger.warning("WebSocket client not available for market updates")
                return False
                
        except Exception as e:
            logger.error(f"Error subscribing to market updates: {e}")
            return False