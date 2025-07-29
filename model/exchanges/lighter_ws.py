import asyncio
import json
from typing import Callable, List, Dict, Optional
from utils.logger import setup_logger
from websockets.client import connect as connect_async
from lighter.configuration import Configuration

logger = setup_logger(__name__)


class LighterWebSocketClient:
    def __init__(self, order_book_ids: List[int] = None, account_ids: List[int] = None,
                 on_order_book_update: Callable = None, on_account_update: Callable = None,
                 on_market_stats_update: Callable = None, market_mapping: Dict[int, str] = None):
        
        # Provide default subscriptions to avoid "No subscriptions provided" error
        if order_book_ids is None:
            order_book_ids = [0, 1]  # Default to ETH and BTC markets
        if account_ids is None:
            account_ids = [1]  # Default account ID
        
        # Get host from configuration
        host = Configuration.get_default().host.replace("https://", "")
        self.base_url = f"wss://{host}/stream"
        
        self.subscriptions = {
            "order_books": order_book_ids,
            "accounts": account_ids,
        }
        
        if len(order_book_ids) == 0 and len(account_ids) == 0:
            raise Exception("No subscriptions provided.")
        
        self.order_book_states = {}
        self.account_states = {}
        self._market_stats = {}
        self._market_mapping = market_mapping or {}
        
        self.on_order_book_update = on_order_book_update or self._default_order_book_handler
        self.on_account_update = on_account_update or self._default_account_handler
        self.on_market_stats_update = on_market_stats_update or self._default_market_stats_handler
        
        self.ws = None
        self._task = None
        self._connected = False
        self._stop_event = asyncio.Event()

    async def connect(self):
        """Connect to the WebSocket server and start listening."""
        if self.ws and self.ws.open:
            logger.debug("WebSocket already connected.")
            return

        logger.debug(f"Attempting to connect to WebSocket: {self.base_url}")
        try:
            self.ws = await connect_async(self.base_url)
            self._connected = True
            logger.info("✅ Lighter WebSocket connected successfully")
            
            # Subscribe to channels
            await self._subscribe_to_channels()
            
            # Start listening task
            self._task = asyncio.create_task(self._listen())
            
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            self._connected = False
            raise

    async def disconnect(self):
        """Disconnect the WebSocket connection."""
        logger.debug("Disconnect requested.")
        
        if self._task:
            logger.debug("Cancelling listener task...")
            self._stop_event.set()
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.debug("WebSocket listener task cancelled.")
            self._task = None
            logger.debug("Listener task cancelled.")

        if self.ws and self.ws.open:
            logger.debug("Closing WebSocket connection.")
            try:
                await self.ws.close()
                logger.debug("WebSocket connection closed.")
            except Exception as e:
                logger.error(f"Error closing websocket: {e}")
        else:
            logger.debug("Websocket already closed or not initialized.")
        
        self.ws = None
        self._connected = False
        logger.debug("Disconnect process complete.")

    async def _subscribe_to_channels(self):
        """Subscribe to all channels"""
        if not self.ws or not self.ws.open:
            logger.warning("WebSocket not connected. Cannot subscribe.")
            return
            
        # Subscribe to order books
        for market_id in self.subscriptions["order_books"]:
            await self.ws.send(
                json.dumps({"type": "subscribe", "channel": f"order_book/{market_id}"})
            )
        
        # Subscribe to accounts
        for account_id in self.subscriptions["accounts"]:
            await self.ws.send(
                json.dumps({"type": "subscribe", "channel": f"account_all/{account_id}"})
            )
        
        # Subscribe to market stats for all markets
        await self.ws.send(
            json.dumps({"type": "subscribe", "channel": "market_stats/all"})
        )
        
        logger.debug("Subscribed to all channels")

    async def _listen(self):
        """Listen for messages and handle them."""
        logger.debug("WebSocket listener task started.")
        try:
            while not self._stop_event.is_set():
                try:
                    message = await self.ws.recv()
                    await self.on_message_async(self.ws, message)
                except asyncio.CancelledError:
                    logger.debug("WebSocket listener task cancelled.")
                    break
                except Exception as e:
                    logger.error(f"Error in WebSocket listener: {e}")
                    await asyncio.sleep(1)  # Avoid tight loop on errors
        finally:
            logger.debug("WebSocket listener task stopped.")

    async def on_message_async(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            if isinstance(message, str):
                message = json.loads(message)
            
            message_type = message.get("type")
            
            if message_type == "connected":
                await self._subscribe_to_channels()
            elif message_type == "subscribed/order_book":
                self.handle_subscribed_order_book(message)
            elif message_type == "update/order_book":
                self.handle_update_order_book(message)
            elif message_type == "subscribed/account_all":
                self.handle_subscribed_account(message)
            elif message_type == "update/account_all":
                self.handle_update_account(message)
            elif message_type == "subscribed/market_stats":
                self.handle_subscribed_market_stats(message)
            elif message_type == "update/market_stats":
                self.handle_update_market_stats(message)
            else:
                self.handle_unhandled_message(message)
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def handle_subscribed_order_book(self, message):
        """Handle order book subscription confirmation"""
        market_id = message["channel"].split(":")[1]
        self.order_book_states[market_id] = message["order_book"]
        if self.on_order_book_update:
            self.on_order_book_update(market_id, self.order_book_states[market_id])

    def handle_update_order_book(self, message):
        """Handle order book updates"""
        market_id = message["channel"].split(":")[1]
        self.update_order_book_state(market_id, message["order_book"])
        if self.on_order_book_update:
            self.on_order_book_update(market_id, self.order_book_states[market_id])

    def update_order_book_state(self, market_id, order_book):
        """Update order book state"""
        if market_id not in self.order_book_states:
            self.order_book_states[market_id] = {"asks": [], "bids": []}
        
        self.update_orders(
            order_book["asks"], self.order_book_states[market_id]["asks"]
        )
        self.update_orders(
            order_book["bids"], self.order_book_states[market_id]["bids"]
        )

    def update_orders(self, new_orders, existing_orders):
        """Update orders in the order book"""
        for new_order in new_orders:
            is_new_order = True
            for existing_order in existing_orders:
                if new_order["price"] == existing_order["price"]:
                    is_new_order = False
                    existing_order["size"] = new_order["size"]
                    if float(new_order["size"]) == 0:
                        existing_orders.remove(existing_order)
                    break
            if is_new_order:
                existing_orders.append(new_order)

        # Remove zero-size orders
        existing_orders[:] = [
            order for order in existing_orders if float(order["size"]) > 0
        ]

    def handle_subscribed_account(self, message):
        """Handle account subscription confirmation"""
        account_id = message["channel"].split(":")[1]
        self.account_states[account_id] = message
        if self.on_account_update:
            self.on_account_update(account_id, self.account_states[account_id])

    def handle_update_account(self, message):
        """Handle account updates"""
        account_id = message["channel"].split(":")[1]
        self.account_states[account_id] = message
        if self.on_account_update:
            self.on_account_update(account_id, self.account_states[account_id])

    def handle_subscribed_market_stats(self, message):
        """Handle market stats subscription confirmation"""
        logger.debug("Subscribed to market stats")

    def handle_update_market_stats(self, message):
        """Handle market stats updates"""
        try:
            market_stats = message.get("market_stats", {})
            market_id = market_stats.get("market_id")
            
            if market_id is not None:
                symbol = self._get_symbol_by_market_id(market_id)
                funding_rate = float(market_stats.get("current_funding_rate", 0))
                mark_price = float(market_stats.get("mark_price", 0))
                index_price = float(market_stats.get("index_price", 0))
                funding_timestamp = market_stats.get("funding_timestamp", 0)
                
                # Only log funding rate changes, reduce verbosity
                logger.debug(f"📊 Market Stats [{symbol}]: Funding Rate={funding_rate}, Mark Price={mark_price}, Index Price={index_price}")
                
                # Store the latest market stats
                self._market_stats[market_id] = {
                    "symbol": symbol,
                    "funding_rate": funding_rate,
                    "mark_price": mark_price,
                    "index_price": index_price,
                    "funding_timestamp": funding_timestamp,
                    "market_stats": market_stats
                }
                
                if self.on_market_stats_update:
                    self.on_market_stats_update(market_stats)
                    
        except Exception as e:
            logger.error(f"Error processing market stats: {e}")

    def handle_unhandled_message(self, message):
        """Handle unhandled messages"""
        # Only log unhandled messages at debug level to reduce noise
        logger.debug(f"Unhandled message type: {message.get('type', 'unknown')}")

    def _default_order_book_handler(self, market_id, order_book):
        """Default order book update handler"""
        # Reduced logging - only log significant updates
        logger.debug(f"Order book update for market {market_id}")

    def _default_account_handler(self, account_id, account):
        """Default account update handler"""
        # Reduced logging - only log significant updates
        logger.debug(f"Account update for account {account_id}")

    def _default_market_stats_handler(self, market_stats: Dict):
        """Default market stats handler"""
        # Only log funding rate changes, not the full market stats
        market_id = market_stats.get("market_id")
        funding_rate = market_stats.get("current_funding_rate")
        if market_id is not None and funding_rate is not None:
            symbol = self._get_symbol_by_market_id(market_id)
            logger.info(f"📊 Market Stats [{symbol}]: Funding Rate={funding_rate}")

    def set_market_mapping(self, market_mapping: Dict[int, str]):
        """Set market mapping from parent exchange"""
        self._market_mapping = market_mapping

    def _get_symbol_by_market_id(self, market_id: int) -> str:
        """Get symbol name by market ID"""
        return self._market_mapping.get(market_id, f"UNKNOWN_{market_id}")

    def get_market_stats(self) -> Dict:
        """Get all current market stats"""
        return self._market_stats

    def get_funding_rates(self) -> Dict[str, Dict]:
        """Get funding rates in normalized format"""
        result = {}
        for market_id, stats in self._market_stats.items():
            symbol = stats["symbol"]
            result[symbol] = {
                "rate": stats["funding_rate"],
                "mark_price": stats["mark_price"],
                "index_price": stats["index_price"],
                "next_funding_time": stats["funding_timestamp"],
                "exchange": "Lighter"
            }
        return result

    def is_connected(self) -> bool:
        """Check if WebSocket is connected"""
        return self._connected and self.ws and self.ws.open

    async def close(self):
        """Close the WebSocket connection"""
        await self.disconnect()
