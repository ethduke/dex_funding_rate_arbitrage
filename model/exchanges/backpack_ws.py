import asyncio
import json
import time
import base64
import websockets
import nacl.signing
from typing import Dict, Optional, List, Callable
from utils.logger import setup_logger
from utils.config import CONFIG

logger = setup_logger(__name__)

class BackpackWebSocketClient:
    def __init__(self):
        self.api_key = CONFIG.get("BACKPACK_PUBLIC_KEY")
        self.api_secret_b64 = CONFIG.get("BACKPACK_PRIVATE_KEY")
        self.ws_url = CONFIG.get("BACKPACK_WS_URL")
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.subscriptions: Dict[str, Callable] = {}
        self._stop_event = asyncio.Event()
        self._connection_task = None
        self._pending_subscriptions: Dict[str, Callable] = {} # Store subscriptions requested before connect

    async def _sign_subscription(self) -> List[str]:
        """Generates the signature required for subscribing to private streams."""
        private_key = base64.b64decode(self.api_secret_b64)
        signing_key = nacl.signing.SigningKey(private_key)

        timestamp = int(time.time() * 1000)
        window = CONFIG.get("BACKPACK_DEFAULT_WINDOW")
        message = f"instruction=subscribe&timestamp={timestamp}&window={window}"

        logger.debug(f"Signing WebSocket subscription message: {message}")
        signature = signing_key.sign(message.encode())

        return [
            self.api_key, 
            base64.b64encode(signature.signature).decode(), 
            str(timestamp),
            str(window)
        ]

    async def connect(self):
        """Connects to the WebSocket server and starts listening."""
        if self.websocket and self.websocket.state == 1:
            return

        try:
            self.websocket = await websockets.connect(self.ws_url, ping_interval=60, ping_timeout=120)
            logger.info("WebSocket connection established successfully")
            self._stop_event.clear()
            self._connection_task = asyncio.create_task(self._listen())
            # Resubscribe to any stored subscriptions
            await self._resubscribe()
            await self._process_pending_subscriptions()
        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.InvalidHandshake) as e:
            logger.error(f"WebSocket connection failed: {e}", exc_info=True)
            self.websocket = None
        except RuntimeError as e:
            if "can't register atexit after shutdown" in str(e):
                logger.warning("Cannot connect to WebSocket during interpreter shutdown.")
            else:
                logger.error(f"WebSocket connection failed with runtime error: {e}", exc_info=True)
            self.websocket = None
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}", exc_info=True)
            self.websocket = None

    async def disconnect(self):
        """Disconnects the WebSocket connection."""
        if self._connection_task:
            self._stop_event.set()
            self._connection_task.cancel()
            try:
                await self._connection_task
            except asyncio.CancelledError:
                pass
            self._connection_task = None

        if self.websocket and self.websocket.state == 1:
            try:
                await self.websocket.close()
            except Exception as e:
                logger.error(f"Error closing websocket: {e}", exc_info=True)
        self.websocket = None

    def is_connected(self) -> bool:
        """Check if the WebSocket connection is active.
        
        Returns:
            bool: True if the connection is open, False otherwise
        """
        return self.websocket is not None and self.websocket.state == 1

    async def subscribe(self, stream_name: str, handler: Callable):
        """Subscribes to a WebSocket stream."""
        if not self.websocket or not self.websocket.state == 1:
            logger.warning(f"WebSocket not connected. Queuing subscription for: {stream_name}")
            # Store subscription intent if not connected
            self._pending_subscriptions[stream_name] = handler
            return

        logger.debug(f"Subscribing to stream: {stream_name}")
        self.subscriptions[stream_name] = handler # Store handler
        payload = {
            "method": "SUBSCRIBE",
            "params": [stream_name]
        }

        # Check if it's a private stream (requires signature)
        if stream_name.startswith("account."):
            try:
                logger.debug(f"Generating signature for private stream {stream_name}...")
                signature_data = await self._sign_subscription()
                payload["signature"] = signature_data
                logger.debug(f"Signature generated for {stream_name}.")
            except Exception as e:
                logger.error(f"Failed to sign subscription for {stream_name}: {e}", exc_info=True)
                # Remove handler if signing failed
                if stream_name in self.subscriptions:
                     del self.subscriptions[stream_name]
                return

        try:
            logger.debug(f"Sending subscription payload for {stream_name}: {json.dumps(payload)}")
            await self.websocket.send(json.dumps(payload))
            logger.debug(f"Subscription payload sent for {stream_name}.")
            logger.debug(f"Successfully sent subscription request for {stream_name}")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.error(f"Failed to send subscription for {stream_name}. Connection closed: {e}")
            # Connection closed, queue subscription for reconnect
            self._pending_subscriptions[stream_name] = handler
            if stream_name in self.subscriptions:
                del self.subscriptions[stream_name]
            self.websocket = None # Mark as disconnected
            # Optionally trigger reconnection logic here
        except Exception as e:
            logger.error(f"Failed to send subscription for {stream_name}: {e}", exc_info=True)
            # Remove handler if sending failed
            if stream_name in self.subscriptions:
                del self.subscriptions[stream_name]

    async def unsubscribe(self, stream_name: str):
        """Unsubscribes from a WebSocket stream."""
        # Remove from pending if it was queued
        if stream_name in self._pending_subscriptions:
            logger.debug(f"Removing queued subscription for {stream_name}")
            del self._pending_subscriptions[stream_name]

        if not self.websocket or not self.websocket.state == 1:
            logger.warning("WebSocket not connected. Cannot unsubscribe immediately.")
            if stream_name in self.subscriptions:
                del self.subscriptions[stream_name] # Remove from active subscriptions
            return

        logger.debug(f"Unsubscribing from stream: {stream_name}")
        payload = {
            "method": "UNSUBSCRIBE",
            "params": [stream_name]
        }

        try:
            logger.debug(f"Sending unsubscription payload for {stream_name}: {json.dumps(payload)}")
            await self.websocket.send(json.dumps(payload))
            logger.debug(f"Unsubscription payload sent for {stream_name}.")
            if stream_name in self.subscriptions:
                del self.subscriptions[stream_name]
            logger.debug(f"Successfully sent unsubscription request for {stream_name}")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.error(f"Failed to send unsubscription for {stream_name}. Connection closed: {e}")
            self.websocket = None # Mark as disconnected
        except Exception as e:
            logger.error(f"Failed to send unsubscription for {stream_name}: {e}", exc_info=True)


    async def _resubscribe(self):
        """Resubscribes to all previously active subscriptions."""
        if not self.websocket or not self.websocket.state == 1:
            logger.warning("Cannot resubscribe, WebSocket not connected.")
            return

        current_subscriptions = list(self.subscriptions.items()) # Copy items
        if not current_subscriptions:
            logger.debug("No active subscriptions to resubscribe to.")
            return
        
        self.subscriptions.clear() # Clear temporarily
        logger.debug(f"Resubscribing to {len(current_subscriptions)} streams...")
        for stream_name, handler in current_subscriptions:
            logger.debug(f"Attempting to resubscribe to {stream_name}")
            try:
                await self.subscribe(stream_name, handler)
            except Exception as e:
                 logger.error(f"Error during resubscription to {stream_name}: {e}", exc_info=True)

    async def _process_pending_subscriptions(self):
        """Processes subscriptions requested before the connection was established."""
        if not self.websocket or not self.websocket.state == 1:
             logger.warning("Cannot process pending subscriptions, WebSocket not connected.")
             return
        
        pending = list(self._pending_subscriptions.items())
        if not pending:
            logger.debug("No pending subscriptions to process.")
            return
            
        logger.debug(f"Processing {len(pending)} pending subscriptions...")
        self._pending_subscriptions.clear() # Clear pending list before processing
        for stream_name, handler in pending:
            logger.debug(f"Processing pending subscription for {stream_name}")
            await self.subscribe(stream_name, handler) # Call subscribe to handle sending etc.

    async def _listen(self):
        """Listens for messages and handles them."""
        logger.debug("WebSocket listener task started.")
        try:
            while not self._stop_event.is_set():
                message_str = None # Initialize for error logging
                try:
                    # Check if websocket is still valid before trying to receive
                    if not self.websocket or self.websocket.state != 1:
                        logger.warning("WebSocket connection is closed or None, stopping listener.")
                        break
                    
                    logger.debug("Waiting for message from WebSocket...")
                    message_str = await self.websocket.recv()
                    logger.debug(f"Received message string: {message_str}")
                    message = json.loads(message_str)
                    # logger.debug(f"Received parsed message: {message}") # Can be noisy

                    if "stream" in message and "data" in message:
                        stream_name = message["stream"]
                        data = message["data"]
                        if stream_name in self.subscriptions:
                            try:
                                logger.debug(f"Dispatching message for stream: {stream_name}")
                                handler = self.subscriptions[stream_name]
                                if asyncio.iscoroutinefunction(handler):
                                    asyncio.create_task(handler(data))
                                else:
                                     handler(data)
                            except Exception as e:
                                logger.error(f"Error processing message for stream {stream_name}: {e} | Data: {data}", exc_info=True)
                        else:
                            logger.warning(f"Received message for stream not in active subscriptions: {stream_name}")
                    elif isinstance(message, dict) and "id" in message and ("result" in message or "error" in message):
                         # Handle subscription/unsubscription confirmation/error messages
                         # TODO: Correlate ID with sent subscribe/unsubscribe requests if needed
                         if "error" in message:
                            logger.error(f"Received error response from server: {message}")
                         else:
                            logger.debug(f"Received confirmation response from server: {message}")
                    elif isinstance(message, dict) and message.get("method") == "PING":
                        # Handle PING from server if needed (library usually handles PONG)
                        logger.debug("Received PING from server, sending PONG.")
                        await self.websocket.pong()
                    else:
                        logger.debug(f"Received unhandled message format: {message}")

                except websockets.exceptions.ConnectionClosedError as e:
                    logger.warning(f"WebSocket connection closed by server: {e}. Code: {e.code}, Reason: {e.reason}")
                    self.websocket = None
                    # Stop listening, connection task will end. Reconnection handled elsewhere if needed.
                    break
                except websockets.exceptions.ConnectionClosedOK as e:
                    logger.debug(f"WebSocket connection closed normally by server. Code: {e.code}, Reason: {e.reason}")
                    break # Exit listen loop on normal closure
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON message: {message_str}")
                except asyncio.TimeoutError:
                    logger.warning("Timeout waiting for message from WebSocket.") # If using asyncio.wait_for
                except Exception as e:
                    logger.error(f"Error in WebSocket listener loop: {e}", exc_info=True)
                    # Avoid tight loop on persistent errors
                    await asyncio.sleep(1) 


        except asyncio.CancelledError:
            logger.debug("WebSocket listener task explicitly cancelled.")
        finally:
            logger.debug("WebSocket listener task stopped.")
            # Ensure connection is marked as closed if exiting loop unexpectedly
            if self.websocket and self.websocket.state == 1:
                 try:
                     await self.websocket.close()
                 except Exception as e:
                     logger.error(f"Error closing websocket in listener finally block: {e}", exc_info=True)
            self.websocket = None

    # --- Specific Message Handlers ---

    async def handle_mark_price(self, data: Dict):
        """Handles mark price updates."""
        try:
            symbol = data.get('s')
            mark_price = data.get('p')
            funding_rate = data.get('f')
            timestamp_us = data.get('E') 
            logger.debug(f"Mark Price Update [{symbol}]: Price={mark_price}, Funding={funding_rate}, Time={timestamp_us}")
            # Add further processing logic here (e.g., update internal state, notify other components)
        except Exception as e:
            logger.error(f"Error processing mark price data: {e} | Data: {data}", exc_info=True)

    async def handle_position_query(self, data: Dict):
        """Handles individual position update events."""
        try:
            # Data is a single dictionary, not a list
            position = data
            symbol = position.get('s')
            net_quantity = position.get('q') # Use 'q' for quantity based on provided log
            entry_price = position.get('b') # Use 'b' for entry/break even based on provided log
            pnl_unrealized = position.get('p') # Use 'p' for unrealized PnL based on provided log
            est_liq_price = position.get('l') # Use 'l' for liquidation price based on provided log
            mark_price = position.get('M') # Use 'M' for mark price based on provided log

            logger.debug(
                f"  Position Update [{symbol}]: Qty={net_quantity}, Entry={entry_price}, "
                f"uPnL={pnl_unrealized}, Mark={mark_price}, LiqEst={est_liq_price}"
            )
            # Add further processing logic here (e.g., update internal state)
        except Exception as e:
            logger.error(f"Error processing position update data: {e} | Raw Data: {data}", exc_info=True)
