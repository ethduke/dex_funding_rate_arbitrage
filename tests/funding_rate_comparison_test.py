import asyncio
import json
import time
import functools
from typing import Dict, Tuple, Literal
from utils.logger import setup_logger
from model.exchanges.hyperliquid import HyperliquidExchange
from model.exchanges.backpack_ws import BackpackWebSocketClient

# Initialize logger
logger = setup_logger(__name__)

# Symbol configuration
TARGET_SYMBOL = "ETH" 
BP_SYMBOL = f"{TARGET_SYMBOL}_USDC_PERP"

# Queue item type definition
FundingRateItem = Tuple[Literal["HL", "BP"], float, float]  # (exchange, rate, timestamp)

# --- Callback Functions ---
async def hl_funding_callback(funding_data: Dict, queue: asyncio.Queue):
    """Process Hyperliquid funding updates."""
    try:
        if isinstance(funding_data, dict) and funding_data.get("channel") == "userFundings":
            data = funding_data.get("data", {})
            fundings_list = data.get('fundings', [])

            for funding_event in reversed(fundings_list):
                if funding_event.get("coin") == TARGET_SYMBOL:
                    funding_rate_str = funding_event.get("fundingRate")
                    timestamp_ms = funding_event.get("time")

                    if funding_rate_str is not None:
                        funding_rate = float(funding_rate_str)
                        logger.info(f"HL Update [{TARGET_SYMBOL}]: Funding Rate = {funding_rate:+.8f}")
                        timestamp = timestamp_ms / 1000 if timestamp_ms else time.time()
                        await queue.put(("HL", funding_rate, timestamp))
                        break
    except Exception as e:
        logger.error(f"Error processing Hyperliquid funding data: {e}", exc_info=True)

# Sync wrapper for Hyperliquid callback
def hl_funding_callback_sync_wrapper(data: Dict, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue):
    """Thread-safe wrapper for Hyperliquid callback."""
    asyncio.run_coroutine_threadsafe(hl_funding_callback(data, queue), loop)

# Backpack callback
async def bp_mark_price_callback(message: Dict, queue: asyncio.Queue):
    """Process Backpack mark price updates."""
    try:
        # Handle both full message and direct data payload formats
        data_payload = message.get('data', {}) if message.get('stream') == f"markPrice.{BP_SYMBOL}" else message
        
        symbol = data_payload.get('s')
        mark_price = data_payload.get('p')
        funding_rate_str = data_payload.get('f')
        timestamp_us = data_payload.get('E')

        if funding_rate_str is not None and symbol:
            funding_rate = float(funding_rate_str)
            timestamp = timestamp_us / 1_000_000 if timestamp_us else time.time()
            logger.info(f"BP Update [{symbol}]: MarkPrice={mark_price}, FundingRate={funding_rate:+.8f}")
            await queue.put(("BP", funding_rate, timestamp))
    except Exception as e:
        logger.error(f"Error processing Backpack mark price message: {e}", exc_info=True)

# Factory function for Backpack callback
def create_bp_callback_with_queue(queue):
    """Create a Backpack callback with queue access."""
    async def callback(message):
        await bp_mark_price_callback(message, queue)
    return callback

# --- Funding Rate Processor ---
async def process_funding_rates(queue: asyncio.Queue):
    """Process and compare funding rates from both exchanges."""
    latest_rates = {
        "HL": {"rate": None, "timestamp": None},
        "BP": {"rate": None, "timestamp": None}
    }
    
    # Configuration for trading recommendations
    SIGNIFICANT_DIFFERENCE_THRESHOLD = 0.0001  # 0.01% difference threshold
    
    while True:
        # Get the next update
        exchange, rate, timestamp = await queue.get()
        
        # Update stored rates
        latest_rates[exchange]["rate"] = rate
        latest_rates[exchange]["timestamp"] = timestamp
        
        # Compare rates if we have both
        if latest_rates["HL"]["rate"] is not None and latest_rates["BP"]["rate"] is not None:
            hl_rate = latest_rates["HL"]["rate"]
            bp_rate = latest_rates["BP"]["rate"]
            hl_time = latest_rates["HL"]["timestamp"]
            bp_time = latest_rates["BP"]["timestamp"]
            
            logger.info(f"--- Funding Rate Comparison ({TARGET_SYMBOL}) ---")
            logger.info(f"Hyperliquid (Funding Rate): {hl_rate:+.8f} (Updated: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(hl_time))})")
            logger.info(f"Backpack    (Funding Rate): {bp_rate:+.8f} (Updated: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(bp_time))})")
            diff = hl_rate - bp_rate
            logger.info(f"Difference (HL Rate - BP Rate): {diff:+.8f}")
            
            # Generate trading recommendation
            if abs(diff) > SIGNIFICANT_DIFFERENCE_THRESHOLD:
                if diff > 0:
                    # HL rate is higher than BP rate
                    logger.info(f"TRADING RECOMMENDATION: LONG on Backpack, SHORT on Hyperliquid")
                    logger.info(f"Action: Buy {TARGET_SYMBOL} on Backpack, Sell {TARGET_SYMBOL} on Hyperliquid")
                else:
                    # BP rate is higher than HL rate
                    logger.info(f"TRADING RECOMMENDATION: LONG on Hyperliquid, SHORT on Backpack")
                    logger.info(f"Action: Buy {TARGET_SYMBOL} on Hyperliquid, Sell {TARGET_SYMBOL} on Backpack")
            else:
                logger.info("No significant funding rate difference for trading")
            
            logger.info("-------------------------------------")

# --- Optimized version of BackpackWebSocketClient ---
class EnhancedBackpackClient(BackpackWebSocketClient):
    """Enhanced Backpack client that passes full message to handler."""
    async def _listen(self):
        """Listen for WebSocket messages and process them."""
        logger.info("WebSocket listener started")
        try:
            while not self._stop_event.is_set():
                try:
                    message_str = await self.websocket.recv()
                    message = json.loads(message_str)
                    
                    if "stream" in message and "data" in message:
                        stream_name = message["stream"]
                        if stream_name in self.subscriptions:
                            handler = self.subscriptions[stream_name]
                            if asyncio.iscoroutinefunction(handler):
                                asyncio.create_task(handler(message))  # Pass full message
                            else:
                                handler(message)
                except Exception as e:
                    if not self._stop_event.is_set():
                        logger.error(f"Error in WebSocket listener: {e}", exc_info=True)
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("WebSocket listener cancelled")
        finally:
            if self.websocket and self.websocket.open:
                await self.websocket.close()
            self.websocket = None

# --- Main Execution ---
async def main():
    logger.info("Initializing clients...")
    bp_client = EnhancedBackpackClient()
    hl_client = HyperliquidExchange()
    main_loop = asyncio.get_running_loop()
    
    # Create data queue and shutdown event
    funding_rate_queue = asyncio.Queue()
    shutdown_event = asyncio.Future()
    
    # Start processor task
    processor_task = asyncio.create_task(process_funding_rates(funding_rate_queue))

    try:
        # Setup Backpack client
        logger.info("Connecting Backpack WebSocket...")
        await bp_client.connect()
        logger.info(f"Subscribing to Backpack mark price for {BP_SYMBOL}...")
        bp_callback = create_bp_callback_with_queue(funding_rate_queue)
        await bp_client.subscribe(f"markPrice.{BP_SYMBOL}", bp_callback)
        
        # Setup REST API fallback
        async def check_backpack_updates():
            """Periodically fetch Backpack funding rate via REST API."""
            while True:
                try:
                    from model.exchanges.backpack import BackpackExchange
                    rest_client = BackpackExchange()
                    mark_prices = rest_client.get_mark_prices(TARGET_SYMBOL)
                    
                    if isinstance(mark_prices, list):
                        for item in mark_prices:
                            if item.get("symbol") == f"{TARGET_SYMBOL}_USDC_PERP":
                                funding_rate_str = item.get("fundingRate")
                                if funding_rate_str:
                                    funding_rate = float(funding_rate_str)
                                    logger.info(f"BP REST API Update [{TARGET_SYMBOL}]: FundingRate={funding_rate:+.8f}")
                                    await funding_rate_queue.put(("BP", funding_rate, time.time()))
                                    break
                except Exception as e:
                    logger.error(f"Error fetching Backpack data via REST API: {e}")
                
                # Check every 5 minutes
                await asyncio.sleep(300)
        
        # Start the polling task
        backpack_polling_task = asyncio.create_task(check_backpack_updates())
        
        # Setup Hyperliquid client
        logger.info(f"Subscribing to Hyperliquid user fundings...")
        hl_callback_with_loop = functools.partial(
            hl_funding_callback_sync_wrapper, 
            loop=main_loop, 
            queue=funding_rate_queue
        )
        hl_subscription_task = hl_client.subscribe_to_user_fundings(hl_callback_with_loop)
        
        logger.info("Setup complete. Monitoring funding rates (Press Ctrl+C to exit)...")
        await shutdown_event

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
        if not shutdown_event.done():
            shutdown_event.set_result(None)
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        if not shutdown_event.done():
            shutdown_event.set_result(None)
    finally:
        # Cleanup tasks
        for task in [processor_task, 
                     backpack_polling_task if 'backpack_polling_task' in locals() else None]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
        # Disconnect clients
        await bp_client.disconnect()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Main loop interrupted by Ctrl+C.") 