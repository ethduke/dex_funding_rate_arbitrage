import asyncio
import json
import time
import functools
from typing import Dict, Tuple, Literal

from utils.logger import setup_logger
from model.exchanges.hyperliquid import HyperliquidExchange
from model.exchanges.backpack_ws import BackpackWebSocketClient
from model.exchanges.lighter import LighterExchange

# Initialize logger
logger = setup_logger(__name__)

# Symbol configuration
TARGET_SYMBOL = "ETH" 
BP_SYMBOL = f"{TARGET_SYMBOL}_USDC_PERP"

# Queue item type definition
FundingRateItem = Tuple[Literal["HL", "BP", "LT"], float, float]  # (exchange, rate, timestamp)

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

# Lighter callback
async def lt_funding_callback(funding_data: Dict, queue: asyncio.Queue):
    """Process Lighter funding updates."""
    try:
        # Extract funding rate from Lighter market stats
        market_id = funding_data.get("market_id")
        current_funding_rate = funding_data.get("current_funding_rate")
        
        if current_funding_rate is not None:
            funding_rate = float(current_funding_rate)
            timestamp = time.time()
            logger.info(f"LT Update [{TARGET_SYMBOL}]: Funding Rate = {funding_rate:+.8f}")
            await queue.put(("LT", funding_rate, timestamp))
    except Exception as e:
        logger.error(f"Error processing Lighter funding data: {e}", exc_info=True)

# Factory function for Lighter callback
def create_lt_callback_with_queue(queue):
    """Create a Lighter callback with queue access."""
    async def callback(funding_data):
        await lt_funding_callback(funding_data, queue)
    return callback

# --- Funding Rate Processor ---
async def process_funding_rates(queue: asyncio.Queue):
    """Process and compare funding rates from all three exchanges."""
    latest_rates = {
        "HL": {"rate": None, "timestamp": None},
        "BP": {"rate": None, "timestamp": None},
        "LT": {"rate": None, "timestamp": None}
    }
    
    # Configuration for trading recommendations
    SIGNIFICANT_DIFFERENCE_THRESHOLD = 0.0001  # 0.01% difference threshold
    
    while True:
        # Get the next update
        exchange, rate, timestamp = await queue.get()
        
        # Update stored rates
        latest_rates[exchange]["rate"] = rate
        latest_rates[exchange]["timestamp"] = timestamp
        
        # Compare rates if we have at least two exchanges
        available_rates = {k: v for k, v in latest_rates.items() if v["rate"] is not None}
        
        if len(available_rates) >= 2:
            logger.info(f"--- Funding Rate Comparison ({TARGET_SYMBOL}) ---")
            
            # Log all available rates
            for ex, data in available_rates.items():
                exchange_name = {"HL": "Hyperliquid", "BP": "Backpack", "LT": "Lighter"}[ex]
                logger.info(f"{exchange_name:<12} (Funding Rate): {data['rate']:+.8f} (Updated: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data['timestamp']))})")
            
            # Find best arbitrage opportunities
            exchanges = list(available_rates.keys())
            best_opportunity = None
            max_difference = 0
            
            for i in range(len(exchanges)):
                for j in range(i + 1, len(exchanges)):
                    ex1, ex2 = exchanges[i], exchanges[j]
                    rate1 = available_rates[ex1]["rate"]
                    rate2 = available_rates[ex2]["rate"]
                    difference = abs(rate1 - rate2)
                    
                    if difference > max_difference:
                        max_difference = difference
                        if rate1 > rate2:
                            best_opportunity = (ex1, ex2, rate1, rate2, difference)
                        else:
                            best_opportunity = (ex2, ex1, rate2, rate1, difference)
            
            if best_opportunity and max_difference > SIGNIFICANT_DIFFERENCE_THRESHOLD:
                ex_high, ex_low, rate_high, rate_low, diff = best_opportunity
                ex_high_name = {"HL": "Hyperliquid", "BP": "Backpack", "LT": "Lighter"}[ex_high]
                ex_low_name = {"HL": "Hyperliquid", "BP": "Backpack", "LT": "Lighter"}[ex_low]
                
                logger.info(f"TRADING RECOMMENDATION: LONG on {ex_low_name}, SHORT on {ex_high_name}")
                logger.info(f"Action: Buy {TARGET_SYMBOL} on {ex_low_name}, Sell {TARGET_SYMBOL} on {ex_high_name}")
                logger.info(f"Funding Rate Differential: {diff:+.8f}")
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
    # Setup Lighter client
    logger.info(f"Initializing Lighter exchange...")
    lt_client = LighterExchange(use_ws=True)
    await lt_client.initialize_ws()
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
        
        # Setup Lighter client
        logger.info(f"Subscribing to Lighter funding for {TARGET_SYMBOL}...")
        lt_callback = create_lt_callback_with_queue(funding_rate_queue)
        # Note: Lighter WebSocket subscription will be handled by the client itself
        
        # Setup REST API fallback for Backpack
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
        
        # Setup REST API fallback for Lighter
        async def check_lighter_updates():
            """Periodically fetch Lighter funding rate via REST API."""
            while True:
                try:
                    funding_rates = await lt_client.get_funding_rates()
                    
                    if isinstance(funding_rates, dict) and TARGET_SYMBOL in funding_rates:
                        funding_rate = funding_rates[TARGET_SYMBOL]["rate"]
                        logger.info(f"LT REST API Update [{TARGET_SYMBOL}]: FundingRate={funding_rate:+.8f}")
                        await funding_rate_queue.put(("LT", funding_rate, time.time()))
                except Exception as e:
                    logger.error(f"Error fetching Lighter data via REST API: {e}")
                
                # Check every 5 minutes
                await asyncio.sleep(300)
        
        # Start the polling tasks
        backpack_polling_task = asyncio.create_task(check_backpack_updates())
        lighter_polling_task = asyncio.create_task(check_lighter_updates())
        
        # Setup Hyperliquid client
        logger.info(f"Subscribing to Hyperliquid user fundings...")
        hl_callback_with_loop = functools.partial(
            hl_funding_callback_sync_wrapper, 
            loop=main_loop, 
            queue=funding_rate_queue
        )
        hl_subscription_task = hl_client.subscribe_to_funding_updates(hl_callback_with_loop)
        
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
                     backpack_polling_task if 'backpack_polling_task' in locals() else None,
                     lighter_polling_task if 'lighter_polling_task' in locals() else None]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
        # Disconnect clients
        await bp_client.disconnect()
        await lt_client.close()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Main loop interrupted by Ctrl+C.") 