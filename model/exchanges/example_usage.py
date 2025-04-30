import asyncio
from model.exchanges.backpack import BackpackExchange
from utils.logger import setup_logger

logger = setup_logger(__name__)

async def handle_mark_price_update(data):
    """Handle mark price updates from WebSocket."""
    symbol = data.get('s')
    mark_price = data.get('p')
    funding_rate = data.get('f')
    logger.info(f"[WS] Mark Price Update for {symbol}: Price={mark_price}, Funding={funding_rate}")

async def example_with_websocket():
    """Example of using Backpack exchange with WebSocket."""
    # Initialize with WebSocket support
    exchange = BackpackExchange(use_ws=True)
    
    # Initialize WebSocket connection
    connected = await exchange.initialize_ws()
    logger.info(f"WebSocket connected: {connected}")
    
    if connected:
        # Subscribe to funding rate updates
        subscription = await exchange.subscribe_to_funding_updates(handle_mark_price_update)
        logger.info(f"Subscription status: {subscription}")
        
        # Keep the connection open for some time to receive updates
        logger.info("Waiting for WebSocket updates for 60 seconds...")
        await asyncio.sleep(60)
        
        # Close the WebSocket connection
        await exchange.close_ws()
        logger.info("WebSocket connection closed")

def example_without_websocket():
    """Example of using Backpack exchange without WebSocket."""
    # Initialize without WebSocket
    exchange = BackpackExchange(use_ws=False)
    
    # Get funding rates using REST API
    funding_rates = exchange.get_funding_rates()
    logger.info(f"Funding rates: {funding_rates}")
    
    # Get positions
    positions = exchange.get_positions()
    logger.info(f"Positions: {positions}")

async def main():
    # Example without WebSocket (synchronous)
    logger.info("----- Example without WebSocket -----")
    example_without_websocket()
    
    # Example with WebSocket (asynchronous)
    logger.info("----- Example with WebSocket -----")
    await example_with_websocket()

if __name__ == "__main__":
    asyncio.run(main()) 