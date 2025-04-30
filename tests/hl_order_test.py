import time
from utils.logger import setup_logger
from model.exchanges.hyperliquid import HyperliquidExchange

# Initialize logger
logger = setup_logger(__name__)

def test_hyperliquid_market_order(symbol: str, amount: float, side: str):
    """Test Hyperliquid market order using the official SDK"""
    
    logger.info("=== Starting Hyperliquid Market Order Test ===")
    logger.info(f"Asset: {symbol}")
    logger.info(f"Position size: {amount} USD")
    logger.info(f"Side: {side}")
    
    try:
        # Initialize client - use testnet for testing
        logger.info("Initializing Hyperliquid client...")
        hl = HyperliquidExchange()
        logger.info(f"Using address: {hl.address}")
        
        # Get funding rates
        logger.info("Fetching funding rates...")
        funding_data = hl.get_funding_rates()
        
        # Check current positions
        logger.info("Checking current positions...")
        positions = hl.get_positions()
        logger.info(f"Current positions: {positions}")
        
        # Place a market order based on side parameter
        order_type = "buy" if side.lower() == "bid" else "sell"
        logger.info(f"Placing {amount} USD market {order_type} order for {symbol}...")
        
        if side.lower() == "bid":
            order_result = hl.open_long(
                asset=symbol,
                usd_amount=amount
            )
        else:
            order_result = hl.open_short(
                asset=symbol,
                usd_amount=amount
            )
            
        logger.info(f"Order result: {order_result}")
        
        if order_result.get("status") == "ok":
            # Process successful order
            logger.info("Order succeeded. Details:")
            for status in order_result["response"]["data"]["statuses"]:
                try:
                    filled = status["filled"]
                    logger.info(f'Order #{filled["oid"]} filled {filled["totalSz"]} @{filled["avgPx"]}')
                except KeyError:
                    logger.info(f'Error: {status["error"]}')
            
            # Wait a moment for the order to process
            logger.info("Waiting 2 seconds...")
            time.sleep(2)
            
            # Close the position
            logger.info(f"Closing {symbol} position...")
            close_result = hl.close_position(symbol)
            logger.info(f"Close result: {close_result}")
            
            # Display closing details
            if close_result.get("status") == "ok":
                for status in close_result["response"]["data"]["statuses"]:
                    try:
                        filled = status["filled"]
                        logger.info(f'Order #{filled["oid"]} filled {filled["totalSz"]} @{filled["avgPx"]}')
                    except KeyError:
                        logger.info(f'Error: {status["error"]}')
        else:
            logger.error(f"Order failed: {order_result}")
    
    except Exception as e:
        logger.error(f"Error during test: {str(e)}", exc_info=True)
    
    logger.info("=== Hyperliquid Market Order Test Complete ===")

if __name__ == "__main__":
    # Updated the default parameters to a more common asset
    test_hyperliquid_market_order("BTC", 100, "Ask") 