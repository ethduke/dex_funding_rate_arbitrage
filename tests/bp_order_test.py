import time
from utils.logger import setup_logger
from model.exchanges.backpack import BackpackExchange

# Initialize logger
logger = setup_logger(__name__)

def test_backpack_market_order(symbol: str, amount_usd: float, side: str):
    """Test Backpack market order functionality with a small position"""
    
    
    logger.info("=== Starting Backpack Market Order Test ===")
    logger.info(f"Symbol: {symbol}")
    logger.info(f"Position size: ${amount_usd} USDC")
    logger.info(f"Side: {side}")
    
    try:
        # Initialize client
        logger.info("Initializing Backpack client...")
        backpack = BackpackExchange()
        
        # Get mark prices to check if exchange connection works
        logger.info("Fetching mark prices...")
        mark_prices = backpack.get_mark_prices(symbol)
        
        # Check if there's an error in the response
        if isinstance(mark_prices, dict) and "error" in mark_prices:
            logger.error(f"Error fetching mark prices: {mark_prices}")
            return
            
        if isinstance(mark_prices, list) and len(mark_prices) > 0:
            mark_price = None
            for price_data in mark_prices:
                if price_data.get("symbol") == f"{symbol}_USDC_PERP":
                    mark_price = float(price_data.get("markPrice", 0))
                    funding_rate = float(price_data.get("fundingRate", 0))
                    logger.info(f"Current mark price: ${mark_price}")
                    logger.info(f"Current funding rate: {funding_rate}")
                    break
        else:
            logger.info(f"Mark prices response: {mark_prices}")
        
        # Check current positions
        logger.info("Checking current positions...")
        positions = backpack.get_positions()
        logger.info(f"Current positions: {positions}")
        
        # Use the formatted symbol for consistency
        formatted_symbol = f"{symbol}_USDC_PERP"
        logger.info(f"Placing ${amount_usd} market {side} order for {symbol}...") 
        
        # Use the appropriate method based on the side
        if side.lower() == "bid":
            order_result = backpack.open_long(symbol, amount_usd)
        elif side.lower() == "ask":
            order_result = backpack.open_short(symbol, amount_usd)
        else:
            logger.error(f"Invalid side: {side}. Must be 'Bid' or 'Ask'")
            return
            
        logger.info(f"Order result: {order_result}")
        
        # Check if there's an error in the order result
        if isinstance(order_result, dict) and "error" in order_result:
            logger.error(f"Order failed with error: {order_result['error']}")
            return
            
        if isinstance(order_result, dict) and order_result.get("orderId"):
            order_id = order_result.get("orderId")
            logger.info(f"Order placed successfully with ID: {order_id}")
            
            # Wait a moment for the order to process
            logger.info("Waiting 20 seconds...")
            time.sleep(20)
            
            # Check updated positions
            logger.info("Checking updated positions...")
            updated_positions = backpack.get_positions()
            logger.info(f"Updated positions: {updated_positions}")
            
            # Find our position
            position_size = 0
            for position in updated_positions:
                # Use the formatted symbol for comparison
                if position.get("symbol") == formatted_symbol: 
                    position_size = float(position.get("positionSize", 0))
                    logger.info(f"Current position size: {position_size}")
                    break
            
            if position_size > 0:
                # Close the position
                logger.info(f"Closing position for {formatted_symbol}...")
                # Pass the formatted symbol to close_position
                close_result = backpack.close_position(formatted_symbol, position_size) 
                logger.info(f"Close result: {close_result}")
                
                # Verify position is closed
                logger.info("Checking final positions...")
                final_positions = backpack.get_positions()
                
                # Check if position is closed
                position_closed = True
                for position in final_positions:
                    # Use the formatted symbol for comparison
                    if position.get("symbol") == formatted_symbol and float(position.get("positionSize", 0)) != 0:
                        position_closed = False
                        logger.info(f"Position still open: {position}")
                        break
                
                if position_closed:
                    logger.info("Position successfully closed")
            else:
                logger.warning("No position was opened or position size is 0")
        else:
            logger.error(f"Order failed: {order_result}")
    
    except Exception as e:
        logger.error(f"Error during test: {str(e)}", exc_info=True)
    
    logger.info("=== Backpack Market Order Test Complete ===")

if __name__ == "__main__":
    test_backpack_market_order("BTC", 50, "Bid")  # Changed from "Ask" to "Bid" for a long position 