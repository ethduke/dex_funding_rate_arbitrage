import asyncio
import logging
from utils.config import CONFIG
from model.exchanges.lighter import LighterExchange

logging.basicConfig(level=logging.INFO)

async def test_lighter_order_placement():
    """Test Lighter order placement with buy and sell orders"""
    
    print("Testing Lighter order placement with buy/sell orders...")
    
    try:
        # Create Lighter exchange instance
        lt_client = LighterExchange(use_ws=False)
        
        print("Lighter exchange created")
        
        # Initialize account
        await lt_client._ensure_account_initialized()
        print("Account initialized")
        
        # Check if SignerClient is available
        if not lt_client.signer_client:
            print("SignerClient not available - cannot place orders")
            print("   Make sure LIGHTER_PRIVATE_KEY is set in environment")
            return
        
        print("SignerClient available for order placement")
        
        # Get current funding rates for reference
        funding_rates = await lt_client.get_funding_rates()
        print(f"Available markets: {list(funding_rates.keys())[:5]}...")  # Show first 5 markets
        
        # Test with BTC (market 1) - smaller position
        symbol = "BTC"
        quote_amount = 10.0  # $10 USD
        
        print(f"\nPlacing SELL order for ${quote_amount} of {symbol} (short position)...")
        sell_result = await lt_client.place_market_order(
            symbol=symbol,
            side="SELL",
            quote_quantity=quote_amount
        )
        
        print(f"Sell order result: {sell_result}")
        
        if "error" in sell_result:
            print(f"Sell order failed: {sell_result['error']}")
            return
        
        print("Sell order placed successfully!")
        
        # Wait a moment for order to process
        print("Waiting 10 seconds for order to process...")
        await asyncio.sleep(10)
        
        # Check positions
        print("\nChecking positions...")
        positions = await lt_client.get_positions()
        print(f"Current positions: {positions}")
        
        # Find the position we just created
        target_position = None
        for position in positions:
            if position.get("symbol") == symbol:
                target_position = position
                break
        
        if target_position:
            print(f"Found {symbol} position: {target_position}")
            
            # Check if position size is significant enough to close
            position_size = abs(target_position["size"])
            if position_size < 0.001:  # Very small position
                print(f"Position size ({position_size}) is very small, might not close effectively")
            
            # Place buy order to close short position
            print(f"\n🟢 Placing BUY order to close {symbol} short position...")
            
            # Try different approaches for closing the position
            print("Trying approach 1: Regular market buy order...")
            buy_result = await lt_client.place_market_order(
                symbol=symbol,
                side="BUY",
                quantity=position_size,
                reduce_only=True  # Add reduce_only flag
            )
            
            print(f"Buy order result: {buy_result}")
            
            if "error" in buy_result:
                print(f"Buy order failed: {buy_result['error']}")
            else:
                print("Buy order placed successfully!")
                
                # Wait and check if position closed
                print("Waiting 10 seconds to check if position closed...")
                await asyncio.sleep(10)
                
                check_positions = await lt_client.get_positions()
                btc_position = None
                for pos in check_positions:
                    if pos.get("symbol") == symbol:
                        btc_position = pos
                        break
                
                if btc_position and abs(btc_position["size"]) > 0.001:
                    print(f"Position still exists: {btc_position}")
                    print("Trying approach 2: Using close_position method...")
                    
                    # Try using the close_position method instead
                    close_result = await lt_client.close_position(symbol)
                    print(f"Close position result: {close_result}")
                    
                    if "error" in close_result:
                        print(f"Close position failed: {close_result['error']}")
                    else:
                        print("Close position order placed successfully!")
                        
                        # Wait and check again
                        print("Waiting 10 more seconds...")
                        await asyncio.sleep(10)
                        
                        final_check = await lt_client.get_positions()
                        print(f"Final position check: {final_check}")
                else:
                    print(f"{symbol} position successfully closed!")
        else:
            print(f"No {symbol} position found to close")
        
        # Wait longer for sell order to process
        print("\nWaiting 15 seconds for sell order to process...")
        await asyncio.sleep(15)
        
        # Check positions multiple times to see if it's closing
        print("\nChecking final positions...")
        for i in range(3):
            final_positions = await lt_client.get_positions()
            print(f"Check {i+1}: {final_positions}")
            
            # Check if our position was closed
            btc_position = None
            for pos in final_positions:
                if pos.get("symbol") == symbol:
                    btc_position = pos
                    break
            
            if btc_position:
                print(f"Position still exists: {btc_position}")
                if i < 2:  # Wait more if position still exists
                    print("Waiting 5 more seconds...")
                    await asyncio.sleep(5)
            else:
                print(f"{symbol} position successfully closed!")
                break
        
        # Close connections
        await lt_client.close()
        print("Test completed!")
        
    except Exception as e:
        print(f"Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_lighter_order_placement()) 