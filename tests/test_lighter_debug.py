import asyncio
import logging
from utils.config import CONFIG
from model.exchanges.lighter import LighterExchange

logging.basicConfig(level=logging.INFO)

async def debug_order_parameters():
    """Debug order parameters to see what's being sent"""
    
    print("Debugging order parameters...")
    
    try:
        # Create Lighter exchange instance
        lt_client = LighterExchange(use_ws=False)
        
        # Initialize account
        await lt_client._ensure_account_initialized()
        
        symbol = "ETH"
        quote_amount = 100.0
        
        # Debug the conversion
        market_id = await lt_client._get_market_id(symbol)
        estimated_price = 3000  # ETH price estimate
        base_amount = int((quote_amount / estimated_price) * 1e6)
        
        print(f"Debug parameters:")
        print(f"  Symbol: {symbol}")
        print(f"  Market ID: {market_id}")
        print(f"  Quote Amount: ${quote_amount}")
        print(f"  Estimated Price: ${estimated_price}")
        print(f"  Base Amount: {base_amount} (micro units)")
        print(f"  Base Amount (normal): {base_amount / 1e6}")
        
        # Test SELL order parameters
        side = "SELL"
        is_ask = side.upper() == "SELL"
        print(f"\nSELL order parameters:")
        print(f"  Side: {side}")
        print(f"  is_ask: {is_ask}")
        print(f"  Market ID: {market_id}")
        
        # Try to place order
        result = await lt_client.place_market_order(
            symbol=symbol,
            side=side,
            quote_quantity=quote_amount
        )
        print(f"  Result: {result}")
        
        await lt_client.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_order_parameters()) 