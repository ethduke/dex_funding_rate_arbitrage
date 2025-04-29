from utils.logger import setup_logger
from model.exchanges.backpack import BackpackExchange
from model.exchanges.hyperliquid import HyperliquidExchange
from model.core.arbitrage import find_arbitrage_opportunities
from utils.config import CONFIG


logger = setup_logger()

def main():
    logger.info("Fetching funding rates from exchanges...")
    
    try:
        backpack = BackpackExchange()
        hyperliquid = HyperliquidExchange()

        investment_amount = 1000 
        
        # Get and process data using exchange methods
        backpack_data = backpack.get_mark_prices()
        if "error" in backpack_data:
            logger.error(f"Failed to get Backpack data: {backpack_data['error']}")
            return
            
        hyperliquid_data = hyperliquid.get_funding_rates()
        if not hyperliquid_data:
            logger.error("Failed to get Hyperliquid data")
            return
        
        # Process data using exchange-specific methods
        backpack_rates = backpack.process_funding_rates(backpack_data)
        hyperliquid_rates = hyperliquid.process_funding_rates(hyperliquid_data)
        
        # Prepare exchange rates dictionary
        exchange_rates = {
            "Backpack": backpack_rates,
            "Hyperliquid": hyperliquid_rates
        }
        
        # Find arbitrage opportunities
        opportunities = find_arbitrage_opportunities(exchange_rates)
        
        # Calculate profit based on investment amount
        for opp in opportunities:
            opp['daily_profit_usd'] = investment_amount * abs(opp['potential_profit'])
            opp['apr'] = abs(opp['potential_profit']) * 365 * 100
        
        # Display arbitrage opportunities
        logger.info(f"\n---- Top 5 Arbitrage Opportunities (Based on ${investment_amount} Investment) ----")
        if not opportunities:
            logger.info("No significant arbitrage opportunities found")
        else:
            # Sort by APR (highest first)
            opportunities.sort(key=lambda x: x['apr'], reverse=True)
            
            for i, opp in enumerate(opportunities[:5]):
                logger.info(f"{i+1}. {opp['asset']}: Potential Profit = {opp['potential_profit']:.6f}")
                logger.info(f"   Strategy: {opp['strategy']}")
                logger.info(f"   Daily Profit: ${opp['daily_profit_usd']:.2f} (APR: {opp['apr']:.2f}%)")
                
                # Display funding rates
                for ex in opp['exchanges']:
                    logger.info(f"   {ex} Rate: {opp['actions'][ex]['rate']:.6f}")
            
            # Execute the best opportunity with $100 position size
            if opportunities:
                best_opp = opportunities[0]
                execute_arbitrage(backpack, hyperliquid, best_opp, 100)
                
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {str(e)}")
        return

def execute_arbitrage(backpack, hyperliquid, opportunity, position_size_usd):
    """
    Execute arbitrage by opening positions on both exchanges.
    
    Args:
        opportunity: The arbitrage opportunity to execute
        position_size_usd: Position size in USD for each side
    """
    logger.info(f"\n---- Executing Arbitrage for {opportunity['asset']} with ${position_size_usd} position size ----")
        
    # Get the exchanges and actions
    long_exchange = opportunity['long_exchange']
    short_exchange = opportunity['short_exchange']
    
    # Execute the trades
    try:
        # Open long position
        if long_exchange == "Backpack":
            logger.info(f"Opening LONG position on Backpack for {opportunity['asset']}")
            result = backpack.open_long(opportunity['asset'], position_size_usd)
            logger.info(f"Backpack LONG result: {result}")
            logger.info(f"Opening SHORT position on Hyperliquid for {opportunity['asset']}")
            result = hyperliquid.open_short(opportunity['asset'], position_size_usd)
            logger.info(f"Hyperliquid SHORT result: {result}")
        elif long_exchange == "Hyperliquid":
            logger.info(f"Opening LONG position on Hyperliquid for {opportunity['asset']}")
            result = hyperliquid.open_long(opportunity['asset'], position_size_usd)
            logger.info(f"Hyperliquid LONG result: {result}")
            logger.info(f"Opening SHORT position on Backpack for {opportunity['asset']}")
            result = backpack.open_short(opportunity['asset'], position_size_usd)
            logger.info(f"Backpack SHORT result: {result}")
        
    except Exception as e:
        logger.error(f"Error executing arbitrage: {str(e)}")
        try:
            if long_exchange == "Backpack":
                backpack.close_asset_position(opportunity['asset'])
            elif long_exchange == "Hyperliquid":
                hyperliquid.close_position(opportunity['asset'])
                
            if short_exchange == "Backpack":
                backpack.close_asset_position(opportunity['asset'])
            elif short_exchange == "Hyperliquid":
                hyperliquid.close_position(opportunity['asset'])
        except Exception as close_error:
            logger.error(f"Error closing positions after failure: {str(close_error)}")

if __name__ == "__main__":
    main()
