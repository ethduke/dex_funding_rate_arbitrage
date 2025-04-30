import time
import datetime
from typing import Dict, List
from utils.logger import setup_logger
from model.exchanges.backpack import BackpackExchange
from model.exchanges.hyperliquid import HyperliquidExchange

# Initialize logger
logger = setup_logger(__name__)

def process_backpack_funding_rates(backpack_data: Dict) -> Dict[str, Dict]:
    """Process raw Backpack funding rate data into normalized format."""
    if not backpack_data or isinstance(backpack_data, dict) and "error" in backpack_data:
        logger.error(f"Invalid Backpack data: {backpack_data}")
        return {}
        
    result = {}
    for item in backpack_data:
        # Extract base symbol (e.g., "BTC" from "BTC_USDC_PERP")
        symbol = item.get("symbol", "").split("_")[0]
        if not symbol:
            continue
            
        result[symbol] = {
            "rate": float(item.get("fundingRate", 0)),
            "next_funding_time": item.get("nextFundingTimestamp", 0),
            "exchange": "Backpack",
            "mark_price": float(item.get("markPrice", 0)),
            "index_price": float(item.get("indexPrice", 0))
        }
    return result

def process_hyperliquid_funding_rates(hl_data: List) -> Dict[str, Dict]:
    """Process raw Hyperliquid funding rate data into normalized format."""
    if not hl_data:
        logger.error("Empty Hyperliquid data")
        return {}
        
    result = {}
    for asset_data in hl_data:
        if not isinstance(asset_data, list) or len(asset_data) < 2:
            continue
            
        asset = asset_data[0]
        for venue_data in asset_data[1]:
            if venue_data[0] == "HlPerp":  # Only use Hyperliquid's own rate
                result[asset] = {
                    "rate": float(venue_data[1].get("fundingRate", 0)),
                    "next_funding_time": venue_data[1].get("nextFundingTime", 0),
                    "exchange": "Hyperliquid"
                }
    return result

def find_best_arbitrage_opportunity(backpack_rates, hyperliquid_rates, min_diff=0.0001):
    """Find the best funding rate arbitrage opportunity between exchanges"""
    opportunities = []
    
    # Find common assets
    common_assets = set(backpack_rates.keys()).intersection(set(hyperliquid_rates.keys()))
    logger.info(f"Found {len(common_assets)} common assets between exchanges")
    
    for asset in common_assets:
        bp_rate = backpack_rates[asset]["rate"]
        hl_rate = hyperliquid_rates[asset]["rate"]
        diff = bp_rate - hl_rate
        
        # Skip if funding rate difference is too small
        if abs(diff) < min_diff:
            continue
            
        opportunities.append({
            "asset": asset,
            "bp_rate": bp_rate,
            "hl_rate": hl_rate,
            "difference": diff,
            "bp_mark_price": backpack_rates[asset].get("mark_price", 0),
            "bp_index_price": backpack_rates[asset].get("index_price", 0),
            "bp_next_funding": backpack_rates[asset].get("next_funding_time", 0),
            "strategy": "Long on Backpack, Short on Hyperliquid" if diff < 0 else "Long on Hyperliquid, Short on Backpack",
            "daily_profit_estimate": abs(diff) * 3  # 3 funding periods per day
        })
    
    # Sort by largest difference
    opportunities.sort(key=lambda x: abs(x["difference"]), reverse=True)
    return opportunities

def test_funding_arbitrage():
    """Run a complete funding rate arbitrage test with real positions"""
    logger.info("=== Starting Funding Rate Arbitrage Live Test ===")
    logger.info(f"Time: {datetime.datetime.now()}")
    
    # Test parameters
    position_size_usd = 50  # $50 on each side
    hold_time_seconds = 120  # 2 minutes
    
    try:
        # Initialize exchange clients
        logger.info("Initializing exchange clients...")
        backpack = BackpackExchange()
        hyperliquid = HyperliquidExchange()
        
        # Get funding rates from both exchanges
        logger.info("Fetching funding rates from Backpack...")
        backpack_data = backpack.get_mark_prices()
        backpack_rates = process_backpack_funding_rates(backpack_data)
        
        logger.info("Fetching funding rates from Hyperliquid...")
        hyperliquid_data = hyperliquid.get_funding_rates()
        hyperliquid_rates = process_hyperliquid_funding_rates(hyperliquid_data)
        
        # Find arbitrage opportunities
        logger.info("Analyzing funding rate arbitrage opportunities...")
        opportunities = find_best_arbitrage_opportunity(backpack_rates, hyperliquid_rates)
        
        if not opportunities:
            logger.info("No significant arbitrage opportunities found. Test aborted.")
            return
        
        # Get the best opportunity
        best_opp = opportunities[0]
        asset = best_opp["asset"]
        bp_symbol = f"{asset}_USDC_PERP"
        
        logger.info("\n=== Best Arbitrage Opportunity ===")
        logger.info(f"Asset: {asset}")
        logger.info(f"Backpack funding rate: {best_opp['bp_rate']:.6f}")
        logger.info(f"Hyperliquid funding rate: {best_opp['hl_rate']:.6f}")
        logger.info(f"Difference: {best_opp['difference']:.6f}")
        logger.info(f"Strategy: {best_opp['strategy']}")
        logger.info(f"Estimated daily profit rate: {best_opp['daily_profit_estimate']:.6f}")
        logger.info(f"Backpack mark price: ${best_opp['bp_mark_price']}")
        next_funding_time_bp = datetime.datetime.fromtimestamp(best_opp['bp_next_funding']/1000)
        logger.info(f"Backpack next funding: {next_funding_time_bp}")
        
        # Determine position sides
        long_on_backpack = "Long on Backpack" in best_opp["strategy"]
        
        # Record initial state
        logger.info("\n=== Initial State ===")
        bp_positions_before = backpack.get_positions()
        hl_positions_before = hyperliquid.get_positions()
        logger.info(f"Backpack positions before: {bp_positions_before}")
        logger.info(f"Hyperliquid positions before: {hl_positions_before}")
        
        # Open positions
        logger.info("\n=== Opening Positions ===")
        bp_order = None
        hl_order = None
        
        # Backpack position
        if long_on_backpack:
            logger.info(f"Opening LONG position on Backpack for {bp_symbol}...")
            bp_order = backpack.place_market_order(bp_symbol, "Bid", quote_quantity=position_size_usd)
        else:
            logger.info(f"Opening SHORT position on Backpack for {bp_symbol}...")
            bp_order = backpack.place_market_order(bp_symbol, "Ask", quote_quantity=position_size_usd)
        
        logger.info(f"Backpack order result: {bp_order}")
        
        # Hyperliquid position (opposite side)
        if long_on_backpack:
            logger.info(f"Opening SHORT position on Hyperliquid for {asset}...")
            hl_order = hyperliquid.place_market_order(asset, "Ask", quote_quantity=position_size_usd)
        else:
            logger.info(f"Opening LONG position on Hyperliquid for {asset}...")
            hl_order = hyperliquid.place_market_order(asset, "Bid", quote_quantity=position_size_usd)
        
        logger.info(f"Hyperliquid order result: {hl_order}")
        
        # Check positions after opening
        logger.info("\n=== Positions After Opening ===")
        time.sleep(2)  # Wait for orders to process
        
        bp_positions_after = backpack.get_positions()
        hl_positions_after = hyperliquid.get_positions()
        
        bp_position = None
        for pos in bp_positions_after:
            if pos.get("symbol") == bp_symbol:
                bp_position = pos
                break
                
        hl_position = None
        for pos in hl_positions_after:
            if pos.get("coin") == asset:
                hl_position = pos
                break
                
        if bp_position:
            logger.info(f"Backpack position: {bp_position}")
            logger.info(f"Position size: {bp_position.get('positionSize')}")
            logger.info(f"Entry price: {bp_position.get('entryPrice')}")
        else:
            logger.info("No Backpack position found")
            
        if hl_position:
            logger.info(f"Hyperliquid position: {hl_position}")
            logger.info(f"Position size: {hl_position.get('position')}")
            logger.info(f"Entry price: {hl_position.get('entryPx')}")
        else:
            logger.info("No Hyperliquid position found")
        
        # Hold positions for the specified time
        logger.info(f"\n=== Holding positions for {hold_time_seconds} seconds ===")
        time.sleep(hold_time_seconds)
        
        # Get updated funding rates
        logger.info("\n=== Updated Funding Rates ===")
        
        bp_data_after = backpack.get_mark_prices(bp_symbol)
        hl_data_after = hyperliquid.get_funding_rates()
        
        bp_rate_after = None
        bp_mark_after = None
        for item in bp_data_after:
            if item.get("symbol") == bp_symbol:
                bp_rate_after = float(item.get("fundingRate", 0))
                bp_mark_after = float(item.get("markPrice", 0))
                break
                
        hl_rate_after = None
        for rate_data in hl_data_after:
            if isinstance(rate_data, list) and len(rate_data) > 0 and rate_data[0] == asset:
                for venue in rate_data[1]:
                    if venue[0] == "HlPerp":
                        hl_rate_after = float(venue[1].get("fundingRate", 0))
                        break
                        
        logger.info(f"Backpack updated funding rate: {bp_rate_after}")
        logger.info(f"Backpack updated mark price: {bp_mark_after}")
        logger.info(f"Hyperliquid updated funding rate: {hl_rate_after}")
        
        # Close positions
        logger.info("\n=== Closing Positions ===")
        
        # Close Backpack position
        if bp_position:
            logger.info(f"Closing Backpack position for {bp_symbol}...")
            bp_close = backpack.close_position(bp_symbol, float(bp_position.get("positionSize", 0)))
            logger.info(f"Backpack close result: {bp_close}")
        
        # Close Hyperliquid position
        if hl_position:
            logger.info(f"Closing Hyperliquid position for {asset}...")
            hl_close = hyperliquid.close_position(asset)
            logger.info(f"Hyperliquid close result: {hl_close}")
        
        # Final check
        logger.info("\n=== Final State ===")
        time.sleep(2)
        
        bp_final = backpack.get_positions()
        hl_final = hyperliquid.get_positions()
        
        bp_still_open = False
        for pos in bp_final:
            if pos.get("symbol") == bp_symbol and float(pos.get("positionSize", 0)) != 0:
                bp_still_open = True
                logger.info(f"Backpack position still open: {pos}")
                
        hl_still_open = False
        for pos in hl_final:
            if pos.get("coin") == asset and float(pos.get("position", 0)) != 0:
                hl_still_open = True
                logger.info(f"Hyperliquid position still open: {pos}")
                
        if not bp_still_open and not hl_still_open:
            logger.info("All positions successfully closed")
        
    except Exception as e:
        logger.error(f"Error during test: {str(e)}", exc_info=True)
    
    logger.info("=== Funding Rate Arbitrage Test Complete ===")

if __name__ == "__main__":
    test_funding_arbitrage() 