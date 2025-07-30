import asyncio
import queue
import time
from typing import Dict, List, Optional, Type
from datetime import datetime
from utils.logger import setup_logger

from model.exchanges.backpack import BackpackExchange
from model.exchanges.hyperliquid import HyperliquidExchange
from model.exchanges.lighter import LighterExchange
from model.exchanges.base import BaseExchange
from utils.config import CONFIG

logger = setup_logger(__name__)

# Time constants
SECONDS_PER_HOUR = 3600

class FundingArbitrageEngine:
    def __init__(self, 
                 min_rate_difference: float,
                 position_size: float,
                 min_hold_time_seconds: int,
                 magnitude_reduction_threshold: float,
                 check_interval_minutes: int,
                 exchanges: Optional[List[Type[BaseExchange]]] = None,
                 use_ws: bool = True):
        """Initialize the funding rate arbitrage engine."""
        logger.info("Initializing FundingArbitrageEngine...")
        self.position_size = position_size
        self.min_rate_difference = min_rate_difference
        self.min_hold_time_seconds = min_hold_time_seconds
        self.magnitude_reduction_threshold = magnitude_reduction_threshold
        self.check_interval_minutes = check_interval_minutes
        self.use_ws = use_ws
        
        self.active_positions = {}  # Track active arbitrage positions
        self.position_stats = {}  # Track position statistics
        self.running = False
        
        # Cancel event for stopping scheduled tasks
        self.stop_event = asyncio.Event()
        
        # Lock to prevent concurrent checks
        self._check_lock = asyncio.Lock()
        
        # Initialize exchange clients
        logger.info("Initializing exchange clients...")
        if exchanges is None:
            exchanges = [BackpackExchange, HyperliquidExchange, LighterExchange]
            
        self.exchanges = {}
        for exchange_class in exchanges:
            exchange_name = exchange_class.__name__.replace('Exchange', '')
            logger.info(f"Initializing {exchange_name} exchange...")
            try:
                # Initialize Backpack and Lighter with WebSocket support if enabled
                if exchange_name == "Backpack" or exchange_name == "Lighter":
                    self.exchanges[exchange_name] = exchange_class(use_ws=self.use_ws)
                    logger.info(f"Successfully initialized {exchange_name} exchange with WebSocket={self.use_ws}")
                else:
                    self.exchanges[exchange_name] = exchange_class()
                    logger.info(f"Successfully initialized {exchange_name} exchange")
            except Exception as e:
                logger.error(f"Error initializing {exchange_name} exchange: {str(e)}", exc_info=True)
        
        # Task for periodic checking
        self.check_task = None
        
        logger.debug("FundingArbitrageEngine initialization complete")
        
    async def start(self):
        """Start the arbitrage engine."""
        logger.info("Starting arbitrage engine...")
        
        if not self.running:
            self.running = True
            self.stop_event.clear()
            
            # Initialize WebSocket connections if enabled
            if self.use_ws:
                backpack = self.exchanges.get("Backpack")
                if backpack:
                    logger.info("Initializing Backpack WebSocket connection...")
                    connected = await backpack.initialize_ws()
                    logger.info(f"Backpack WebSocket connected: {connected}")
                
                lighter = self.exchanges.get("Lighter")
                if lighter:
                    logger.info("Initializing Lighter WebSocket connection...")
                    connected = await lighter.initialize_ws()
                    logger.info(f"Lighter WebSocket connected: {connected}")
            
            # Schedule initial check after a short delay
            logger.debug("Scheduling initial check after 5 seconds")
            asyncio.create_task(self._delayed_initial_check())
            
            # Schedule periodic checks
            logger.debug(f"Scheduling opportunity check every {self.check_interval_minutes} minutes")
            self.check_task = asyncio.create_task(self._periodic_check())
            
            logger.info("Funding arbitrage engine started")
        else:
            logger.warning("Engine already running. Not starting a new instance.")
            
    async def stop(self):
        """Stop the arbitrage engine."""
        logger.info("Stopping arbitrage engine...")
        
        if self.running:
            self.running = False
            self.stop_event.set()
            
            # Close WebSocket connections if enabled
            if self.use_ws:
                backpack = self.exchanges.get("Backpack")
                if backpack:
                    logger.info("Closing Backpack WebSocket connection...")
                    await backpack.close_ws()
                
                lighter = self.exchanges.get("Lighter")
                if lighter:
                    logger.info("Closing Lighter WebSocket connection...")
                    await lighter.close_ws()
            
            # Cancel periodic check task if it exists
            if self.check_task:
                self.check_task.cancel()
                try:
                    await self.check_task
                except asyncio.CancelledError:
                    pass
                
            logger.info("Funding arbitrage engine stopped")
        else:
            logger.warning("Engine already stopped")
    
    async def _delayed_initial_check(self):
        """Run initial check after a short delay."""
        await asyncio.sleep(5)  # 5 second delay
        await self.check_opportunities()
    
    async def _periodic_check(self):
        """Run periodic checks at the specified interval."""
        try:
            while self.running:
                # Calculate interval in seconds
                interval = self.check_interval_minutes * 60
                
                # Wait for the specified interval or until stop_event is set
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=interval)
                    # If we reach here, the stop_event was set
                    break
                except asyncio.TimeoutError:
                    # Timeout means we should run another check
                    pass
                
                if self.running:  # Double-check we're still running
                    await self.check_opportunities()
        except Exception as e:
            logger.error(f"Error in periodic check task: {str(e)}", exc_info=True)
        finally:
            logger.info("Periodic check task ended")

    def find_arbitrage_opportunities(
        self,
        exchange_rates: Dict[str, Dict[str, Dict]], 
        min_diff: float = 0.00001
    ) -> List[Dict]:
        """
        Find arbitrage opportunities by comparing funding rates between exchanges.
        For each asset:
        - If funding rate is negative on exchange A, we go LONG on A (we receive funding)
        - If funding rate is positive on exchange B, we go SHORT on B (we pay funding)
        The profit comes from the difference in funding rates.
        
        Args:
            exchange_rates: Dict of exchange names to their asset rates
                        {exchange_name: {asset: {rate: float, ...}}}
            min_diff: Minimum funding rate difference to consider
        
        Returns:
            List of arbitrage opportunities with detailed exchange and action information
        """
        logger.info("Finding arbitrage opportunities...")
        opportunities = []
        exchanges = list(exchange_rates.keys())
        logger.info(f"Comparing rates between exchanges: {exchanges}")
        
        for i in range(len(exchanges)):
            for j in range(i+1, len(exchanges)):
                ex1, ex2 = exchanges[i], exchanges[j]
                
                common_assets = set(exchange_rates[ex1].keys()) & set(exchange_rates[ex2].keys())
                logger.debug(f"Found {len(common_assets)} common assets between {ex1} and {ex2}")
                
                for asset in common_assets:
                    rate1 = exchange_rates[ex1][asset]["rate"]
                    rate2 = exchange_rates[ex2][asset]["rate"]
                    
                    potential_profit = abs(rate1) + abs(rate2)
                    
                    if potential_profit >= min_diff:
                        long_ex = ex1 if rate1 < 0 else ex2
                        short_ex = ex2 if rate1 < 0 else ex1
                        long_rate = rate1 if rate1 < 0 else rate2
                        short_rate = rate2 if rate1 < 0 else rate1
                        
                        # Get additional exchange data
                        long_ex_data = exchange_rates[long_ex][asset]
                        short_ex_data = exchange_rates[short_ex][asset]
                        
                        opportunities.append({
                            "asset": asset,
                            "exchanges": [ex1, ex2],
                            "rates": {ex1: rate1, ex2: rate2},
                            "potential_profit": potential_profit,
                            "strategy": f"LONG on {long_ex} (rate: {long_rate:.6f}), SHORT on {short_ex} (rate: {short_rate:.6f})",
                            "estimated_daily_profit": potential_profit * 3,  # Assuming 8-hour funding periods
                            "actions": {
                                long_ex: {
                                    "action": "LONG",
                                    "rate": long_ex_data["rate"],
                                    "next_funding_time": long_ex_data.get("next_funding_time", 0)
                                },
                                short_ex: {
                                    "action": "SHORT",
                                    "rate": short_ex_data["rate"],
                                    "next_funding_time": short_ex_data.get("next_funding_time", 0)
                                }
                            },
                            "long_exchange": long_ex,
                            "short_exchange": short_ex
                        })
        
        # Sort by largest potential profit first
        opportunities.sort(key=lambda x: x["potential_profit"], reverse=True)
        logger.info(f"Found {len(opportunities)} potential arbitrage opportunities")
        return opportunities

            
    async def check_opportunities(self):
        """Check for new arbitrage opportunities and manage existing positions."""
        # Use a lock to prevent concurrent execution
        if self._check_lock.locked():
            logger.warning("Another check_opportunities is already running. Skipping this execution.")
            return
            
        async with self._check_lock:
            logger.info("Checking for funding rate arbitrage opportunities...")
            
            try:
                # Get latest funding rates
                exchange_rates = {}
                logger.debug("Fetching funding rates from exchanges...")
                
                # Get funding rates from each exchange
                for exchange_name, exchange in self.exchanges.items():
                    try:
                        # Handle async vs sync get_funding_rates methods
                        if exchange_name == "Lighter":
                            data = await exchange.get_funding_rates()
                        else:
                            data = exchange.get_funding_rates()
                        
                        # Process rates using exchange's method 
                        # (all exchange classes must implement process_funding_rates)
                        exchange_rates[exchange_name] = exchange.process_funding_rates(data)
                        
                    except Exception as e:
                        logger.error(f"Error getting funding rates from {exchange_name}: {str(e)}", exc_info=True)
                        
                        # Check if the exception contains a 503 error code
                        error_str = str(e).lower()
                        if "503" in error_str or "service temporarily unavailable" in error_str:
                            logger.error(f"{exchange_name} API is unavailable (503 Service Temporarily Unavailable detected in error). Exiting program.")
                            await self.stop()
                
                # Find arbitrage opportunities
                opportunities = self.find_arbitrage_opportunities(exchange_rates, min_diff=self.min_rate_difference)
                
                # Calculate profit based on position size
                logger.info("Calculating potential profits...")
                for opp in opportunities:
                    opp['daily_profit_usd'] = self.position_size * abs(opp['potential_profit'])
                    opp['apr'] = abs(opp['potential_profit']) * 365 * 100
                
                # Sort by APR (highest first)
                if opportunities:
                    opportunities.sort(key=lambda x: x['apr'], reverse=True)
                    
                    # Display top N opportunities
                    logger.info(f"\n---- Top 3 Arbitrage Opportunities (Based on ${self.position_size} Position Size) ----")
                    for i, opp in enumerate(opportunities[:3]):
                        logger.info(f"{i+1}. {opp['asset']}: Potential Profit = {opp['potential_profit']:.6f}")
                        logger.info(f"   Strategy: {opp['strategy']}")
                        logger.info(f"   Daily Profit: ${opp['daily_profit_usd']:.2f} (APR: {opp['apr']:.2f}%)")
                        
                        # Display funding rates
                        for ex in opp['exchanges']:
                            logger.info(f"   {ex} Rate: {opp['actions'][ex]['rate']:.6f}")
                else:
                    logger.info("No significant arbitrage opportunities found")
                
                # Check monitoring tasks for active positions
                await self._manage_active_monitors()
                
                # Enter new positions if not at max capacity
                await self._enter_new_positions(opportunities)
                
                logger.debug("Opportunity check cycle completed successfully")
                
            except Exception as e:
                logger.error(f"Error checking opportunities: {str(e)}", exc_info=True)
            
    async def _manage_active_monitors(self):
        """Manage active position monitors."""
        # Nothing to do if no active positions
        if not self.active_positions:
            return
            
        # Check for completed monitors and process results
        for asset, position in list(self.active_positions.items()):
            logger.info(f"Checking monitor for {asset}...")
            monitor_task = position.get('monitor_task')
            if monitor_task and monitor_task.done():
                logger.info(f"Monitor for {asset} is complete, processing results")
                try:
                    # Get stats from completed monitor
                    stats = monitor_task.result()
                    
                    # Store stats for this position
                    self.position_stats[asset] = stats
                    
                    # Log position results
                    logger.info(f"\n---- Arbitrage Cycle Summary for {asset} ----")
                    logger.info(f"Initial Strategy: {position['strategy']}")
                    logger.info(f"Position Size: ${self.position_size}")
                    logger.info(f"Duration: {stats['duration_hours']:.2f} hours")
                    logger.info(f"Total PnL: ${stats['total_pnl']:.2f} ({stats['pnl_percent']:.2f}%)")
                    logger.info(f"  Entry/Exit Spread PnL: ${stats['price_pnl']:.2f}")
                    logger.info(f"  Funding Payments: ${stats['funding_pnl']:.2f}")
                    logger.info(f"  Trading Fees: ${stats['fees']:.2f}")
                    logger.info(f"Annualized ROI: {stats['annualized_roi']:.2f}%")
                    
                    # Remove from active positions
                    del self.active_positions[asset]
                    logger.info(f"Position for {asset} completed and removed from tracking")
                    
                except Exception as e:
                    logger.error(f"Error processing completed monitor for {asset}: {str(e)}", exc_info=True)
                    # Remove problematic position from tracking
                    
    async def _enter_new_positions(self, opportunities: List[Dict]):
        """Enter new arbitrage positions based on opportunities."""
        logger.info("Checking for new positions to enter...")
        # Limit the number of concurrent positions
        max_positions = int(CONFIG.get('MAX_CONCURRENT_POSITIONS'))
        current_pos_count = len(self.active_positions)
        
        logger.debug(f"Current positions: {current_pos_count}, Max allowed: {max_positions}")
        
        # If we're already at max positions, don't enter more
        if current_pos_count >= max_positions:
            logger.debug(f"Already at maximum positions ({max_positions}), not entering new positions")
            return
            
        # Calculate how many new positions we can enter
        available_slots = max_positions - current_pos_count
        logger.debug(f"Available slots for new positions: {available_slots}")
        
        # Enter new positions up to the limit
        entered = 0
        for opp in opportunities:
            # Skip if we already have a position for this asset
            if opp['asset'] in self.active_positions:
                logger.info(f"Skipping {opp['asset']} - already have an active position")
                continue
                
            # Enter position
            logger.info(f"Attempting to open new position for {opp['asset']}...")
            success = await self._open_new_position(opp)
            if success:
                entered += 1
                logger.info(f"Successfully opened new position ({entered}/{available_slots})")
                
            # Stop if we've filled all available slots
            if entered >= available_slots:
                logger.debug("All available position slots filled")
                break
                
        logger.debug(f"Entered {entered} new positions")
            
    async def _open_new_position(self, opportunity: Dict) -> bool:
        """Open a new arbitrage position based on an opportunity."""
        asset = opportunity['asset']
        logger.info(f"\n---- Starting arbitrage cycle for {asset} ----")
        
        try:
            # Execute the arbitrage
            result = await self._execute_arbitrage(opportunity)
            
            # Check if execution was successful
            if result is None:
                logger.warning(f"Failed to execute arbitrage for {asset}, skipping monitor creation")
                return False
            
            # Start monitoring task
            backpack = self.exchanges.get("Backpack")
            hyperliquid = self.exchanges.get("Hyperliquid")
            lighter = self.exchanges.get("Lighter")
            
            # Create the monitoring task
            monitor_task = asyncio.create_task(
                self.monitor_funding_rates(backpack, hyperliquid, lighter, opportunity)
            )
            
            # Track the position and its monitor
            self.active_positions[asset] = {
                'asset': asset,
                'long_exchange': opportunity['long_exchange'],
                'short_exchange': opportunity['short_exchange'],
                'entry_time': datetime.now(),
                'strategy': opportunity['strategy'],
                'monitor_task': monitor_task
            }
            
            logger.info(f"Successfully opened and started monitoring for {asset}")
            return True
            
        except Exception as e:
            logger.error(f"Error opening position for {asset}: {str(e)}", exc_info=True)
            return False 
    
    async def _execute_arbitrage(self, opportunity, position_size_usd=None):
        """Execute arbitrage by opening positions on both exchanges."""
        if position_size_usd is None:
            position_size_usd = self.position_size
            
        asset = opportunity['asset']
        logger.info(f"\n---- Executing Arbitrage for {asset} with ${position_size_usd} position size ----")
            
        # Get the exchanges and actions
        long_exchange = opportunity['long_exchange']
        short_exchange = opportunity['short_exchange']
        
                    # Execute the trades
        try:
            backpack = self.exchanges.get("Backpack")
            hyperliquid = self.exchanges.get("Hyperliquid")
            lighter = self.exchanges.get("Lighter")
            
            # Check available balances on both exchanges first
            logger.info("Checking available balances on both exchanges...")
                        
            # Detail what we're going to do
            logger.info(f"TRADE PLAN: LONG on {long_exchange}, SHORT on {short_exchange} for {asset}")
            
            # Check if Lighter has order placement capability
            if "Lighter" in [long_exchange, short_exchange]:
                if not lighter or not hasattr(lighter, 'signer_client') or not lighter.signer_client:
                    logger.warning("Lighter order placement not available (SignerClient not initialized) - skipping this opportunity")
                    return None
                else:
                    logger.info("Lighter order placement available - will attempt orders")
            
            long_result = None
            short_result = None
            long_success = False
            short_success = False
            
            # Open long position
            if long_exchange == "Backpack":
                logger.info(f"Opening LONG position on Backpack for {asset} with size ${position_size_usd}")
                try:
                    long_result = backpack.open_long(asset, position_size_usd)
                    long_success = "status" not in long_result or long_result["status"] != "error"
                    logger.debug(f"Backpack LONG result: {long_result}")
                    if not long_success:
                        logger.error(f"Failed to open LONG position on Backpack: {long_result.get('message', 'Unknown error')}")
                        return None
                except Exception as e:
                    logger.error(f"Failed to open LONG position on Backpack: {str(e)}", exc_info=True)
                    return None
                
                logger.info(f"Opening SHORT position on Hyperliquid for {asset} with size ${position_size_usd}")
                try:
                    short_result = hyperliquid.open_short(asset, position_size_usd)
                    short_success = "status" not in short_result or short_result["status"] != "error"
                    logger.debug(f"Hyperliquid SHORT result: {short_result}")
                    if not short_success:
                        logger.error(f"Failed to open SHORT position on Hyperliquid: {short_result.get('message', 'Unknown error')}")
                        # Try to close the long position we just opened
                        try:
                            logger.debug(f"Closing LONG position on Backpack for {asset} after SHORT position failure")
                            backpack.close_asset_position(asset)
                        except Exception as close_e:
                            logger.error(f"Failed to close LONG position after SHORT position failure: {str(close_e)}", exc_info=True)
                        return None
                except Exception as e:
                    logger.error(f"Failed to open SHORT position on Hyperliquid: {str(e)}", exc_info=True)
                    # Try to close the long position we just opened
                    try:
                        logger.debug(f"Closing LONG position on Backpack for {asset} after SHORT position failure")
                        backpack.close_asset_position(asset)
                    except Exception as close_e:
                        logger.error(f"Failed to close LONG position after SHORT position failure: {str(close_e)}", exc_info=True)
                    return None
            elif long_exchange == "Hyperliquid":
                logger.info(f"Opening LONG position on Hyperliquid for {asset} with size ${position_size_usd}")
                try:
                    long_result = hyperliquid.open_long(asset, position_size_usd)
                    long_success = "status" not in long_result or long_result["status"] != "error"
                    logger.debug(f"Hyperliquid LONG result: {long_result}")
                    if not long_success:
                        logger.error(f"Failed to open LONG position on Hyperliquid: {long_result.get('message', 'Unknown error')}")
                        return None
                except Exception as e:
                    logger.error(f"Failed to open LONG position on Hyperliquid: {str(e)}", exc_info=True)
                    return None
                
                logger.info(f"Opening SHORT position on Backpack for {asset} with size ${position_size_usd}")
                try:
                    short_result = backpack.open_short(asset, position_size_usd)
                    short_success = "status" not in short_result or short_result["status"] != "error"
                    logger.debug(f"Backpack SHORT result: {short_result}")
                    if not short_success:
                        logger.error(f"Failed to open SHORT position on Backpack: {short_result.get('message', 'Unknown error')}")
                        # Try to close the long position we just opened
                        try:
                            logger.debug(f"Closing LONG position on Hyperliquid for {asset} after SHORT position failure")
                            hyperliquid.close_position(asset)
                        except Exception as close_e:
                            logger.error(f"Failed to close LONG position after SHORT position failure: {str(close_e)}", exc_info=True)
                        return None
                except Exception as e:
                    logger.error(f"Failed to open SHORT position on Backpack: {str(e)}", exc_info=True)
                    # Try to close the long position we just opened
                    try:
                        logger.debug(f"Closing LONG position on Hyperliquid for {asset} after SHORT position failure")
                        hyperliquid.close_position(asset)
                    except Exception as close_e:
                        logger.error(f"Failed to close LONG position after SHORT position failure: {str(close_e)}", exc_info=True)
                    return None
            elif long_exchange == "Lighter":
                logger.info(f"Opening LONG position on Lighter for {asset} with size ${position_size_usd}")
                try:
                    long_result = await lighter.open_long(asset, position_size_usd)
                    long_success = "error" not in long_result
                    logger.debug(f"Lighter LONG result: {long_result}")
                    if not long_success:
                        logger.error(f"Failed to open LONG position on Lighter: {long_result.get('error', 'Unknown error')}")
                        logger.warning("Lighter order placement failed - this is expected without valid credentials")
                        return None
                except Exception as e:
                    logger.error(f"Failed to open LONG position on Lighter: {str(e)}", exc_info=True)
                    logger.warning("Lighter order placement failed - this is expected without valid credentials")
                    return None
                
                logger.info(f"Opening SHORT position on Backpack for {asset} with size ${position_size_usd}")
                try:
                    short_result = backpack.open_short(asset, position_size_usd)
                    short_success = "status" not in short_result or short_result["status"] != "error"
                    logger.debug(f"Backpack SHORT result: {short_result}")
                    if not short_success:
                        logger.error(f"Failed to open SHORT position on Backpack: {short_result.get('message', 'Unknown error')}")
                        # Try to close the long position we just opened
                        try:
                            logger.debug(f"Closing LONG position on Lighter for {asset} after SHORT position failure")
                            await lighter.close_position(asset)
                        except Exception as close_e:
                            logger.error(f"Failed to close LONG position after SHORT position failure: {str(close_e)}", exc_info=True)
                        return None
                except Exception as e:
                    logger.error(f"Failed to open SHORT position on Backpack: {str(e)}", exc_info=True)
                    # Try to close the long position we just opened
                    try:
                        logger.debug(f"Closing LONG position on Lighter for {asset} after SHORT position failure")
                        await lighter.close_position(asset)
                    except Exception as close_e:
                        logger.error(f"Failed to close LONG position after SHORT position failure: {str(close_e)}", exc_info=True)
                    return None
            elif long_exchange == "Backpack" and short_exchange == "Lighter":
                logger.info(f"Opening LONG position on Backpack for {asset} with size ${position_size_usd}")
                try:
                    long_result = backpack.open_long(asset, position_size_usd)
                    long_success = "status" not in long_result or long_result["status"] != "error"
                    logger.debug(f"Backpack LONG result: {long_result}")
                    if not long_success:
                        logger.error(f"Failed to open LONG position on Backpack: {long_result.get('message', 'Unknown error')}")
                        return None
                except Exception as e:
                    logger.error(f"Failed to open LONG position on Backpack: {str(e)}", exc_info=True)
                    return None
                
                logger.info(f"Opening SHORT position on Lighter for {asset} with size ${position_size_usd}")
                try:
                    short_result = await lighter.open_short(asset, position_size_usd)
                    short_success = "error" not in short_result
                    logger.debug(f"Lighter SHORT result: {short_result}")
                    if not short_success:
                        logger.error(f"Failed to open SHORT position on Lighter: {short_result.get('error', 'Unknown error')}")
                        logger.warning("Lighter order placement failed - this is expected without valid credentials")
                        # Try to close the long position we just opened
                        try:
                            logger.debug(f"Closing LONG position on Backpack for {asset} after SHORT position failure")
                            backpack.close_asset_position(asset)
                        except Exception as close_e:
                            logger.error(f"Failed to close LONG position after SHORT position failure: {str(close_e)}", exc_info=True)
                        return None
                except Exception as e:
                    logger.error(f"Failed to open SHORT position on Lighter: {str(e)}", exc_info=True)
                    logger.warning("Lighter order placement failed - this is expected without valid credentials")
                    # Try to close the long position we just opened
                    try:
                        logger.debug(f"Closing LONG position on Backpack for {asset} after SHORT position failure")
                        backpack.close_asset_position(asset)
                    except Exception as close_e:
                        logger.error(f"Failed to close LONG position after SHORT position failure: {str(close_e)}", exc_info=True)
                    return None
            
            # Make sure both sides were opened successfully
            if not (long_success and short_success):
                logger.error(f"Failed to execute complete arbitrage for {asset}")
                # Cleanup any positions that were opened
                self._cleanup_positions(asset, long_exchange, short_exchange, long_success, short_success)
                return None
            
            # Log successful arbitrage execution
            logger.debug(f"Successfully executed arbitrage for {asset}:")
            logger.debug(f"   LONG on {long_exchange}: {long_result}")
            logger.debug(f"   SHORT on {short_exchange}: {short_result}")
            
            # Check positions to confirm they were actually opened
            try:
                logger.debug("Verifying positions were opened...")
                # Check Backpack positions
                bp_positions = backpack.get_positions()
                if isinstance(bp_positions, list):
                    logger.debug(f"Backpack positions: {len(bp_positions)}")
                    for pos in bp_positions:
                        if pos.get("symbol", "").startswith(asset):
                            logger.debug(f"   Backpack position: {pos}")
                else:
                    logger.warning(f"Unexpected Backpack positions format: {bp_positions}")
                
                # Check Hyperliquid positions
                hl_positions = hyperliquid.get_positions()
                if isinstance(hl_positions, list):
                    logger.debug(f"Hyperliquid positions: {len(hl_positions)}")
                    for pos in hl_positions:
                        if pos.get("coin") == asset:
                            logger.debug(f"   Hyperliquid position: {pos}")
                else:
                    logger.warning(f"Unexpected Hyperliquid positions format: {hl_positions}")
            except Exception as e:
                logger.error(f"Error verifying positions: {str(e)}", exc_info=True)
            
            return {
                "long_exchange": long_exchange,
                "short_exchange": short_exchange,
                "long_result": long_result,
                "short_result": short_result,
                "asset": asset,
                "size": position_size_usd
            }
            
        except Exception as e:
            logger.error(f"Error executing arbitrage: {str(e)}", exc_info=True)
            logger.debug("Attempting to close any open positions from failed execution...")
            try:
                if long_exchange == "Backpack":
                    logger.debug(f"Closing any LONG position on Backpack for {asset}")
                    backpack.close_asset_position(asset)
                elif long_exchange == "Hyperliquid":
                    logger.debug(f"Closing any LONG position on Hyperliquid for {asset}")
                    hyperliquid.close_position(asset)
                    
                if short_exchange == "Backpack":
                    logger.debug(f"Closing any SHORT position on Backpack for {asset}")
                    backpack.close_asset_position(asset)
                elif short_exchange == "Hyperliquid":
                    logger.debug(f"Closing any SHORT position on Hyperliquid for {asset}")
                    hyperliquid.close_position(asset)
            except Exception as close_error:
                logger.error(f"Error closing positions after failure: {str(close_error)}", exc_info=True)
            return None

    def _cleanup_positions(self, asset, long_exchange, short_exchange, long_success, short_success):
        """Cleanup positions if arbitrage was partially executed."""
        logger.info(f"Cleaning up partially executed arbitrage for {asset}")
        
        try:
            backpack = self.exchanges.get("Backpack")
            hyperliquid = self.exchanges.get("Hyperliquid")
            
            # Close long position if it was successfully opened
            if long_success:
                if long_exchange == "Backpack":
                    logger.info(f"Closing LONG position on Backpack for {asset}")
                    backpack.close_asset_position(asset)
                elif long_exchange == "Hyperliquid":
                    logger.info(f"Closing LONG position on Hyperliquid for {asset}")
                    hyperliquid.close_position(asset)
            
            # Close short position if it was successfully opened
            if short_success:
                if short_exchange == "Backpack":
                    logger.info(f"Closing SHORT position on Backpack for {asset}")
                    backpack.close_asset_position(asset)
                elif short_exchange == "Hyperliquid":
                    logger.info(f"Closing SHORT position on Hyperliquid for {asset}")
                    hyperliquid.close_position(asset)
        except Exception as e:
            logger.error(f"Error cleaning up positions: {str(e)}", exc_info=True)
    
    async def monitor_funding_rates(self, backpack, hyperliquid, lighter, opportunity):
        """Monitor funding rates and close positions when exit criteria are met."""
        logger.info(f"Starting funding rate monitor for {opportunity['asset']}...")
        asset = opportunity['asset']
        bp_symbol = f"{asset}_USDC_PERP"
        
        # Initial rates from the opportunity
        initial_hl_rate = opportunity['actions']['Hyperliquid']['rate']
        initial_bp_rate = opportunity['actions']['Backpack']['rate']
        initial_lt_rate = opportunity['actions'].get('Lighter', {}).get('rate', 0)
        
        # Calculate initial metrics
        initial_hl_sign = 1 if initial_hl_rate >= 0 else -1
        initial_bp_sign = 1 if initial_bp_rate >= 0 else -1
        initial_lt_sign = 1 if initial_lt_rate >= 0 else -1
        initial_max_magnitude = max(abs(initial_hl_rate), abs(initial_bp_rate), abs(initial_lt_rate))
        
        # Log initial conditions
        logger.info(f"Initial conditions for {asset}:")
        logger.info(f"  Hyperliquid rate: {initial_hl_rate:.8f} (sign: {initial_hl_sign})")
        logger.info(f"  Backpack rate: {initial_bp_rate:.8f} (sign: {initial_bp_sign})")
        logger.info(f"  Lighter rate: {initial_lt_rate:.8f} (sign: {initial_lt_sign})")
        logger.info(f"  Initial max magnitude: {initial_max_magnitude:.8f}")
        logger.info(f"Exit strategy:")
        logger.info(f" Min hold time: {self.min_hold_time_seconds/SECONDS_PER_HOUR} hour && Sign flip on either exchange && Magnitude reduction to {self.magnitude_reduction_threshold*100}% of initial")
        
        # Exchange for each side of the trade
        long_exchange = opportunity['long_exchange'] 
        short_exchange = opportunity['short_exchange']
        
        # Create async queue for rate updates
        funding_rate_queue = asyncio.Queue()
        
        # Thread-safe queue for Hyperliquid callbacks
        hl_thread_queue = queue.Queue()
        
        # Trade start time
        trade_start_time = time.time()
        
        # Statistics tracking
        stats = {
            "entry_time": trade_start_time,
            "exit_time": None,
            "duration_seconds": 0,
            "duration_hours": 0,
            "funding_payments": {
                "Backpack": 0,
                "Hyperliquid": 0,
                "Lighter": 0
            },
            "entry_prices": {
                "Backpack": None,
                "Hyperliquid": None,
                "Lighter": None
            },
            "exit_prices": {
                "Backpack": None,
                "Hyperliquid": None,
                "Lighter": None
            },
            "funding_pnl": 0,
            "price_pnl": 0,
            "fees": 0,
            "total_pnl": 0,
            "pnl_percent": 0,
            "annualized_roi": 0
        }
        
        # Get entry prices
        try:
            bp_positions = backpack.get_positions()
            hl_positions = hyperliquid.get_positions()
            
            # Process Backpack positions
            for pos in bp_positions:
                if pos.get("symbol").startswith(asset):
                    stats["entry_prices"]["Backpack"] = float(pos.get("avgEntryPrice", 0))
                    break
            
            # Process Hyperliquid positions
            for pos in hl_positions:
                if pos.get("coin") == asset:
                    stats["entry_prices"]["Hyperliquid"] = float(pos.get("entryPx", 0))
                    break
                    
        except Exception as e:
            logger.error(f"Error getting entry prices: {e}")
        
        # Define callbacks for websocket updates
        async def bp_rate_callback(message):
            try:
                data = message.get('data', {}) if message.get('stream') == f"markPrice.{bp_symbol}" else message
                funding_rate_str = data.get('f')
                
                if funding_rate_str is not None:
                    funding_rate = float(funding_rate_str)
                    await funding_rate_queue.put(("Backpack", funding_rate, time.time()))
            except Exception as e:
                logger.error(f"Error in Backpack callback: {e}")
        
        # Thread-safe callback that puts data in a regular Queue
        def hl_rate_callback(data):
            try:
                if isinstance(data, dict) and data.get("channel") == "userFundings":
                    fundings = data.get("data", {}).get('fundings', [])
                    
                    for funding_event in reversed(fundings):
                        if funding_event.get("coin") == asset:
                            funding_rate_str = funding_event.get("fundingRate")
                            
                            if funding_rate_str is not None:
                                funding_rate = float(funding_rate_str)
                                # Put the data in a thread-safe queue
                                hl_thread_queue.put(("Hyperliquid", funding_rate, time.time()))
                                break
            except Exception as e:
                logger.error(f"Error in Hyperliquid callback: {e}", exc_info=True)
        
        # Lighter callback for funding rate updates
        async def lt_rate_callback(market_stats):
            try:
                # Extract funding rate from market stats
                funding_rate_str = market_stats.get("current_funding_rate")
                
                if funding_rate_str is not None:
                    funding_rate = float(funding_rate_str)
                    await funding_rate_queue.put(("Lighter", funding_rate, time.time()))
            except Exception as e:
                logger.error(f"Error in Lighter callback: {e}", exc_info=True)
        
        # Task to move data from thread-safe queue to asyncio queue
        async def process_hl_queue():
            while True:
                try:
                    # Check if there's data in the HL queue (non-blocking)
                    try:
                        data = hl_thread_queue.get_nowait()
                        # Put it in the asyncio queue
                        await funding_rate_queue.put(data)
                    except queue.Empty:
                        # Queue is empty, just continue
                        pass
                    
                    # Short sleep to prevent CPU spinning
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error processing HL queue: {e}", exc_info=True)
                    await asyncio.sleep(1)
        
        # Tasks to be cleaned up at the end
        tasks = []
        
        try:
            # Use integrated WebSocket if enabled
            await backpack.ws_client.subscribe(f"markPrice.{bp_symbol}", bp_rate_callback)
            
            # Subscribe to Hyperliquid updates
            hyperliquid.subscribe_to_funding_updates(hl_rate_callback)
            
            # Subscribe to Lighter updates
            lighter.subscribe_to_funding_updates(lt_rate_callback)
            
            # Start the HL queue processor
            hl_processor_task = asyncio.create_task(process_hl_queue())
            tasks.append(hl_processor_task)
            
            # Create polling task
            polling_task = asyncio.create_task(self._poll_funding_rates(
                backpack, hyperliquid, lighter, asset, funding_rate_queue))
            tasks.append(polling_task)
            
            # Process funding rates
            latest_rates = {
                "Hyperliquid": {"rate": initial_hl_rate, "sign": initial_hl_sign},
                "Backpack": {"rate": initial_bp_rate, "sign": initial_bp_sign},
                "Lighter": {"rate": initial_lt_rate, "sign": initial_lt_sign}
            }
            
            # Track funding payments
            last_funding_time = {
                "Hyperliquid": trade_start_time,
                "Backpack": trade_start_time,
                "Lighter": trade_start_time
            }
            
            position_closed = False
            
            while not position_closed:
                try:
                    # Get next rate update with timeout
                    try:
                        exchange, rate, timestamp = await asyncio.wait_for(
                            funding_rate_queue.get(), timeout=60)  # Timeout after 60 seconds
                    except asyncio.TimeoutError:
                        # No new rates, continue polling
                        continue
                    
                    # Update stored rate
                    current_sign = 1 if rate >= 0 else -1
                    previous_sign = latest_rates[exchange]["sign"]
                    latest_rates[exchange]["rate"] = rate
                    latest_rates[exchange]["sign"] = current_sign
                    
                    # Calculate current metrics
                    hl_rate = latest_rates["Hyperliquid"]["rate"]
                    bp_rate = latest_rates["Backpack"]["rate"]
                    lt_rate = latest_rates["Lighter"]["rate"]
                    current_max_magnitude = max(abs(hl_rate), abs(bp_rate), abs(lt_rate))
                    elapsed_time = timestamp - trade_start_time
                    
                    # Estimate funding payment if it's a fresh funding update (longer intervals)
                    if timestamp - last_funding_time[exchange] > SECONDS_PER_HOUR:  # Assume hourly funding
                        # Only add funding if this is the exchange where we have a position
                        if (exchange == long_exchange or exchange == short_exchange):
                            # Funding rate is per 8 hours typically, so scale accordingly
                            hourly_rate = rate / 8
                            if exchange == long_exchange:
                                stats["funding_payments"][exchange] -= hourly_rate * self.position_size
                            else:
                                stats["funding_payments"][exchange] += hourly_rate * self.position_size
                            
                            last_funding_time[exchange] = timestamp
                    
                    logger.info(f"{exchange} funding rate update: {rate:.8f}")
                    
                    # Only consider exit if minimum hold time has passed
                    if elapsed_time >= self.min_hold_time_seconds:
                        exit_reason = None
                        
                        # 1. Sign flip check
                        if current_sign != previous_sign:
                            exit_reason = f"SIGN FLIP DETECTED for {exchange}: {previous_sign} -> {current_sign}"
                        
                        # 2. Magnitude reduction check
                        magnitude_ratio = current_max_magnitude / initial_max_magnitude
                        logger.info(f"Magnitude ratio: {magnitude_ratio:.2%} {self.magnitude_reduction_threshold}")
                        if not exit_reason and magnitude_ratio <= self.magnitude_reduction_threshold:
                            exit_reason = f"Funding rate magnitude reduced to {magnitude_ratio:.2%} of initial"
                        
                        # Execute exit if conditions met
                        if exit_reason: #exit_reason
                            logger.info(f"EXIT SIGNAL: {exit_reason}")
                            logger.info(f"Current conditions - HL: {hl_rate:.8f}, BP: {bp_rate:.8f}, LT: {lt_rate:.8f}, Max magnitude: {current_max_magnitude:.8f}")
                            
                            # Record exit time
                            stats["exit_time"] = timestamp
                            stats["duration_seconds"] = timestamp - trade_start_time
                            stats["duration_hours"] = stats["duration_seconds"] / SECONDS_PER_HOUR
                            
                            # Close positions on both exchanges
                            logger.info(f"Closing positions on both exchanges")
                            
                            # Close positions using the shared helper method
                            long_close_success = await self._close_exchange_position(
                                long_exchange,
                                backpack if long_exchange == "Backpack" else hyperliquid,
                                asset,
                                stats
                            )
                            
                            short_close_success = await self._close_exchange_position(
                                short_exchange,
                                backpack if short_exchange == "Backpack" else hyperliquid,
                                asset,
                                stats
                            )
                            
                            # Verify positions were actually closed
                            logger.info("Verifying positions were closed...")
                            
                            try:
                                # Wait briefly for position updates to propagate
                                await asyncio.sleep(2)
                                
                                # Check Backpack positions
                                bp_positions = backpack.get_positions()
                                bp_position_closed = True
                                if isinstance(bp_positions, list):
                                    for pos in bp_positions:
                                        if pos.get("symbol", "").startswith(asset) and float(pos.get("positionSize", "0")) != 0:
                                            bp_position_closed = False
                                            logger.warning(f"Backpack position still open: {pos}")
                                            # Try one more time to close
                                            if pos.get("symbol") == f"{asset}_USDC_PERP":
                                                logger.info("Making one final attempt to close Backpack position")
                                                backpack.close_position(pos.get("symbol"), float(pos.get("positionSize", "0")))
                                
                                # Check Hyperliquid positions
                                hl_positions = hyperliquid.get_positions()
                                hl_position_closed = True
                                if isinstance(hl_positions, list):
                                    for pos in hl_positions:
                                        if pos.get("coin") == asset and float(pos.get("szi", "0")) != 0:
                                            hl_position_closed = False
                                            logger.warning(f"Hyperliquid position still open: {pos}")
                                            # Try one more time to close
                                            logger.info("Making one final attempt to close Hyperliquid position")
                                            hyperliquid.close_position(asset)
                                
                                if bp_position_closed and hl_position_closed:
                                    logger.info("All positions successfully closed")
                                else:
                                    logger.warning("Some positions may still be open")
                            except Exception as e:
                                logger.error(f"Error verifying position closure: {e}", exc_info=True)
                            
                            # Calculate P&L components
                            
                            # 1. Funding payments
                            stats["funding_pnl"] = sum(stats["funding_payments"].values())
                            
                            # 2. Entry/Exit price difference (for position_size position)
                            try:
                                if long_exchange == "Backpack" and short_exchange == "Hyperliquid":
                                    # Long BP, Short HL
                                    long_price_pnl = 0
                                    short_price_pnl = 0
                                    
                                    if stats["entry_prices"]["Backpack"] and stats["exit_prices"]["Backpack"]:
                                        long_price_pnl = (stats["exit_prices"]["Backpack"] - stats["entry_prices"]["Backpack"]) * self.position_size / stats["entry_prices"]["Backpack"]
                                    
                                    if stats["entry_prices"]["Hyperliquid"] and stats["exit_prices"]["Hyperliquid"]:
                                        short_price_pnl = (stats["entry_prices"]["Hyperliquid"] - stats["exit_prices"]["Hyperliquid"]) * self.position_size / stats["entry_prices"]["Hyperliquid"]
                                    
                                    stats["price_pnl"] = long_price_pnl + short_price_pnl
                                else:
                                    # Long HL, Short BP
                                    long_price_pnl = 0
                                    short_price_pnl = 0
                                    
                                    if stats["entry_prices"]["Hyperliquid"] and stats["exit_prices"]["Hyperliquid"]:
                                        long_price_pnl = (stats["exit_prices"]["Hyperliquid"] - stats["entry_prices"]["Hyperliquid"]) * self.position_size / stats["entry_prices"]["Hyperliquid"]
                                    
                                    if stats["entry_prices"]["Backpack"] and stats["exit_prices"]["Backpack"]:
                                        short_price_pnl = (stats["entry_prices"]["Backpack"] - stats["exit_prices"]["Backpack"]) * self.position_size / stats["entry_prices"]["Backpack"]
                                    
                                    stats["price_pnl"] = long_price_pnl + short_price_pnl
                            except Exception as e:
                                logger.error(f"Error calculating price PnL: {e}")
                            
                            # 3. Fees (estimate based on typical exchange fees)
                            stats["fees"] = -1.0  # Estimate $1 in total trading fees
                            
                            # Total P&L
                            stats["total_pnl"] = stats["price_pnl"] + stats["funding_pnl"] + stats["fees"]
                            stats["pnl_percent"] = stats["total_pnl"] * 100 / self.position_size
                            
                            # Annualized ROI
                            if stats["duration_hours"] > 0:
                                stats["annualized_roi"] = (stats["pnl_percent"] * 365 * 24) / stats["duration_hours"]
                            
                            position_closed = True
                            logger.info("Positions closed. Stopping rate monitor.")
                            break
                
                except Exception as e:
                    logger.error(f"Error processing funding rate update: {e}")
                    await asyncio.sleep(1)
            
            # Return the statistics
            return stats
            
        except Exception as e:
            logger.error(f"Error in monitor_funding_rates: {e}")
            return stats
        finally:
            # Clean up all tasks
            for task in tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
    
    async def _poll_funding_rates(self, backpack, hyperliquid, lighter, asset, funding_rate_queue):
        """Periodically poll funding rates as a backup."""
        bp_symbol = f"{asset}_USDC_PERP"
        
        while True:
            try:
                # Get Backpack rates
                bp_data = backpack.get_mark_prices()
                for item in bp_data:
                    if item.get("symbol") == bp_symbol:
                        rate = float(item.get("fundingRate", 0))
                        await funding_rate_queue.put(("Backpack", rate, time.time()))
                
                # Get Hyperliquid rates
                hl_data = hyperliquid.get_funding_rates()
                hl_rates = hyperliquid.process_funding_rates(hl_data)
                if asset in hl_rates:
                    rate = hl_rates[asset]["rate"]
                    await funding_rate_queue.put(("Hyperliquid", rate, time.time()))
            
                # Get Lighter rates
                lt_data = await lighter.get_funding_rates()
                lt_rates = lighter.process_funding_rates(lt_data)
                if asset in lt_rates:
                    rate = lt_rates[asset]["rate"]
                    await funding_rate_queue.put(("Lighter", rate, time.time()))
            
            except Exception as e:
                logger.error(f"Error polling rates: {e}")
            
            # Sleep for a minute before polling again
            await asyncio.sleep(60) 

    async def close_position(self, asset: str) -> bool:
        """Manually close an active arbitrage position.
        
        This is useful for closing problematic positions or when
        needing to force-close a position before its monitor completes.
        
        Args:
            asset: The asset symbol of the position to close
            
        Returns:
            bool: True if position was closed successfully, False otherwise
        """
        logger.info(f"Manually closing position for {asset}...")
        
        # Check if we have an active position for this asset
        position = self.active_positions.get(asset)
        if not position:
            logger.warning(f"No active position found for {asset}")
            return False
        
        try:
            # Get exchange references
            backpack = self.exchanges.get("Backpack")
            hyperliquid = self.exchanges.get("Hyperliquid")
            lighter = self.exchanges.get("Lighter")
            
            # Get position details
            long_exchange = position.get('long_exchange')
            short_exchange = position.get('short_exchange')
            
            # Close positions using the shared helper method
            long_close_success = await self._close_exchange_position(
                long_exchange,
                backpack if long_exchange == "Backpack" else (hyperliquid if long_exchange == "Hyperliquid" else lighter),
                asset
            )
            
            short_close_success = await self._close_exchange_position(
                short_exchange,
                backpack if short_exchange == "Backpack" else (hyperliquid if short_exchange == "Hyperliquid" else lighter),
                asset
            )
            
            # Verify positions were actually closed
            try:
                # Wait briefly for position updates to propagate
                await asyncio.sleep(2)
                
                # Check both exchanges again to verify positions were closed
                bp_position_closed = True
                hl_position_closed = True
                lt_position_closed = True
                
                # Check Backpack positions
                bp_positions = backpack.get_positions()
                if isinstance(bp_positions, list):
                    for pos in bp_positions:
                        if pos.get("symbol", "").startswith(asset) and float(pos.get("positionSize", "0")) != 0:
                            bp_position_closed = False
                            logger.warning(f"Backpack position still open: {pos}")
                            # Try one more time to close
                            if pos.get("symbol") == f"{asset}_USDC_PERP":
                                logger.info("Making one final attempt to close Backpack position")
                                backpack.close_position(pos.get("symbol"), float(pos.get("positionSize", "0")))
                
                # Check Hyperliquid positions
                hl_positions = hyperliquid.get_positions()
                if isinstance(hl_positions, list):
                    for pos in hl_positions:
                        if pos.get("coin") == asset and float(pos.get("szi", "0")) != 0:
                            hl_position_closed = False
                            logger.warning(f"Hyperliquid position still open: {pos}")
                            # Try one more time to close
                            logger.info("Making one final attempt to close Hyperliquid position")
                            hyperliquid.close_position(asset)
                
                # Check Lighter positions
                lt_positions = lighter.get_positions()
                if isinstance(lt_positions, list):
                    for pos in lt_positions:
                        if pos.get("symbol") == asset and float(pos.get("size", "0")) != 0:
                            lt_position_closed = False
                            logger.warning(f"Lighter position still open: {pos}")
                            # Try one more time to close
                            logger.info("Making one final attempt to close Lighter position")
                            lighter.close_position(asset)
                
                if bp_position_closed and hl_position_closed and lt_position_closed:
                    logger.info("All positions successfully closed")
                else:
                    logger.warning("Some positions may still be open")
                
            except Exception as e:
                logger.error(f"Error verifying position closure: {e}", exc_info=True)
            
            # Cancel the monitor task if it's still running
            monitor_task = position.get('monitor_task')
            if monitor_task and not monitor_task.done():
                logger.debug(f"Cancelling monitor task for {asset}")
                monitor_task.cancel()
                try:
                    await monitor_task
                except asyncio.CancelledError:
                    pass
            
            # Remove from active positions
            del self.active_positions[asset]
            logger.info(f"Position for {asset} closed and removed from tracking")
            
            return long_close_success or short_close_success
        
        except Exception as e:
            logger.error(f"Error closing position for {asset}: {str(e)}", exc_info=True)
            return False

    async def _close_exchange_position(self, exchange_name, exchange_obj, asset_name, stats_dict=None):
        """
        Helper method to close positions on an exchange with retries.
        
        Args:
            exchange_name: Name of the exchange ("Backpack" or "Hyperliquid")
            exchange_obj: Exchange client object
            asset_name: Asset symbol to close
            stats_dict: Optional dictionary to store exit prices (for monitoring)
            
        Returns:
            bool: True if position was closed successfully, False otherwise
        """
        bp_symbol = f"{asset_name}_USDC_PERP"
        success = False
        
        for attempt in range(3):
            try:
                logger.info(f"Closing position on {exchange_name} for {asset_name} (attempt {attempt+1})")
                
                if exchange_name == "Backpack":
                    result = exchange_obj.close_asset_position(asset_name)
                elif exchange_name == "Hyperliquid":
                    result = exchange_obj.close_position(asset_name)
                elif exchange_name == "Lighter":
                    result = exchange_obj.close_position(asset_name)
                else:
                    logger.error(f"Unknown exchange: {exchange_name}")
                    return False
                
                # Check if successful
                if isinstance(result, dict) and "error" in result:
                    logger.warning(f"Failed to close {exchange_name} position (attempt {attempt+1}): {result}")
                    await asyncio.sleep(1)  # Wait before retry
                    continue
                
                success = True
                logger.info(f"{exchange_name} close result: {result}")
                
                # If stats dictionary is provided, capture exit prices
                if stats_dict is not None and "exit_prices" in stats_dict:
                    if exchange_name == "Backpack":
                        # Capture exit prices if available in result
                        if isinstance(result, dict) and "avgExitPrice" in result:
                            stats_dict["exit_prices"][exchange_name] = float(result["avgExitPrice"])
                        else:
                            # Fallback to market price
                            try:
                                market_data = exchange_obj.get_mark_prices()
                                for item in market_data:
                                    if item.get("symbol") == bp_symbol:
                                        stats_dict["exit_prices"][exchange_name] = float(item.get("markPrice", 0))
                                        break
                            except Exception as e:
                                logger.error(f"Error getting Backpack exit price: {e}")
                    
                    elif exchange_name == "Hyperliquid":
                        # Capture exit price if available
                        if isinstance(result, dict) and "px" in result:
                            stats_dict["exit_prices"][exchange_name] = float(result["px"])
                        else:
                            # Try to get current market price
                            try:
                                market_data = exchange_obj.get_market_data(asset_name)
                                if isinstance(market_data, dict) and "markPx" in market_data:
                                    stats_dict["exit_prices"][exchange_name] = float(market_data["markPx"])
                            except Exception as e:
                                logger.error(f"Error getting Hyperliquid exit price: {e}")
                    
                    elif exchange_name == "Lighter":
                        # Capture exit price if available
                        if isinstance(result, dict) and "price" in result:
                            stats_dict["exit_prices"][exchange_name] = float(result["price"])
                        else:
                            # Try to get current market price from funding rates
                            try:
                                funding_rates = await exchange_obj.get_funding_rates()
                                if asset_name in funding_rates:
                                    # Use mark price if available
                                    mark_price = funding_rates[asset_name].get("mark_price", 0)
                                    stats_dict["exit_prices"][exchange_name] = float(mark_price)
                            except Exception as e:
                                logger.error(f"Error getting Lighter exit price: {e}")
                
                break  # Success, exit retry loop
                
            except Exception as e:
                logger.error(f"Error closing {exchange_name} position (attempt {attempt+1}): {e}", exc_info=True)
                await asyncio.sleep(1)  # Wait before retry
        
        return success