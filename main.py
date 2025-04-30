import asyncio
import logging
from utils.logger import setup_logger
from model.core.arbitrage_engine import FundingArbitrageEngine
from utils.config import CONFIG

# Initialize logger
logger = setup_logger(__name__)

async def main():
    
    logger.info("Starting funding rate arbitrage system...")
    
    try:
        # Create the arbitrage engine with WebSockets enabled
        engine = FundingArbitrageEngine(
            min_rate_difference=float(CONFIG.get('MIN_RATE_DIFFERENCE')),
            position_size=float(CONFIG.get('POSITION_SIZE')),
            min_hold_time_seconds=int(CONFIG.get('MIN_HOLD_TIME_SECONDS')),
            magnitude_reduction_threshold=float(CONFIG.get('MAGNITUDE_REDUCTION_THRESHOLD')),
            check_interval_minutes=int(CONFIG.get('CHECK_INTERVAL_MINUTES')),
            use_ws=True  # Enable WebSockets for real-time data
        )
        
        await engine.start()
        
        logger.info("Engine started successfully. Press Ctrl+C to exit.")
        
        # Run until interrupted
        print("Running arbitrage engine. Press Ctrl+C to stop.")
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Program interrupted by user. Shutting down...")
        if 'engine' in locals() and engine.running:
            await engine.stop()
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {str(e)}")
    finally:
        logger.info("Funding rate arbitrage system shutdown complete.")
        print("Arbitrage engine stopped.")

if __name__ == "__main__":
    try:
        # Run the async main function
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted during startup. Exiting...")
