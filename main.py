import asyncio
from utils.logger import setup_logger
from model.core.arbitrage_engine import FundingArbitrageEngine
from utils.config import CONFIG

# Initialize logger
logger = setup_logger(__name__)

async def main():
    logger.info("Starting funding rate arbitrage system...")
    
    try:
        # Initialize the arbitrage engine with configuration
        engine = FundingArbitrageEngine(
            position_size=CONFIG.POSITION_SIZE,       # Actual position size to open
            min_rate_difference=CONFIG.MIN_RATE_DIFFERENCE,  # Minimum funding rate difference to consider
            min_hold_time_seconds=CONFIG.MIN_HOLD_TIME_SECONDS,  # Minimum 1 hour hold time
            magnitude_reduction_threshold=CONFIG.MAGNITUDE_REDUCTION_THRESHOLD,  # Exit when magnitude drops to 10% of initial
            check_interval_minutes=CONFIG.CHECK_INTERVAL_MINUTES  # Check for new opportunities every 30 minutes
        )
        
        await engine.start()
        
        logger.info("Engine started successfully. Press Ctrl+C to exit.")
        
        # Keep the main event loop running until interrupted
        try:
            # Run forever until interrupted
            while engine.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Program interrupted by user. Shutting down...")
            await engine.stop()
            
    except KeyboardInterrupt:
        logger.info("Program interrupted by user. Shutting down...")
        if 'engine' in locals() and engine.running:
            await engine.stop()
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {str(e)}")
    finally:
        logger.info("Funding rate arbitrage system shutdown complete.")

if __name__ == "__main__":
    try:
        # Run the async main function
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted during startup. Exiting...")
