import asyncio
from model.exchanges.backpack_ws import BackpackWebSocketClient
from utils.config import CONFIG
from utils.logger import setup_logger

# Initialize logger
logger = setup_logger(__name__)

async def main():

    # Load credentials securely
    logger.debug("Loading credentials...")
    api_key = CONFIG.get("BACKPACK_PUBLIC_KEY")
    api_secret = CONFIG.get("BACKPACK_PRIVATE_KEY")

    if not api_key or not api_secret:
        logger.error("API Key or Secret not configured in CONFIG. Exiting.")
        print("API Key or Secret not configured. Exiting.")
        return
    logger.debug("Credentials loaded.")

    client = BackpackWebSocketClient()

    try:
        logger.debug("Connecting client...")
        await client.connect()
        logger.debug("Client connect initiated.")

        # Subscribe to SOL mark price (optional, uncomment to test)
        # logger.debug("Subscribing to mark price...")
        await client.subscribe("markPrice.ETH_USDC_PERP", client.handle_mark_price)

        # Subscribe to position updates
        logger.debug("Subscribing to position updates...")
        await client.subscribe("account.positionUpdate.ETH_USDC_PERP", client.handle_position_query)
        logger.debug("Subscription requests sent.")

        # Keep the connection alive / main task running
        logger.info("Setup complete. Running main loop (Press Ctrl+C to exit)...")
        while True:
            await asyncio.sleep(3600) # Sleep for a long time, woken by Ctrl+C

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Disconnecting...")
    except Exception as e:
        logger.error(f"An error occurred in main: {e}", exc_info=True)
    finally:
        logger.info("Initiating client disconnection...")
        await client.disconnect()
        logger.info("Client disconnected. Exiting main.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Main interrupted by second Ctrl+C.") 
