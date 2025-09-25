import os
import yaml
from dotenv import load_dotenv
from typing import Any

load_dotenv()

class Config:
    """Configuration class that loads all environment variables at once."""
    
    def __init__(self):

        # Load config from config.yaml
        with open('config.yaml', 'r') as file:
            config = yaml.safe_load(file)

        # Log file path
        self.LOG_FILE_PATH = config.get("LOG_FILE_PATH", "logs/arbitrage.log")

        # Backpack credentials
        self.BACKPACK_PRIVATE_KEY = os.environ.get("BACKPACK_API_SECRET")
        if not self.BACKPACK_PRIVATE_KEY:
            raise ValueError("Backpack private key not found in environment variables")
            
        self.BACKPACK_PUBLIC_KEY = os.environ.get("BACKPACK_API_KEY")
        if not self.BACKPACK_PUBLIC_KEY:
            raise ValueError("Backpack public key not found in environment variables")
        
        # Backpack settings
        self.BACKPACK_DEFAULT_WINDOW = int(config.get("BACKPACK_AUTH", {}).get("DEFAULT_WINDOW", 5000))
        self.BACKPACK_API_URL = config.get("BACKPACK_API_URL", "https://api.backpack.exchange/")
        self.BACKPACK_WS_URL = config.get("BACKPACK_WS_URL", "wss://ws.backpack.exchange/")
        self.BACKPACK_API_URL_HISTORY_FUNDING = f"{self.BACKPACK_API_URL}wapi/v1/history/funding"
        self.BACKPACK_API_URL_MARK_PRICES = f"{self.BACKPACK_API_URL}api/v1/markPrices"
        self.BACKPACK_API_URL_POSITION = f"{self.BACKPACK_API_URL}api/v1/position"
        self.BACKPACK_API_URL_ORDER = f"{self.BACKPACK_API_URL}api/v1/order"

        # Hyperliquid credentials
        self.HYPERLIQUID_API_URL = config.get("HYPERLIQUID_API_URL", "https://api.hyperliquid.xyz")
        self.HYPERLIQUID_API_URL_INFO = f"{self.HYPERLIQUID_API_URL}/info"

        self.HYPERLIQUID_ADDRESS = os.environ.get("HYPERLIQUID_ADDRESS")

        self.HYPERLIQUID_API_PRIVATE_KEY = os.environ.get("HYPERLIQUID_API_PRIVATE_KEY")
        if not self.HYPERLIQUID_API_PRIVATE_KEY:
            raise ValueError("Hyperliquid private key not found in environment variables")
        
        # Lighter credentials
        self.LIGHTER_PRIVATE_KEY = os.environ.get("LIGHTER_PRIVATE_KEY")
        # Note: Lighter private key is optional - order placement will be disabled if not provided
            
        self.LIGHTER_API_URL = config.get("LIGHTER_API_URL", "https://mainnet.zklighter.elliot.ai/")
        self.LIGHTER_API_KEY_INDEX = int(config.get("LIGHTER_API_KEY_INDEX", 2))
        self.LIGHTER_ACCOUNT_INDEX = int(config.get("LIGHTER_ACCOUNT_INDEX", 5725))

        # Arbitrage Engine Configuration
        self.POSITION_SIZE = float(config.get("POSITION_SIZE"))
        self.MIN_RATE_DIFFERENCE = float(config.get("MIN_RATE_DIFFERENCE"))
        self.MIN_HOLD_TIME_SECONDS = int(config.get("MIN_HOLD_TIME_SECONDS"))
        self.MAGNITUDE_REDUCTION_THRESHOLD = float(config.get("MAGNITUDE_REDUCTION_THRESHOLD"))
        self.CHECK_INTERVAL_MINUTES = int(config.get("CHECK_INTERVAL_MINUTES"))
        self.MAX_CONCURRENT_POSITIONS = int(config.get("MAX_CONCURRENT_POSITIONS"))

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key name.
        
        Args:
            key: The configuration key to retrieve
            default: Default value if key is not found
            
        Returns:
            The configuration value or default if not found
        """
        return getattr(self, key, default)

# Create a singleton instance
CONFIG = Config()
