import logging
import sys
from utils.config import CONFIG

class ColorFormatter(logging.Formatter):
    """
    Custom formatter to add color to console logs
    """
    COLORS = {
        'DEBUG': '\033[94m',  # Blue
        'INFO': '\033[92m',   # Green
        'WARNING': '\033[93m', # Yellow
        'ERROR': '\033[91m',  # Red
        'CRITICAL': '\033[91m\033[1m',  # Bold Red
        'RESET': '\033[0m'    # Reset
    }
    
    def format(self, record):
        log_message = super().format(record)
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        return f"{color}{log_message}{self.COLORS['RESET']}"

def setup_logger(name="funding_arb", level=logging.INFO):
    """
    Setup and return a configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Prevent duplicate configuration
    if logger.handlers:
        return logger
        
    logger.setLevel(level)
    
    # Prevent propagation to parent loggers to avoid duplicate logs
    logger.propagate = False
    
    # Reduce websockets library logging to WARNING level
    websockets_logger = logging.getLogger("websockets")
    websockets_logger.setLevel(logging.WARNING)
    
    # Reduce urllib3 logging to WARNING level
    urllib3_logger = logging.getLogger("urllib3")
    urllib3_logger.setLevel(logging.WARNING)
    
    # Reduce asyncio logging to WARNING level
    asyncio_logger = logging.getLogger("asyncio")
    asyncio_logger.setLevel(logging.WARNING)
    
    # Reduce websocket library logging to WARNING level
    websocket_logger = logging.getLogger("websocket")
    websocket_logger.setLevel(logging.WARNING)
    
    # Reduce root logger debug output
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    
    # Reduce Hyperliquid SDK logging
    hyperliquid_logger = logging.getLogger("hyperliquid")
    hyperliquid_logger.setLevel(logging.WARNING)
    
    # Reduce any other potential debug sources
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connection").setLevel(logging.WARNING)
    
    # Default formatter for file logs
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Color formatter for console
    console_formatter = ColorFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.FileHandler(CONFIG.LOG_FILE_PATH, encoding='utf-8')
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger