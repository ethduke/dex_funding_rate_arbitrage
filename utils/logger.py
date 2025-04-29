import logging
import sys
from utils.config import CONFIG

def setup_logger(name="funding_arb", level=logging.INFO):
    """
    Setup and return a configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Prevent duplicate configuration
    if logger.handlers:
        return logger
        
    logger.setLevel(level)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.FileHandler(CONFIG.LOG_FILE_PATH, encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger