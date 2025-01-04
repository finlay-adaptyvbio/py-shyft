import logging
import sys
from typing import Optional

def setup_logging(name: str, log_level: Optional[str] = None) -> logging.Logger:
    """Configure logging with a consistent format across the application."""
    logger = logging.getLogger(name)
    
    if not logger.handlers:  # Avoid adding handlers multiple times
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    # Set log level
    level = getattr(logging, (log_level or 'INFO').upper())
    logger.setLevel(level)
    
    return logger
