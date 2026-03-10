"""
Logging configuration for PVIGR data processing pipeline.

Provides centralized logging setup with console and file output,
configurable log levels, and consistent formatting across all modules.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


class PVIGRLogger:
    """
    Centralized logging configuration for PVIGR data processing.
    
    Provides consistent logging across all pipeline modules with both
    console and file output capabilities.
    """
    
    _loggers = {}
    _log_dir = None
    _log_level = logging.INFO
    _file_handler = None
    _console_handler = None
    
    @classmethod
    def setup_logging(cls, 
                      log_dir: Optional[str] = None,
                      log_level: str = 'INFO',
                      log_to_file: bool = True,
                      log_to_console: bool = True) -> None:
        """
        Configure logging for the entire application.
        
        Args:
            log_dir: Directory for log files (default: './logs')
            log_level: Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR')
            log_to_file: Enable file logging
            log_to_console: Enable console logging
        """
        cls._log_level = getattr(logging, log_level.upper())
        
        if log_to_file:
            if log_dir is None:
                log_dir = Path.cwd() / 'logs'
            else:
                log_dir = Path(log_dir)
            
            log_dir.mkdir(exist_ok=True)
            cls._log_dir = log_dir
            
            # Create file handler with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = log_dir / f'pvigr_processing_{timestamp}.log'
            
            cls._file_handler = logging.FileHandler(log_file, encoding='utf-8')
            cls._file_handler.setLevel(cls._log_level)
            
            # Detailed format for file logs
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            cls._file_handler.setFormatter(file_formatter)
        
        if log_to_console:
            cls._console_handler = logging.StreamHandler(sys.stdout)
            cls._console_handler.setLevel(cls._log_level)
            
            # Cleaner format for console
            console_formatter = logging.Formatter(
                '%(levelname)s - %(name)s - %(message)s'
            )
            cls._console_handler.setFormatter(console_formatter)
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Get or create a logger for a specific module.
        
        Args:
            name: Logger name (typically __name__ from calling module)
            
        Returns:
            Configured logger instance
        """
        if name in cls._loggers:
            return cls._loggers[name]
        
        # Create new logger
        logger = logging.getLogger(name)
        logger.setLevel(cls._log_level)
        logger.propagate = False
        
        # Remove any existing handlers
        logger.handlers.clear()
        
        # Add configured handlers
        if cls._file_handler:
            logger.addHandler(cls._file_handler)
        if cls._console_handler:
            logger.addHandler(cls._console_handler)
        
        cls._loggers[name] = logger
        return logger
    
    @classmethod
    def set_level(cls, level: str) -> None:
        """
        Change logging level for all loggers.
        
        Args:
            level: New logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR')
        """
        new_level = getattr(logging, level.upper())
        cls._log_level = new_level
        
        for logger in cls._loggers.values():
            logger.setLevel(new_level)
        
        if cls._file_handler:
            cls._file_handler.setLevel(new_level)
        if cls._console_handler:
            cls._console_handler.setLevel(new_level)


# Initialize default logging configuration
PVIGRLogger.setup_logging(
    log_level='INFO',
    log_to_file=False,  # Disabled by default, enable when needed
    log_to_console=True
)


def get_logger(name: str) -> logging.Logger:
    """
    Convenience function to get a logger.
    
    Args:
        name: Logger name (use __name__ from calling module)
        
    Returns:
        Configured logger instance
        
    Example:
        logger = get_logger(__name__)
        logger.info("Processing started")
    """
    return PVIGRLogger.get_logger(name)
