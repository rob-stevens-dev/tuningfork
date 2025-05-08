"""
Base Analyzer module for TuningFork database performance optimization tool.

This module provides the base class for all analyzer components.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional

from tuningfork.connection.connection_manager import ConnectionManager
from tuningfork.core.config_manager import ConfigManager

logger = logging.getLogger(__name__)


class BaseAnalyzer(ABC):
    """
    Base class for all analyzer components.
    
    This abstract class defines the common interface and functionality for
    all analyzer components in the TuningFork tool.
    """
    
    def __init__(self, connection_manager: ConnectionManager, config_manager: ConfigManager):
        """
        Initialize the BaseAnalyzer.
        
        Args:
            connection_manager: The ConnectionManager instance
            config_manager: The ConfigManager instance
        """
        self.connection_manager = connection_manager
        self.config_manager = config_manager
        
    def validate_connection(self, connection_id: str) -> bool:
        """
        Validates that a connection exists and is active.
        
        Args:
            connection_id: The ID of the database connection
            
        Returns:
            True if the connection is valid, False otherwise
        """
        connection = self.connection_manager.get_connection(connection_id)
        return connection is not None and connection.is_connected()