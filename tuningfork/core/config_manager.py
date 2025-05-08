"""
Configuration Manager module for TuningFork database performance optimization tool.

This module provides functionality for loading, accessing, and saving configuration.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, Union

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Manages configuration settings for the TuningFork tool.
    
    This class provides functionality for loading, accessing, and saving configuration
    settings from a JSON file or dictionary.
    """
    
    def __init__(self, config_file: Optional[str] = None, config_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize the ConfigManager.
        
        Args:
            config_file: Path to the configuration file (optional)
            config_dict: Configuration dictionary (optional)
            
        Raises:
            ValueError: If neither config_file nor config_dict is provided
        """
        self.config = {}
        
        if config_file:
            self.load_config(config_file)
        elif config_dict:
            self.config = config_dict
        else:
            # Default configuration if neither file nor dict provided
            self.config = {
                "storage_directory": "data"
            }
    
    def load_config(self, config_file: str) -> None:
        """
        Load configuration from a file.
        
        Args:
            config_file: Path to the configuration file
            
        Raises:
            FileNotFoundError: If the configuration file does not exist
            ValueError: If the configuration file is not valid JSON
        """
        try:
            logger.info(f"Loading configuration from {config_file}")
            
            if not os.path.exists(config_file):
                raise FileNotFoundError(f"Configuration file {config_file} not found")
            
            with open(config_file, 'r') as f:
                self.config = json.load(f)
                
            logger.info("Configuration loaded successfully")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON configuration: {str(e)}")
            raise ValueError(f"Invalid JSON in configuration file: {str(e)}")
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise
    
    def save_config(self, config_file: str) -> None:
        """
        Save configuration to a file.
        
        Args:
            config_file: Path to the configuration file
            
        Raises:
            IOError: If the configuration file cannot be written
        """
        try:
            logger.info(f"Saving configuration to {config_file}")
            
            # Create directory if it doesn't exist
            directory = os.path.dirname(os.path.abspath(config_file))
            os.makedirs(directory, exist_ok=True)
            
            with open(config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
                
            logger.info("Configuration saved successfully")
        except Exception as e:
            logger.error(f"Error saving configuration: {str(e)}")
            raise
    
    def get_value(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            key: The configuration key
            default: Default value if the key is not found
            
        Returns:
            The configuration value or the default value
        """
        # Handle nested keys with dot notation (e.g., "connections.mysql.host")
        if '.' in key:
            keys = key.split('.')
            value = self.config
            
            for k in keys:
                if isinstance(value, dict) and k in value:
                    value = value[k]
                else:
                    return default
                    
            return value
        
        # Simple key lookup
        return self.config.get(key, default)
    
    def set_value(self, key: str, value: Any) -> None:
        """
        Set a configuration value.
        
        Args:
            key: The configuration key
            value: The value to set
        """
        # Handle nested keys with dot notation (e.g., "connections.mysql.host")
        if '.' in key:
            keys = key.split('.')
            config = self.config
            
            # Navigate to the nested dictionary
            for k in keys[:-1]:
                if k not in config:
                    config[k] = {}
                config = config[k]
                
            # Set the value in the nested dictionary
            config[keys[-1]] = value
        else:
            # Simple key setting
            self.config[key] = value
    
    def get_connection_config(self, connection_id: str) -> Dict[str, Any]:
        """
        Get configuration for a specific database connection.
        
        Args:
            connection_id: The connection ID
            
        Returns:
            The connection configuration or an empty dictionary
            
        Raises:
            ValueError: If the connection ID is not found
        """
        connections = self.get_value("connections", {})
        
        if connection_id not in connections:
            logger.warning(f"Connection {connection_id} not found in configuration")
            raise ValueError(f"Connection {connection_id} not found in configuration")
        
        return connections[connection_id]
    
    def add_connection_config(self, connection_id: str, config: Dict[str, Any]) -> None:
        """
        Add or update a connection configuration.
        
        Args:
            connection_id: The connection ID
            config: The connection configuration
        """
        if "connections" not in self.config:
            self.config["connections"] = {}
            
        self.config["connections"][connection_id] = config
        logger.info(f"Added configuration for connection {connection_id}")
    
    def remove_connection_config(self, connection_id: str) -> None:
        """
        Remove a connection configuration.
        
        Args:
            connection_id: The connection ID
            
        Raises:
            ValueError: If the connection ID is not found
        """
        if "connections" not in self.config or connection_id not in self.config["connections"]:
            logger.warning(f"Connection {connection_id} not found in configuration")
            raise ValueError(f"Connection {connection_id} not found in configuration")
            
        del self.config["connections"][connection_id]
        logger.info(f"Removed configuration for connection {connection_id}")
    
    def get_all_connection_ids(self) -> list:
        """
        Get all configured connection IDs.
        
        Returns:
            List of connection IDs
        """
        return list(self.get_value("connections", {}).keys())