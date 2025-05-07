"""
Database Performance Optimization Tool - Stage 1.1 Implementation
Basic Project Setup and Connection Manager

This module implements the core infrastructure for the database
performance optimization tool, including the project structure,
connection manager, configuration management, and basic CLI interface.
"""

import os
import json
import logging
import argparse
from typing import Dict, Any, Optional, List, Tuple
import psycopg2
import mysql.connector
import pyodbc
import sqlite3

logger = logging.getLogger(__name__)

class ConfigManager:
    """
    Handles configuration settings and credentials.
    
    This class is responsible for loading, accessing, and saving
    configuration settings for the application.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize the ConfigManager.
        
        Args:
            config_file: Optional path to a configuration file.
        """
        self.config = {}
        self.config_file = config_file
        
        # If config file is provided, load it
        if config_file and os.path.exists(config_file):
            self.load_config(config_file)
        else:
            # Set default configuration
            self.config = {
                "connections": {},
                "default_timeout": 30,
                "backup_directory": "./backups",
                "report_directory": "./reports",
                "log_level": "INFO"
            }
    
    def load_config(self, config_file: str) -> None:
        """
        Load configuration from a JSON file.
        
        Args:
            config_file: Path to the configuration file.
            
        Raises:
            FileNotFoundError: If the configuration file does not exist.
            json.JSONDecodeError: If the configuration file is not valid JSON.
        """
        try:
            with open(config_file, 'r') as f:
                self.config = json.load(f)
            logger.info(f"Loaded configuration from {config_file}")
        except FileNotFoundError:
            logger.error(f"Configuration file {config_file} not found")
            raise
        except json.JSONDecodeError:
            logger.error(f"Configuration file {config_file} is not valid JSON")
            raise
    
    def save_config(self, config_file: Optional[str] = None) -> None:
        """
        Save the current configuration to a JSON file.
        
        Args:
            config_file: Optional path to save the configuration file.
                         If not provided, uses the original path.
        """
        file_path = config_file or self.config_file
        if not file_path:
            logger.warning("No configuration file path provided, skipping save")
            return
        
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
            
            with open(file_path, 'w') as f:
                json.dump(self.config, f, indent=4)
            logger.info(f"Saved configuration to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save configuration: {str(e)}")
            raise
    
    def get_value(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key.
        
        Args:
            key: The configuration key.
            default: Default value to return if key is not found.
            
        Returns:
            The configuration value or the default value if not found.
        """
        # Support nested keys with dot notation (e.g., "connections.postgres")
        if '.' in key:
            parts = key.split('.')
            current = self.config
            for part in parts:
                if part in current:
                    current = current[part]
                else:
                    return default
            return current
        
        return self.config.get(key, default)
    
    def set_value(self, key: str, value: Any) -> None:
        """
        Set a configuration value.
        
        Args:
            key: The configuration key.
            value: The value to set.
        """
        # Support nested keys with dot notation
        if '.' in key:
            parts = key.split('.')
            current = self.config
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        else:
            self.config[key] = value
    
    def get_connection_config(self, connection_id: str) -> Dict[str, Any]:
        """
        Get the configuration for a specific database connection.
        
        Args:
            connection_id: The ID of the connection.
            
        Returns:
            A dictionary with the connection configuration.
            
        Raises:
            KeyError: If the connection ID is not found.
        """
        connections = self.get_value("connections", {})
        if connection_id not in connections:
            raise KeyError(f"Connection configuration for {connection_id} not found")
        
        return connections[connection_id]
    
    def add_connection_config(self, connection_id: str, config: Dict[str, Any]) -> None:
        """
        Add or update a connection configuration.
        
        Args:
            connection_id: The ID of the connection.
            config: The connection configuration.
        """
        connections = self.get_value("connections", {})
        connections[connection_id] = config
        self.set_value("connections", connections)
    
    def remove_connection_config(self, connection_id: str) -> None:
        """
        Remove a connection configuration.
        
        Args:
            connection_id: The ID of the connection to remove.
            
        Raises:
            KeyError: If the connection ID is not found.
        """
        connections = self.get_value("connections", {})
        if connection_id not in connections:
            raise KeyError(f"Connection configuration for {connection_id} not found")
        
        del connections[connection_id]
        self.set_value("connections", connections)
    
    def list_connections(self) -> List[str]:
        """
        List all configured connections.
        
        Returns:
            A list of connection IDs.
        """
        return list(self.get_value("connections", {}).keys())
