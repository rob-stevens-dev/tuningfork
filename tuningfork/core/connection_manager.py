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

from tuningfork.core.config_manager import ConfigManager

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages database connections across different platforms.
    
    This class is responsible for establishing, managing, and closing
    connections to various database systems.
    """
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize the ConnectionManager.
        
        Args:
            config_manager: A ConfigManager instance for accessing configuration.
        """
        self.config_manager = config_manager
        self.connections = {}  # Dictionary to store active connections
        self.connection_details = {}  # Dictionary to store connection details
    
    def connect(self, 
                connection_id: str, 
                db_type: str, 
                host: str, 
                port: int, 
                username: str, 
                password: str, 
                database: str,
                **kwargs) -> Tuple[bool, Optional[str]]:
        """
        Establish a database connection.
        
        Args:
            connection_id: Unique identifier for this connection.
            db_type: Type of database (postgres, mysql, mssql, sqlite).
            host: Database host.
            port: Database port.
            username: Database username.
            password: Database password.
            database: Database name.
            **kwargs: Additional connection parameters.
            
        Returns:
            A tuple of (success, error_message).
            
        Raises:
            ValueError: If an unsupported database type is specified.
        """
        # Check if connection already exists
        if connection_id in self.connections:
            logger.warning(f"Connection {connection_id} already exists")
            return False, "Connection already exists"
        
        # Store connection details for later reference
        self.connection_details[connection_id] = {
            "db_type": db_type,
            "host": host,
            "port": port,
            "username": username,
            "database": database,
            "additional_params": kwargs
        }
        
        try:
            # Connect based on database type
            if db_type.lower() == "postgres":
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    user=username,
                    password=password,
                    database=database,
                    **kwargs
                )
            elif db_type.lower() == "mysql":
                conn = mysql.connector.connect(
                    host=host,
                    port=port,
                    user=username,
                    password=password,
                    database=database,
                    **kwargs
                )
            elif db_type.lower() == "mssql":
                conn_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={host},{port};"
                    f"DATABASE={database};"
                    f"UID={username};"
                    f"PWD={password}"
                )
                conn = pyodbc.connect(conn_string)
            elif db_type.lower() == "sqlite":
                # For SQLite, the database parameter is the file path
                conn = sqlite3.connect(database)
            else:
                return False, f"Unsupported database type: {db_type}"
            
            # Store the connection
            self.connections[connection_id] = conn
            
            # Save connection configuration
            connection_config = {
                "db_type": db_type,
                "host": host,
                "port": port,
                "username": username,
                "database": database,
                **kwargs
            }
            # Don't store the password in the config
            self.config_manager.add_connection_config(connection_id, connection_config)
            
            logger.info(f"Successfully connected to {db_type} database at {host}:{port}/{database} as {username}")
            return True, None
        
        except Exception as e:
            logger.error(f"Failed to connect to {db_type} database: {str(e)}")
            return False, str(e)
    
    def disconnect(self, connection_id: str) -> Tuple[bool, Optional[str]]:
        """
        Close a database connection.
        
        Args:
            connection_id: ID of the connection to close.
            
        Returns:
            A tuple of (success, error_message).
        """
        if connection_id not in self.connections:
            logger.warning(f"Connection {connection_id} does not exist")
            return False, "Connection does not exist"
        
        try:
            self.connections[connection_id].close()
            del self.connections[connection_id]
            del self.connection_details[connection_id]
            logger.info(f"Disconnected from {connection_id}")
            return True, None
        except Exception as e:
            logger.error(f"Failed to disconnect from {connection_id}: {str(e)}")
            return False, str(e)
    
    def disconnect_all(self) -> None:
        """
        Close all database connections.
        """
        for connection_id in list(self.connections.keys()):
            self.disconnect(connection_id)
    
    def execute_query(self, 
                     connection_id: str, 
                     query: str, 
                     params: Optional[Any] = None) -> Tuple[bool, Any, Optional[str]]:
        """
        Execute a query on the specified connection.
        
        Args:
            connection_id: ID of the connection to use.
            query: The SQL query to execute.
            params: Optional parameters for the query.
            
        Returns:
            A tuple of (success, result, error_message).
        """
        if connection_id not in self.connections:
            logger.warning(f"Connection {connection_id} does not exist")
            return False, None, "Connection does not exist"
        
        conn = self.connections[connection_id]
        db_type = self.connection_details[connection_id]["db_type"]
        
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Fetch results if this is a SELECT query
            if query.strip().lower().startswith("select"):
                result = cursor.fetchall()
            else:
                result = cursor.rowcount
                conn.commit()
            
            cursor.close()
            return True, result, None
        
        except Exception as e:
            logger.error(f"Failed to execute query on {connection_id}: {str(e)}")
            return False, None, str(e)
    
    def get_connection_info(self, connection_id: str) -> Dict[str, Any]:
        """
        Get information about a specific connection.
        
        Args:
            connection_id: ID of the connection.
            
        Returns:
            A dictionary with connection information.
            
        Raises:
            KeyError: If the connection ID is not found.
        """
        if connection_id not in self.connection_details:
            raise KeyError(f"Connection {connection_id} does not exist")
        
        return self.connection_details[connection_id]
    
    def list_connections(self) -> List[str]:
        """
        List all active connections.
        
        Returns:
            A list of connection IDs.
        """
        return list(self.connections.keys())
    
    def is_connected(self, connection_id: str) -> bool:
        """
        Check if a connection is active.
        
        Args:
            connection_id: ID of the connection.
            
        Returns:
            True if the connection is active, False otherwise.
        """
        if connection_id not in self.connections:
            return False
        
        # Test the connection by executing a simple query
        try:
            conn = self.connections[connection_id]
            db_type = self.connection_details[connection_id]["db_type"]
            
            cursor = conn.cursor()
            if db_type.lower() == "postgres":
                cursor.execute("SELECT 1")
            elif db_type.lower() == "mysql":
                cursor.execute("SELECT 1")
            elif db_type.lower() == "mssql":
                cursor.execute("SELECT 1")
            elif db_type.lower() == "sqlite":
                cursor.execute("SELECT 1")
            
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            # Connection is broken
            del self.connections[connection_id]
            return False