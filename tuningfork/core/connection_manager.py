"""
Connection Manager module for TuningFork database performance optimization tool.

This module provides functionality for managing database connections.
"""

import logging
import importlib
from typing import Dict, Any, Optional, List, Union

from tuningfork.core.config_manager import ConfigManager
from tuningfork.util.exceptions import ConnectionError

logger = logging.getLogger(__name__)


class Connection:
    """
    Represents a database connection.
    
    This class is a wrapper around different database connection objects.
    """
    
    def __init__(self, connection_id: str, db_type: str, connection_obj, config: Dict[str, Any]):
        """
        Initialize a Connection object.
        
        Args:
            connection_id: The connection ID
            db_type: The database type (postgresql, mysql, mssql, sqlite)
            connection_obj: The actual database connection object
            config: The connection configuration
        """
        self.connection_id = connection_id
        self.db_type = db_type.lower()
        self.connection_obj = connection_obj
        self.config = config
        self.is_ssh_tunnel = False
        self.ssh_tunnel = None
    
    def cursor(self):
        """
        Get a cursor for the connection.
        
        Returns:
            A database cursor
        """
        return self.connection_obj.cursor()
    
    def commit(self):
        """Commit the current transaction."""
        return self.connection_obj.commit()
    
    def rollback(self):
        """Rollback the current transaction."""
        return self.connection_obj.rollback()
    
    def close(self):
        """Close the connection."""
        if self.is_connected():
            self.connection_obj.close()
            
            # Close SSH tunnel if present
            if self.is_ssh_tunnel and self.ssh_tunnel:
                try:
                    self.ssh_tunnel.close()
                    logger.info(f"Closed SSH tunnel for connection {self.connection_id}")
                except Exception as e:
                    logger.error(f"Error closing SSH tunnel for connection {self.connection_id}: {str(e)}")
    
    def is_connected(self) -> bool:
        """
        Check if the connection is still active.
        
        Returns:
            True if the connection is active, False otherwise
        """
        if not self.connection_obj:
            return False
            
        try:
            # Different ways to check connection based on database type
            if self.db_type == "postgresql":
                return not self.connection_obj.closed
            elif self.db_type == "mysql":
                self.connection_obj.ping(reconnect=False)
                return True
            elif self.db_type == "mssql":
                cursor = self.connection_obj.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return True
            elif self.db_type == "sqlite":
                cursor = self.connection_obj.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return True
            else:
                # Default for unknown types - just return True and hope for the best
                return True
        except Exception:
            return False
    
    def execute_query(self, query: str, params: Optional[Union[tuple, dict]] = None) -> List[tuple]:
        """
        Execute a query and return the results.
        
        Args:
            query: The SQL query to execute
            params: Parameters for the query
            
        Returns:
            List of result rows
            
        Raises:
            ConnectionError: If there is an error executing the query
        """
        try:
            cursor = self.cursor()
            cursor.execute(query, params or ())
            
            # Get results if it's a SELECT query
            if query.strip().upper().startswith(("SELECT", "SHOW", "DESCRIBE", "PRAGMA")):
                results = cursor.fetchall()
            else:
                results = []
                
            cursor.close()
            return results
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise ConnectionError(f"Error executing query: {str(e)}")


class ConnectionManager:
    """
    Manages database connections.
    
    This class is responsible for creating, storing, and closing database connections.
    """
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize ConnectionManager.
        
        Args:
            config_manager: The ConfigManager instance
        """
        self.config_manager = config_manager
        self.connections: Dict[str, Connection] = {}
    
    def connect(self, connection_id: str, **kwargs) -> Connection:
        """
        Create a new database connection.
        
        Args:
            connection_id: The connection ID
            **kwargs: Additional connection parameters that override configuration
            
        Returns:
            The Connection object
            
        Raises:
            ConnectionError: If there is an error creating the connection
        """
        try:
            # Get connection configuration
            config = self.config_manager.get_connection_config(connection_id).copy()
            
            # Override with any provided kwargs
            config.update(kwargs)
            
            # Get database type
            db_type = config.get("type", "").lower()
            if not db_type:
                raise ConnectionError(f"Database type not specified for connection {connection_id}")
            
            # Create connection based on database type
            connection_obj = None
            
            if db_type == "postgresql":
                connection_obj = self._connect_postgresql(config)
            elif db_type == "mysql":
                connection_obj = self._connect_mysql(config)
            elif db_type == "mssql":
                connection_obj = self._connect_mssql(config)
            elif db_type == "sqlite":
                connection_obj = self._connect_sqlite(config)
            else:
                raise ConnectionError(f"Unsupported database type: {db_type}")
            
            # Create Connection object
            connection = Connection(connection_id, db_type, connection_obj, config)
            
            # Store connection
            self.connections[connection_id] = connection
            
            logger.info(f"Created connection {connection_id} ({db_type})")
            return connection
        
        except Exception as e:
            logger.error(f"Error creating connection {connection_id}: {str(e)}")
            raise ConnectionError(f"Error creating connection {connection_id}: {str(e)}")
    
    def get_connection(self, connection_id: str) -> Optional[Connection]:
        """
        Get an existing connection.
        
        Args:
            connection_id: The connection ID
            
        Returns:
            The Connection object or None if not found
        """
        connection = self.connections.get(connection_id)
        
        # Check if connection is still active
        if connection and not connection.is_connected():
            logger.warning(f"Connection {connection_id} is no longer active, removing it")
            del self.connections[connection_id]
            return None
            
        return connection
    
    def close_connection(self, connection_id: str) -> bool:
        """
        Close a connection.
        
        Args:
            connection_id: The connection ID
            
        Returns:
            True if the connection was closed, False if it was not found
        """
        connection = self.connections.get(connection_id)
        if connection:
            try:
                connection.close()
                del self.connections[connection_id]
                logger.info(f"Closed connection {connection_id}")
                return True
            except Exception as e:
                logger.error(f"Error closing connection {connection_id}: {str(e)}")
                return False
        else:
            logger.warning(f"Connection {connection_id} not found")
            return False
    
    def close_all_connections(self) -> None:
        """Close all active connections."""
        for connection_id in list(self.connections.keys()):
            self.close_connection(connection_id)
    
    def list_connections(self) -> List[Dict[str, Any]]:
        """
        List all active connections.
        
        Returns:
            List of connection information dictionaries
        """
        return [
            {
                "connection_id": conn.connection_id,
                "db_type": conn.db_type,
                "config": {k: v for k, v in conn.config.items() if k != "password"}
            }
            for conn in self.connections.values()
        ]
    
    def _connect_postgresql(self, config: Dict[str, Any]):
        """
        Create a PostgreSQL connection.
        
        Args:
            config: Connection configuration
            
        Returns:
            psycopg2 connection object
            
        Raises:
            ConnectionError: If there is an error creating the connection
        """
        try:
            import psycopg2
            
            # Create connection
            conn = psycopg2.connect(
                host=config.get("host", "localhost"),
                port=config.get("port", 5432),
                database=config.get("database", "postgres"),
                user=config.get("user", "postgres"),
                password=config.get("password", ""),
                connect_timeout=config.get("timeout", 30)
            )
            
            return conn
        except ImportError:
            raise ConnectionError("psycopg2 module not found. Install it with: pip install psycopg2-binary")
        except Exception as e:
            raise ConnectionError(f"Error connecting to PostgreSQL: {str(e)}")
    
    def _connect_mysql(self, config: Dict[str, Any]):
        """
        Create a MySQL connection.
        
        Args:
            config: Connection configuration
            
        Returns:
            mysql.connector connection object
            
        Raises:
            ConnectionError: If there is an error creating the connection
        """
        try:
            import mysql.connector
            
            # Create connection
            conn = mysql.connector.connect(
                host=config.get("host", "localhost"),
                port=config.get("port", 3306),
                database=config.get("database", "mysql"),
                user=config.get("user", "root"),
                password=config.get("password", ""),
                connection_timeout=config.get("timeout", 30)
            )
            
            return conn
        except ImportError:
            raise ConnectionError("mysql-connector-python module not found. Install it with: pip install mysql-connector-python")
        except Exception as e:
            raise ConnectionError(f"Error connecting to MySQL: {str(e)}")
    
    def _connect_mssql(self, config: Dict[str, Any]):
        """
        Create a Microsoft SQL Server connection.
        
        Args:
            config: Connection configuration
            
        Returns:
            pyodbc connection object
            
        Raises:
            ConnectionError: If there is an error creating the connection
        """
        try:
            import pyodbc
            
            # Create connection string
            conn_str = (
                f"DRIVER={config.get('driver', '{ODBC Driver 17 for SQL Server}')};"
                f"SERVER={config.get('server', 'localhost')},"
                f"{config.get('port', 1433)};"
                f"DATABASE={config.get('database', 'master')};"
                f"UID={config.get('user', 'sa')};"
                f"PWD={config.get('password', '')};"
                f"Timeout={config.get('timeout', 30)};"
            )
            
            # Create connection
            conn = pyodbc.connect(conn_str)
            
            return conn
        except ImportError:
            raise ConnectionError("pyodbc module not found. Install it with: pip install pyodbc")
        except Exception as e:
            raise ConnectionError(f"Error connecting to MSSQL: {str(e)}")
    
    def _connect_sqlite(self, config: Dict[str, Any]):
        """
        Create a SQLite connection.
        
        Args:
            config: Connection configuration
            
        Returns:
            sqlite3 connection object
            
        Raises:
            ConnectionError: If there is an error creating the connection
        """
        try:
            import sqlite3
            
            # Create connection
            conn = sqlite3.connect(
                config.get("database", ":memory:"),
                timeout=config.get("timeout", 30)
            )
            
            return conn
        except ImportError:
            raise ConnectionError("sqlite3 module not found, but it should be included in Python standard library")
        except Exception as e:
            raise ConnectionError(f"Error connecting to SQLite: {str(e)}")