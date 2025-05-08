"""
ConnectionAdapter module for TuningFork database performance optimization tool.

This module provides an adapter for making different ConnectionManager
implementations work with the ResourceAnalyzer.
"""

from tuningfork.connection.connection_manager import ConnectionManager


class ConnectionAdapter:
    """
    Adapter for making different ConnectionManager implementations compatible.
    
    This adapter wraps a ConnectionManager instance and provides the interface
    expected by the ResourceAnalyzer.
    """
    
    def __init__(self, connection_manager, config_manager=None):
        """
        Initialize the ConnectionAdapter.
        
        Args:
            connection_manager: The ConnectionManager instance to adapt
            config_manager: Optional ConfigManager instance
        """
        self.connection_manager = connection_manager
        self.config_manager = config_manager
        self.connections = {}  # Store connections by ID
    
    def connect(self, connection_id, **kwargs):
        """
        Create a connection with the specified ID.
        
        Args:
            connection_id: The connection ID
            **kwargs: Additional connection parameters
            
        Returns:
            The Connection object
        """
        # Use connection_id as db_type if not specified
        db_type = kwargs.get('db_type', connection_id)
        
        # Set default parameters based on db_type
        if db_type == 'sqlite':
            defaults = {
                'host': ':memory:',
                'port': 0,
                'username': '',
                'password': '',
                'database': ':memory:'
            }
        elif db_type in ('postgresql', 'postgres'):
            defaults = {
                'host': 'localhost',
                'port': 5432,
                'username': 'postgres',
                'password': 'postgres',
                'database': 'postgres'
            }
        elif db_type in ('mysql', 'mariadb'):
            defaults = {
                'host': 'localhost',
                'port': 3306,
                'username': 'root',
                'password': 'root',
                'database': 'mysql'
            }
        elif db_type in ('mssql', 'sqlserver'):
            defaults = {
                'host': 'localhost',
                'port': 1433,
                'username': 'sa',
                'password': 'P@ssw0rd',
                'database': 'master'
            }
        else:
            defaults = {
                'host': 'localhost',
                'port': 0,
                'username': '',
                'password': '',
                'database': ''
            }
        
        # Merge defaults with provided kwargs
        params = defaults.copy()
        params.update(kwargs)
        
        # Add db_type if not already provided
        params.setdefault('db_type', db_type)
        
        # Create the connection
        connection = self.connection_manager.connect(**params)
        
        # Store the connection
        self.connections[connection_id] = connection
        
        return connection
    
    def get_connection(self, connection_id):
        """
        Get a previously created connection.
        
        Args:
            connection_id: The connection ID
            
        Returns:
            The Connection object or None if not found
        """
        return self.connections.get(connection_id)
    
    def close_connection(self, connection_id):
        """
        Close a connection.
        
        Args:
            connection_id: The connection ID
            
        Returns:
            True if closed successfully, False otherwise
        """
        connection = self.connections.get(connection_id)
        if connection:
            connection.close()
            del self.connections[connection_id]
            return True
        return False
    
    def close_all_connections(self):
        """Close all connections."""
        for conn_id in list(self.connections.keys()):
            self.close_connection(conn_id)
    
    def __getattr__(self, name):
        """
        Forward any other method calls to the wrapped connection manager.
        
        Args:
            name: Method name
            
        Returns:
            The method from the wrapped connection manager
        """
        return getattr(self.connection_manager, name)
