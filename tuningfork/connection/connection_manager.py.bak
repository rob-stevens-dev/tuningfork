"""
Connection Manager for database connections.

This module provides functionality to manage connections to various database systems
across different environments, including local, remote, and cloud platforms.
"""

import logging
import time
import uuid
import queue
from typing import Dict, Any, Optional, Tuple, List, Union, Callable

# Set up logging
logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages database connections across different platforms."""

    def __init__(self, config_manager=None, ssh_manager=None, cloud_connection_factory=None):
        """
        Initialize the Connection Manager.

        Args:
            config_manager: Optional config manager for configuration settings
            ssh_manager: Optional SSH manager for SSH tunneling
            cloud_connection_factory: Optional cloud connection factory for cloud services
        """
        self._connections = {}
        self._connection_pools = {}
        self._active_transactions = {}
        self._config_manager = config_manager
        self._ssh_manager = ssh_manager
        self._cloud_connection_factory = cloud_connection_factory
        
        # If managers are not provided, try to import and create them
        if self._ssh_manager is None:
            try:
                from tuningfork.connection.ssh_manager import SSHManager
                self._ssh_manager = SSHManager()
            except ImportError:
                logger.warning("SSH Manager not available. SSH tunneling will be disabled.")
                
        if self._cloud_connection_factory is None:
            try:
                from tuningfork.connection.cloud_connection_factory import CloudConnectionFactory
                self._cloud_connection_factory = CloudConnectionFactory()
            except ImportError:
                logger.warning("Cloud Connection Factory not available. Cloud services will be disabled.")

    def connect(
        self,
        db_type: str,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        connection_id: Optional[str] = None,
        use_ssh: bool = False,
        ssh_config: Optional[Dict[str, Any]] = None,
        use_cloud: bool = False,
        cloud_config: Optional[Dict[str, Any]] = None,
        connection_options: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Establish a connection to a database.

        Args:
            db_type: Database type (postgres, mysql, mariadb, sqlserver, sqlite)
            host: Database host address
            port: Database port
            username: Database username
            password: Database password
            database: Database name
            connection_id: Optional custom connection ID (if not provided, a UUID will be generated)
            use_ssh: Whether to use SSH tunneling
            ssh_config: SSH configuration (required if use_ssh is True)
            use_cloud: Whether this is a cloud database
            cloud_config: Cloud configuration (required if use_cloud is True)
            connection_options: Additional connection options

        Returns:
            Connection ID string

        Raises:
            ValueError: If invalid parameters are provided
            ImportError: If required dependencies are not installed
            Exception: For other connection errors
        """
        # Generate a connection ID if not provided
        if connection_id is None:
            connection_id = str(uuid.uuid4())
        
        # Check for existing connection with the same ID
        if connection_id in self._connections:
            logger.warning(f"Connection {connection_id} already exists. Returning existing connection.")
            return connection_id
            
        # Initialize connection options
        if connection_options is None:
            connection_options = {}
            
        # Normalize database type
        db_type = db_type.lower()
        
        # Validate database type
        supported_db_types = ["postgres", "postgresql", "mysql", "mariadb", "sqlserver", "mssql", "sqlite", "oracle"]
        if db_type not in supported_db_types:
            raise ValueError(f"Unsupported database type: {db_type}. Supported types: {supported_db_types}")
            
        # Normalize some common aliases
        if db_type == "postgresql":
            db_type = "postgres"
        elif db_type == "mssql":
            db_type = "sqlserver"
            
        # Set up connection parameters
        conn_params = {
            "db_type": db_type,
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database": database,
            "options": connection_options,
        }
        
        # Set up SSH tunnel if requested
        if use_ssh:
            if self._ssh_manager is None:
                raise ValueError("SSH Manager not available. Cannot use SSH tunneling.")
                
            if ssh_config is None:
                raise ValueError("SSH configuration required when use_ssh is True")
                
            # Required SSH config parameters
            required_ssh_params = ["ssh_host", "ssh_port", "ssh_username"]
            for param in required_ssh_params:
                if param not in ssh_config:
                    raise ValueError(f"Missing required SSH parameter: {param}")
            
            # Create a unique tunnel ID
            tunnel_id = f"tunnel_{connection_id}"
            
            # Create SSH tunnel
            tunnel_local_host, tunnel_local_port = self._ssh_manager.create_tunnel(
                tunnel_id=tunnel_id,
                ssh_host=ssh_config["ssh_host"],
                ssh_port=ssh_config["ssh_port"],
                ssh_username=ssh_config["ssh_username"],
                remote_host=host,
                remote_port=port,
                local_port=ssh_config.get("local_port"),
                ssh_password=ssh_config.get("ssh_password"),
                ssh_key_file=ssh_config.get("ssh_key_file"),
                ssh_key_passphrase=ssh_config.get("ssh_key_passphrase"),
                timeout=ssh_config.get("timeout", 10),
            )
            
            # Update connection parameters to use the tunnel
            conn_params["host"] = tunnel_local_host
            conn_params["port"] = tunnel_local_port
            conn_params["ssh_tunnel_id"] = tunnel_id
            
        # Set up cloud connection if requested
        if use_cloud:
            if self._cloud_connection_factory is None:
                raise ValueError("Cloud Connection Factory not available. Cannot use cloud services.")
                
            if cloud_config is None:
                raise ValueError("Cloud configuration required when use_cloud is True")
                
            # Required cloud config parameters
            if "provider" not in cloud_config:
                raise ValueError("Missing required cloud parameter: provider")
                
            # Create a unique cloud connection ID
            cloud_connection_id = f"cloud_{connection_id}"
            
            # Create cloud connection
            cloud_connection = self._cloud_connection_factory.create_connection(
                connection_id=cloud_connection_id,
                cloud_provider=cloud_config["provider"],
                db_type=db_type,
                credentials=cloud_config.get("credentials", {}),
                options=cloud_config.get("options", {}),
            )
            
            # Store cloud connection details
            conn_params["cloud_connection_id"] = cloud_connection_id
            conn_params["cloud_connection"] = cloud_connection
        
        # Create the actual database connection
        connection = self._create_db_connection(db_type, conn_params)
        
        # Store the connection
        self._connections[connection_id] = {
            "connection": connection,
            "db_type": db_type,
            "params": conn_params,
            "created_at": time.time(),
            "last_used": time.time(),
            "transaction_active": False,
        }
        
        logger.info(f"Created connection {connection_id} to {db_type} database")
        return connection_id

    def _create_db_connection(self, db_type: str, params: Dict[str, Any]) -> Any:
        """
        Create a database connection based on the database type.

        Args:
            db_type: Database type
            params: Connection parameters

        Returns:
            Database connection object

        Raises:
            ImportError: If required dependencies are not installed
            Exception: For connection errors
        """
        connection = None
        
        if db_type == "postgres":
            try:
                import psycopg2
                from psycopg2 import pool
            except ImportError:
                raise ImportError("Required package 'psycopg2' not installed")
                
            # Create connection
            try:
                # Get connection options
                connect_timeout = params["options"].get("connect_timeout", 30)
                application_name = params["options"].get("application_name", "DbOptimizer")
                
                # Create connection
                connection = psycopg2.connect(
                    host=params["host"],
                    port=params["port"],
                    user=params["username"],
                    password=params["password"],
                    dbname=params["database"],
                    connect_timeout=connect_timeout,
                    application_name=application_name,
                    **params["options"].get("psycopg2_options", {})
                )
                
                # Set autocommit mode (default is False)
                connection.autocommit = params["options"].get("autocommit", False)
                
            except Exception as e:
                logger.error(f"Failed to connect to PostgreSQL database: {str(e)}")
                raise
                
        elif db_type == "mysql" or db_type == "mariadb":
            try:
                import mysql.connector
                from mysql.connector import pooling
            except ImportError:
                raise ImportError("Required package 'mysql-connector-python' not installed")
                
            # Create connection
            try:
                # Get connection options
                connect_timeout = params["options"].get("connect_timeout", 30)
                
                # Create connection
                connection = mysql.connector.connect(
                    host=params["host"],
                    port=params["port"],
                    user=params["username"],
                    password=params["password"],
                    database=params["database"],
                    connection_timeout=connect_timeout,
                    **params["options"].get("mysql_options", {})
                )
                
                # Set autocommit mode (default is False)
                connection.autocommit = params["options"].get("autocommit", False)
                
            except Exception as e:
                logger.error(f"Failed to connect to MySQL database: {str(e)}")
                raise
                
        elif db_type == "sqlserver":
            try:
                import pyodbc
            except ImportError:
                raise ImportError("Required package 'pyodbc' not installed")
                
            # Create connection
            try:
                # Get connection options
                timeout = params["options"].get("timeout", 30)
                
                # Create connection string
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={params['host']},{params['port']};"
                    f"DATABASE={params['database']};"
                    f"UID={params['username']};"
                    f"PWD={params['password']};"
                    f"Timeout={timeout};"
                )
                
                # Add additional options to connection string
                for key, value in params["options"].get("odbc_options", {}).items():
                    conn_str += f"{key}={value};"
                
                # Create connection
                connection = pyodbc.connect(conn_str)
                
                # Set autocommit mode (default is False)
                connection.autocommit = params["options"].get("autocommit", False)
                
            except Exception as e:
                logger.error(f"Failed to connect to SQL Server database: {str(e)}")
                raise
                
        elif db_type == "sqlite":
            try:
                import sqlite3
            except ImportError:
                raise ImportError("Required package 'sqlite3' not installed")
                
            # Create connection
            try:
                # Get connection options
                timeout = params["options"].get("timeout", 30)
                
                # For SQLite, host is the database file path
                db_path = params["database"]
                
                # Create connection
                connection = sqlite3.connect(
                    db_path,
                    timeout=timeout,
                    **params["options"].get("sqlite_options", {})
                )
                
            except Exception as e:
                logger.error(f"Failed to connect to SQLite database: {str(e)}")
                raise
                
        elif db_type == "oracle":
            try:
                import cx_Oracle
            except ImportError:
                raise ImportError("Required package 'cx_Oracle' not installed")
                
            # Create connection
            try:
                # Create connection
                dsn = cx_Oracle.makedsn(
                    params["host"],
                    params["port"],
                    service_name=params["database"]
                )
                
                connection = cx_Oracle.connect(
                    user=params["username"],
                    password=params["password"],
                    dsn=dsn,
                    **params["options"].get("oracle_options", {})
                )
                
                # Set autocommit mode (default is False)
                connection.autocommit = params["options"].get("autocommit", False)
                
            except Exception as e:
                logger.error(f"Failed to connect to Oracle database: {str(e)}")
                raise
        
        return connection

    def create_connection_pool(
        self,
        pool_id: str,
        db_type: str,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        min_connections: int = 1,
        max_connections: int = 10,
        use_ssh: bool = False,
        ssh_config: Optional[Dict[str, Any]] = None,
        use_cloud: bool = False,
        cloud_config: Optional[Dict[str, Any]] = None,
        connection_options: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Create a connection pool for a database.

        Args:
            pool_id: Unique identifier for the connection pool
            db_type: Database type
            host: Database host address
            port: Database port
            username: Database username
            password: Database password
            database: Database name
            min_connections: Minimum number of connections in the pool
            max_connections: Maximum number of connections in the pool
            use_ssh: Whether to use SSH tunneling
            ssh_config: SSH configuration (required if use_ssh is True)
            use_cloud: Whether this is a cloud database
            cloud_config: Cloud configuration (required if use_cloud is True)
            connection_options: Additional connection options

        Returns:
            Pool ID string

        Raises:
            ValueError: If invalid parameters are provided
            ImportError: If required dependencies are not installed
            Exception: For other connection errors
        """
        # Check for existing pool with the same ID
        if pool_id in self._connection_pools:
            logger.warning(f"Connection pool {pool_id} already exists. Returning existing pool.")
            return pool_id
            
        # Initialize connection options
        if connection_options is None:
            connection_options = {}
            
        # Normalize database type
        db_type = db_type.lower()
        
        # Validate database type
        supported_db_types = ["postgres", "postgresql", "mysql", "mariadb", "sqlserver", "mssql"]
        if db_type not in supported_db_types:
            raise ValueError(f"Unsupported database type for connection pool: {db_type}. "
                           f"Supported types: {supported_db_types}")
            
        # SQLite doesn't support connection pooling
        if db_type == "sqlite":
            raise ValueError("SQLite does not support connection pooling")
            
        # Normalize some common aliases
        if db_type == "postgresql":
            db_type = "postgres"
        elif db_type == "mssql":
            db_type = "sqlserver"
            
        # Set up SSH tunnel if requested
        tunnel_id = None
        if use_ssh:
            if self._ssh_manager is None:
                raise ValueError("SSH Manager not available. Cannot use SSH tunneling.")
                
            if ssh_config is None:
                raise ValueError("SSH configuration required when use_ssh is True")
                
            # Required SSH config parameters
            required_ssh_params = ["ssh_host", "ssh_port", "ssh_username"]
            for param in required_ssh_params:
                if param not in ssh_config:
                    raise ValueError(f"Missing required SSH parameter: {param}")
            
            # Create a unique tunnel ID
            tunnel_id = f"tunnel_pool_{pool_id}"
            
            # Create SSH tunnel
            tunnel_local_host, tunnel_local_port = self._ssh_manager.create_tunnel(
                tunnel_id=tunnel_id,
                ssh_host=ssh_config["ssh_host"],
                ssh_port=ssh_config["ssh_port"],
                ssh_username=ssh_config["ssh_username"],
                remote_host=host,
                remote_port=port,
                local_port=ssh_config.get("local_port"),
                ssh_password=ssh_config.get("ssh_password"),
                ssh_key_file=ssh_config.get("ssh_key_file"),
                ssh_key_passphrase=ssh_config.get("ssh_key_passphrase"),
                timeout=ssh_config.get("timeout", 10),
            )
            
            # Update connection parameters to use the tunnel
            host = tunnel_local_host
            port = tunnel_local_port
        
        # Set up cloud connection if requested
        cloud_connection_id = None
        if use_cloud:
            if self._cloud_connection_factory is None:
                raise ValueError("Cloud Connection Factory not available. Cannot use cloud services.")
                
            if cloud_config is None:
                raise ValueError("Cloud configuration required when use_cloud is True")
                
            # Required cloud config parameters
            if "provider" not in cloud_config:
                raise ValueError("Missing required cloud parameter: provider")
                
            # Create a unique cloud connection ID
            cloud_connection_id = f"cloud_pool_{pool_id}"
            
            # Create cloud connection
            cloud_connection = self._cloud_connection_factory.create_connection(
                connection_id=cloud_connection_id,
                cloud_provider=cloud_config["provider"],
                db_type=db_type,
                credentials=cloud_config.get("credentials", {}),
                options=cloud_config.get("options", {}),
            )
        
        # Create the connection pool based on the database type
        pool = None
        
        if db_type == "postgres":
            try:
                import psycopg2
                from psycopg2 import pool
            except ImportError:
                raise ImportError("Required package 'psycopg2' not installed")
                
            # Create connection pool
            try:
                # Get connection options
                connect_timeout = connection_options.get("connect_timeout", 30)
                application_name = connection_options.get("application_name", "DbOptimizer")
                
                # Create connection pool
                pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=min_connections,
                    maxconn=max_connections,
                    host=host,
                    port=port,
                    user=username,
                    password=password,
                    dbname=database,
                    connect_timeout=connect_timeout,
                    application_name=application_name,
                    **connection_options.get("psycopg2_options", {})
                )
                
            except Exception as e:
                logger.error(f"Failed to create PostgreSQL connection pool: {str(e)}")
                raise
                
        elif db_type == "mysql" or db_type == "mariadb":
            try:
                import mysql.connector
                from mysql.connector import pooling
            except ImportError:
                raise ImportError("Required package 'mysql-connector-python' not installed")
                
            # Create connection pool
            try:
                # Get connection options
                connect_timeout = connection_options.get("connect_timeout", 30)
                
                # Create connection pool configuration
                pool_config = {
                    "pool_name": pool_id,
                    "pool_size": max_connections,
                    "host": host,
                    "port": port,
                    "user": username,
                    "password": password,
                    "database": database,
                    "connection_timeout": connect_timeout,
                    **connection_options.get("mysql_options", {})
                }
                
                # Create connection pool
                pool = mysql.connector.pooling.MySQLConnectionPool(**pool_config)
                
            except Exception as e:
                logger.error(f"Failed to create MySQL connection pool: {str(e)}")
                raise
                
        elif db_type == "sqlserver":
            # SQL Server doesn't have a built-in connection pool in pyodbc
            # We'll implement a simple pool ourselves
            try:
                import pyodbc
                import queue
            except ImportError:
                raise ImportError("Required packages 'pyodbc' and 'queue' not installed")
                
            # Create connection pool
            try:
                # Get connection options
                timeout = connection_options.get("timeout", 30)
                
                # Create connection string
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={host},{port};"
                    f"DATABASE={database};"
                    f"UID={username};"
                    f"PWD={password};"
                    f"Timeout={timeout};"
                )
                
                # Add additional options to connection string
                for key, value in connection_options.get("odbc_options", {}).items():
                    conn_str += f"{key}={value};"
                
                # Create a queue for connections
                connection_queue = queue.Queue(maxsize=max_connections)
                
                # Create initial connections
                for _ in range(min_connections):
                    connection = pyodbc.connect(conn_str)
                    connection_queue.put(connection)
                
                # Create a simple wrapper for the pool
                pool = {
                    "queue": connection_queue,
                    "conn_str": conn_str,
                    "min_connections": min_connections,
                    "max_connections": max_connections,
                    "current_connections": min_connections,
                    "connection_creator": lambda: pyodbc.connect(conn_str),
                }
                
            except Exception as e:
                logger.error(f"Failed to create SQL Server connection pool: {str(e)}")
                raise
        
        # Store the pool
        self._connection_pools[pool_id] = {
            "pool": pool,
            "db_type": db_type,
            "host": host,
            "port": port,
            "username": username,
            "database": database,
            "min_connections": min_connections,
            "max_connections": max_connections,
            "tunnel_id": tunnel_id,
            "cloud_connection_id": cloud_connection_id,
            "created_at": time.time(),
            "last_used": time.time(),
        }
        
        logger.info(f"Created connection pool {pool_id} for {db_type} database")
        return pool_id

    def get_connection_from_pool(self, pool_id: str) -> Tuple[str, Any]:
        """
        Get a connection from a connection pool.

        Args:
            pool_id: ID of the pool to get a connection from

        Returns:
            Tuple of (connection_id, connection)

        Raises:
            ValueError: If pool_id is not found
            Exception: If unable to get a connection from the pool
        """
        if pool_id not in self._connection_pools:
            raise ValueError(f"Connection pool {pool_id} not found")
            
        # Generate a unique connection ID
        connection_id = f"{pool_id}_{str(uuid.uuid4())}"
        
        # Get pool details
        pool_details = self._connection_pools[pool_id]
        pool = pool_details["pool"]
        db_type = pool_details["db_type"]
        
        # Get a connection from the pool based on the database type
        connection = None
        
        try:
            if db_type == "postgres":
                # PostgreSQL pool
                connection = pool.getconn()
                
            elif db_type == "mysql" or db_type == "mariadb":
                # MySQL pool
                connection = pool.get_connection()
                
            elif db_type == "sqlserver":
                # Our custom SQL Server pool
                try:
                    # Try to get a connection from the queue
                    connection = pool["queue"].get(block=False)
                    
                except queue.Empty:
                    # If the queue is empty and we haven't reached max connections,
                    # create a new connection
                    if pool["current_connections"] < pool["max_connections"]:
                        connection = pool["connection_creator"]()
                        pool["current_connections"] += 1
                    else:
                        # Wait for a connection to become available
                        connection = pool["queue"].get(block=True, timeout=30)
            
            # Store the connection
            self._connections[connection_id] = {
                "connection": connection,
                "db_type": db_type,
                "pool_id": pool_id,
                "created_at": time.time(),
                "last_used": time.time(),
                "transaction_active": False,
                "from_pool": True,
            }
            
            # Update pool last used time
            pool_details["last_used"] = time.time()
            
            logger.debug(f"Got connection {connection_id} from pool {pool_id}")
            return connection_id, connection
            
        except Exception as e:
            logger.error(f"Failed to get connection from pool {pool_id}: {str(e)}")
            raise

    def return_connection_to_pool(self, connection_id: str) -> None:
        """
        Return a connection to its pool.

        Args:
            connection_id: ID of the connection to return

        Raises:
            ValueError: If connection_id is not found or the connection is not from a pool
            Exception: If unable to return the connection to the pool
        """
        if connection_id not in self._connections:
            raise ValueError(f"Connection {connection_id} not found")
            
        connection_details = self._connections[connection_id]
        
        # Check if the connection is from a pool
        if not connection_details.get("from_pool", False):
            raise ValueError(f"Connection {connection_id} is not from a pool")
            
        # Get the pool ID
        pool_id = connection_details["pool_id"]
        
        if pool_id not in self._connection_pools:
            raise ValueError(f"Connection pool {pool_id} not found")
            
        # Get the connection and pool
        connection = connection_details["connection"]
        pool = self._connection_pools[pool_id]["pool"]
        db_type = connection_details["db_type"]
        
        # Check if a transaction is active
        if connection_details["transaction_active"]:
            logger.warning(f"Connection {connection_id} has an active transaction. Rolling back before returning to pool.")
            try:
                self.rollback(connection_id)
            except Exception as e:
                logger.error(f"Failed to rollback transaction on connection {connection_id}: {str(e)}")
        
        # Return the connection to the pool based on the database type
        try:
            if db_type == "postgres":
                # PostgreSQL pool
                pool.putconn(connection)
                
            elif db_type == "mysql" or db_type == "mariadb":
                # MySQL pool
                connection.close()  # For MySQL, close() actually returns the connection to the pool
                
            elif db_type == "sqlserver":
                # Our custom SQL Server pool
                # Reset the connection state if needed
                if hasattr(connection, "rollback"):
                    connection.rollback()
                
                # Put the connection back in the queue
                pool["queue"].put(connection)
            
            # Remove the connection from our tracking
            del self._connections[connection_id]
            
            logger.debug(f"Returned connection {connection_id} to pool {pool_id}")
            
        except Exception as e:
            logger.error(f"Failed to return connection {connection_id} to pool {pool_id}: {str(e)}")
            raise

    def close_connection_pool(self, pool_id: str) -> None:
        """
        Close a connection pool and all its connections.

        Args:
            pool_id: ID of the pool to close

        Raises:
            ValueError: If pool_id is not found
            Exception: If unable to close the connection pool
        """
        if pool_id not in self._connection_pools:
            raise ValueError(f"Connection pool {pool_id} not found")
            
        # Get pool details
        pool_details = self._connection_pools[pool_id]
        pool = pool_details["pool"]
        db_type = pool_details["db_type"]
        
        # Close all connections from this pool
        connections_to_close = [
            conn_id for conn_id, conn_details in self._connections.items()
            if conn_details.get("pool_id") == pool_id
        ]
        
        for conn_id in connections_to_close:
            try:
                self.return_connection_to_pool(conn_id)
            except Exception as e:
                logger.warning(f"Error returning connection {conn_id} to pool {pool_id}: {str(e)}")
        
        # Close the pool based on the database type
        try:
            if db_type == "postgres":
                # PostgreSQL pool
                pool.closeall()
                
            elif db_type == "mysql" or db_type == "mariadb":
                # MySQL pools automatically close when they go out of scope
                pass
                
            elif db_type == "sqlserver":
                # Our custom SQL Server pool
                # Close all connections in the queue
                while not pool["queue"].empty():
                    try:
                        connection = pool["queue"].get(block=False)
                        connection.close()
                    except queue.Empty:
                        break
            
            # Close SSH tunnel if used
            tunnel_id = pool_details.get("tunnel_id")
            if tunnel_id and self._ssh_manager:
                try:
                    self._ssh_manager.close_tunnel(tunnel_id)
                except Exception as e:
                    logger.warning(f"Error closing SSH tunnel {tunnel_id} for pool {pool_id}: {str(e)}")
            
            # Close cloud connection if used
            cloud_connection_id = pool_details.get("cloud_connection_id")
            if cloud_connection_id and self._cloud_connection_factory:
                try:
                    self._cloud_connection_factory.close_connection(cloud_connection_id)
                except Exception as e:
                    logger.warning(f"Error closing cloud connection {cloud_connection_id} for pool {pool_id}: {str(e)}")
            
            # Remove the pool from our tracking
            del self._connection_pools[pool_id]
            
            logger.info(f"Closed connection pool {pool_id}")
            
        except Exception as e:
            logger.error(f"Failed to close connection pool {pool_id}: {str(e)}")
            raise

    def disconnect(self, connection_id: str) -> None:
        """
        Close a database connection.

        Args:
            connection_id: ID of the connection to close

        Raises:
            ValueError: If connection_id is not found
            Exception: If unable to close the connection
        """
        if connection_id not in self._connections:
            raise ValueError(f"Connection {connection_id} not found")
            
        connection_details = self._connections[connection_id]
        
        # Check if the connection is from a pool
        if connection_details.get("from_pool", False):
            return self.return_connection_to_pool(connection_id)
            
        # Get the connection
        connection = connection_details["connection"]
        db_type = connection_details["db_type"]
        
        # Check if a transaction is active
        if connection_details["transaction_active"]:
            logger.warning(f"Connection {connection_id} has an active transaction. Rolling back before closing.")
            try:
                self.rollback(connection_id)
            except Exception as e:
                logger.error(f"Failed to rollback transaction on connection {connection_id}: {str(e)}")
        
        # Close the connection based on the database type
        try:
            # All database interfaces support a close() method
            connection.close()
            
            # Close SSH tunnel if used
            tunnel_id = connection_details["params"].get("ssh_tunnel_id")
            if tunnel_id and self._ssh_manager:
                try:
                    self._ssh_manager.close_tunnel(tunnel_id)
                except Exception as e:
                    logger.warning(f"Error closing SSH tunnel {tunnel_id} for connection {connection_id}: {str(e)}")
            
            # Close cloud connection if used
            cloud_connection_id = connection_details["params"].get("cloud_connection_id")
            if cloud_connection_id and self._cloud_connection_factory:
                try:
                    self._cloud_connection_factory.close_connection(cloud_connection_id)
                except Exception as e:
                    logger.warning(f"Error closing cloud connection {cloud_connection_id} for connection {connection_id}: {str(e)}")
            
            # Remove the connection from our tracking
            del self._connections[connection_id]
            
            logger.info(f"Closed connection {connection_id}")
            
        except Exception as e:
            logger.error(f"Failed to close connection {connection_id}: {str(e)}")
            raise

    def execute_query(
        self,
        connection_id: str,
        query: str,
        params: Optional[Any] = None,
        fetchall: bool = False,
        fetchone: bool = False,
    ) -> Any:
        """
        Execute a query on the specified connection.

        Args:
            connection_id: ID of the connection to use
            query: SQL query to execute
            params: Query parameters
            fetchall: Whether to fetch all results
            fetchone: Whether to fetch one result

        Returns:
            Query results if fetchall or fetchone is True, otherwise the cursor

        Raises:
            ValueError: If connection_id is not found
            Exception: For query execution errors
        """
        if connection_id not in self._connections:
            raise ValueError(f"Connection {connection_id} not found")
            
        connection_details = self._connections[connection_id]
        connection = connection_details["connection"]
        db_type = connection_details["db_type"]
        
        # Update last used time
        connection_details["last_used"] = time.time()
        
        # Execute the query based on the database type
        try:
            cursor = None
            
            if db_type == "postgres":
                cursor = connection.cursor()
                cursor.execute(query, params or ())
                
            elif db_type == "mysql" or db_type == "mariadb":
                cursor = connection.cursor()
                cursor.execute(query, params or ())
                
            elif db_type == "sqlserver":
                cursor = connection.cursor()
                cursor.execute(query, params or ())
                
            elif db_type == "sqlite":
                cursor = connection.cursor()
                cursor.execute(query, params or ())
                
            elif db_type == "oracle":
                cursor = connection.cursor()
                cursor.execute(query, params or ())
            
            # Fetch results if requested
            if fetchall:
                result = cursor.fetchall()
                cursor.close()
                return result
                
            elif fetchone:
                result = cursor.fetchone()
                cursor.close()
                return result
                
            else:
                return cursor
                
        except Exception as e:
            logger.error(f"Failed to execute query on connection {connection_id}: {str(e)}")
            raise

    def begin_transaction(self, connection_id: str) -> None:
        """
        Begin a transaction on the specified connection.

        Args:
            connection_id: ID of the connection to use

        Raises:
            ValueError: If connection_id is not found or a transaction is already active
            Exception: For transaction errors
        """
        if connection_id not in self._connections:
            raise ValueError(f"Connection {connection_id} not found")
            
        connection_details = self._connections[connection_id]
        
        # Check if a transaction is already active
        if connection_details["transaction_active"]:
            raise ValueError(f"Transaction already active on connection {connection_id}")
            
        connection = connection_details["connection"]
        db_type = connection_details["db_type"]
        
        # Begin transaction based on the database type
        try:
            if db_type in ["postgres", "mysql", "mariadb", "sqlserver", "oracle"]:
                # These databases typically support the same transaction interface
                connection.begin()
                
            elif db_type == "sqlite":
                # SQLite doesn't have an explicit begin() method, but we can execute BEGIN
                cursor = connection.cursor()
                cursor.execute("BEGIN")
                cursor.close()
            
            # Mark transaction as active
            connection_details["transaction_active"] = True
            logger.debug(f"Started transaction on connection {connection_id}")
            
        except Exception as e:
            logger.error(f"Failed to begin transaction on connection {connection_id}: {str(e)}")
            raise

    def commit(self, connection_id: str) -> None:
        """
        Commit a transaction on the specified connection.

        Args:
            connection_id: ID of the connection to use

        Raises:
            ValueError: If connection_id is not found or no transaction is active
            Exception: For transaction errors
        """
        if connection_id not in self._connections:
            raise ValueError(f"Connection {connection_id} not found")
            
        connection_details = self._connections[connection_id]
        
        # Check if a transaction is active
        if not connection_details["transaction_active"]:
            raise ValueError(f"No active transaction on connection {connection_id}")
            
        connection = connection_details["connection"]
        
        # Commit transaction
        try:
            connection.commit()
            
            # Mark transaction as inactive
            connection_details["transaction_active"] = False
            logger.debug(f"Committed transaction on connection {connection_id}")
            
        except Exception as e:
            logger.error(f"Failed to commit transaction on connection {connection_id}: {str(e)}")
            raise

    def rollback(self, connection_id: str) -> None:
        """
        Rollback a transaction on the specified connection.

        Args:
            connection_id: ID of the connection to use

        Raises:
            ValueError: If connection_id is not found or no transaction is active
            Exception: For transaction errors
        """
        if connection_id not in self._connections:
            raise ValueError(f"Connection {connection_id} not found")
            
        connection_details = self._connections[connection_id]
        
        # Check if a transaction is active
        if not connection_details["transaction_active"]:
            raise ValueError(f"No active transaction on connection {connection_id}")
            
        connection = connection_details["connection"]
        
        # Rollback transaction
        try:
            connection.rollback()
            
            # Mark transaction as inactive
            connection_details["transaction_active"] = False
            logger.debug(f"Rolled back transaction on connection {connection_id}")
            
        except Exception as e:
            logger.error(f"Failed to rollback transaction on connection {connection_id}: {str(e)}")
            raise

    def list_connections(self) -> List[Dict[str, Any]]:
        """
        List all active connections.

        Returns:
            List of dictionaries with connection details
        """
        return [
            {
                "connection_id": conn_id,
                "db_type": details["db_type"],
                "from_pool": details.get("from_pool", False),
                "pool_id": details.get("pool_id"),
                "created_at": details["created_at"],
                "last_used": details["last_used"],
                "transaction_active": details["transaction_active"],
            }
            for conn_id, details in self._connections.items()
        ]

    def list_connection_pools(self) -> List[Dict[str, Any]]:
        """
        List all active connection pools.

        Returns:
            List of dictionaries with connection pool details
        """
        return [
            {
                "pool_id": pool_id,
                "db_type": details["db_type"],
                "host": details["host"],
                "port": details["port"],
                "database": details["database"],
                "min_connections": details["min_connections"],
                "max_connections": details["max_connections"],
                "created_at": details["created_at"],
                "last_used": details["last_used"],
            }
            for pool_id, details in self._connection_pools.items()
        ]

    def close_all_connections(self) -> None:
        """Close all active connections and connection pools."""
        # Close all connection pools first
        pool_ids = list(self._connection_pools.keys())
        for pool_id in pool_ids:
            try:
                self.close_connection_pool(pool_id)
            except Exception as e:
                logger.error(f"Error closing connection pool {pool_id}: {str(e)}")
        
        # Close all remaining connections
        connection_ids = list(self._connections.keys())
        for connection_id in connection_ids:
            try:
                self.disconnect(connection_id)
            except Exception as e:
                logger.error(f"Error closing connection {connection_id}: {str(e)}")

    def __del__(self) -> None:
        """Ensure all connections are closed when the object is destroyed."""
        try:
            self.close_all_connections()
        except Exception:
            pass