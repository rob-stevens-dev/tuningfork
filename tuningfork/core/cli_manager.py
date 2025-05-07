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
from tuningfork.core.connection_manager import ConnectionManager

logger = logging.getLogger(__name__)


class CLIManager:
    """
    Manages the command-line interface.
    
    This class is responsible for parsing command-line arguments
    and executing the appropriate commands.
    """
    
    def __init__(self, config_manager: ConfigManager, connection_manager: ConnectionManager):
        """
        Initialize the CLIManager.
        
        Args:
            config_manager: A ConfigManager instance.
            connection_manager: A ConnectionManager instance.
        """
        self.config_manager = config_manager
        self.connection_manager = connection_manager
    
    def setup_parser(self) -> argparse.ArgumentParser:
        """
        Set up the command-line argument parser.
        
        Returns:
            An ArgumentParser instance.
        """
        parser = argparse.ArgumentParser(
            description="Database Performance Optimization Tool",
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        
        # Global options
        parser.add_argument(
            "--config", 
            help="Path to configuration file"
        )
        parser.add_argument(
            "--output", 
            choices=["text", "json", "html"], 
            default="text",
            help="Output format"
        )
        parser.add_argument(
            "--verbose", 
            action="store_true", 
            help="Enable verbose output"
        )
        parser.add_argument(
            "--quiet", 
            action="store_true", 
            help="Suppress output except errors"
        )
        
        # Subcommands
        subparsers = parser.add_subparsers(
            dest="command",
            help="Command to execute"
        )
        
        # Connect command
        connect_parser = subparsers.add_parser(
            "connect",
            help="Connect to a database"
        )
        connect_parser.add_argument(
            "--id", 
            required=True,
            help="Unique identifier for this connection"
        )
        connect_parser.add_argument(
            "--type", 
            required=True, 
            choices=["postgres", "mysql", "mssql", "sqlite"],
            help="Database type"
        )
        connect_parser.add_argument(
            "--host", 
            help="Database host"
        )
        connect_parser.add_argument(
            "--port", 
            type=int,
            help="Database port"
        )
        connect_parser.add_argument(
            "--username", 
            help="Database username"
        )
        connect_parser.add_argument(
            "--password", 
            help="Database password"
        )
        connect_parser.add_argument(
            "--database", 
            required=True,
            help="Database name or file path for SQLite"
        )
        connect_parser.add_argument(
            "--save", 
            action="store_true",
            help="Save connection details to configuration"
        )
        
        # Disconnect command
        disconnect_parser = subparsers.add_parser(
            "disconnect",
            help="Disconnect from a database"
        )
        disconnect_parser.add_argument(
            "--id", 
            required=True,
            help="ID of the connection to close"
        )
        
        # List connections command
        list_connections_parser = subparsers.add_parser(
            "list-connections",
            help="List active connections"
        )
        
        # Execute query command
        query_parser = subparsers.add_parser(
            "execute-query",
            help="Execute a SQL query"
        )
        query_parser.add_argument(
            "--id", 
            required=True,
            help="ID of the connection to use"
        )
        query_parser.add_argument(
            "--query", 
            required=True,
            help="SQL query to execute"
        )
        
        return parser
    
    def run(self, args: Optional[List[str]] = None) -> int:
        """
        Run the CLI with the given arguments.
        
        Args:
            args: Command-line arguments. If None, uses sys.argv.
            
        Returns:
            Exit code (0 for success, non-zero for failure).
        """
        parser = self.setup_parser()
        parsed_args = parser.parse_args(args)
        
        # Set up logging based on verbose/quiet flags
        if parsed_args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        elif parsed_args.quiet:
            logging.getLogger().setLevel(logging.ERROR)
        
        # Load configuration if specified
        if parsed_args.config:
            try:
                self.config_manager.load_config(parsed_args.config)
            except Exception as e:
                logger.error(f"Failed to load configuration: {str(e)}")
                return 1
        
        # Execute command
        if not parsed_args.command:
            parser.print_help()
            return 0
        
        try:
            if parsed_args.command == "connect":
                return self._handle_connect(parsed_args)
            elif parsed_args.command == "disconnect":
                return self._handle_disconnect(parsed_args)
            elif parsed_args.command == "list-connections":
                return self._handle_list_connections(parsed_args)
            elif parsed_args.command == "execute-query":
                return self._handle_execute_query(parsed_args)
            else:
                logger.error(f"Unknown command: {parsed_args.command}")
                return 1
        except Exception as e:
            logger.error(f"Error executing command: {str(e)}")
            return 1
    
    def _handle_connect(self, args: argparse.Namespace) -> int:
        """
        Handle the connect command.
        
        Args:
            args: Command-line arguments.
            
        Returns:
            Exit code (0 for success, non-zero for failure).
        """
        # For SQLite, only the database file path is required
        if args.type == "sqlite":
            success, error = self.connection_manager.connect(
                connection_id=args.id,
                db_type=args.type,
                host="",
                port=0,
                username="",
                password="",
                database=args.database
            )
        else:
            # Other database types require host, port, username, and password
            if not args.host or not args.port or not args.username:
                logger.error("Host, port, and username are required for non-SQLite databases")
                return 1
            
            # Get password if not provided
            password = args.password
            if not password:
                import getpass
                password = getpass.getpass(f"Password for {args.username}@{args.host}: ")
            
            success, error = self.connection_manager.connect(
                connection_id=args.id,
                db_type=args.type,
                host=args.host,
                port=args.port,
                username=args.username,
                password=password,
                database=args.database
            )
        
        if success:
            logger.info(f"Connected to {args.type} database as {args.id}")
            if args.save:
                self.config_manager.save_config()
            return 0
        else:
            logger.error(f"Failed to connect: {error}")
            return 1
    
    def _handle_disconnect(self, args: argparse.Namespace) -> int:
        """
        Handle the disconnect command.
        
        Args:
            args: Command-line arguments.
            
        Returns:
            Exit code (0 for success, non-zero for failure).
        """
        success, error = self.connection_manager.disconnect(args.id)
        if success:
            logger.info(f"Disconnected from {args.id}")
            return 0
        else:
            logger.error(f"Failed to disconnect: {error}")
            return 1
    
    def _handle_list_connections(self, args: argparse.Namespace) -> int:
        """
        Handle the list-connections command.
        
        Args:
            args: Command-line arguments.
            
        Returns:
            Exit code (0 for success, non-zero for failure).
        """
        connections = self.connection_manager.list_connections()
        
        if args.output == "json":
            import json
            result = {"connections": []}
            for conn_id in connections:
                info = self.connection_manager.get_connection_info(conn_id)
                # Remove sensitive information
                if "password" in info:
                    del info["password"]
                result["connections"].append({
                    "id": conn_id,
                    "info": info
                })
            print(json.dumps(result, indent=2))
        else:
            if not connections:
                print("No active connections")
            else:
                print("Active connections:")
                for conn_id in connections:
                    info = self.connection_manager.get_connection_info(conn_id)
                    print(f"  - {conn_id}: {info['db_type']} at {info.get('host', 'local')}:{info.get('port', 'N/A')}/{info['database']} as {info.get('username', 'N/A')}")
        
        return 0
    
    def _handle_execute_query(self, args: argparse.Namespace) -> int:
        """
        Handle the execute-query command.
        
        Args:
            args: Command-line arguments.
            
        Returns:
            Exit code (0 for success, non-zero for failure).
        """
        success, result, error = self.connection_manager.execute_query(
            connection_id=args.id,
            query=args.query
        )
        
        if not success:
            logger.error(f"Failed to execute query: {error}")
            return 1
        
        if args.output == "json":
            import json
            print(json.dumps({"result": result}, indent=2))
        else:
            if isinstance(result, int):
                print(f"Query executed successfully. Rows affected: {result}")
            else:
                for row in result:
                    print(row)
        
        return 0