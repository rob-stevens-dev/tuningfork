"""
Tuning Fork - Database Performance Optimization Tool
Unit tests for CLIManager
"""

import unittest
import io
import sys
import argparse
from unittest.mock import patch, MagicMock

from tuningfork.core.config_manager import ConfigManager
from tuningfork.core.connection_manager import ConnectionManager
from tuningfork.core.cli_manager import CLIManager

class TestCLIManager(unittest.TestCase):
    """Test cases for the CLIManager class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create mock managers
        self.config_manager = MagicMock(spec=ConfigManager)
        self.connection_manager = MagicMock(spec=ConnectionManager)
        
        # Initialize CLIManager with the mock managers
        self.cli_manager = CLIManager(self.config_manager, self.connection_manager)
    
    def test_setup_parser(self):
        """Test setting up the argument parser."""
        parser = self.cli_manager.setup_parser()
        
        # Verify the parser was created
        self.assertIsNotNone(parser)
        
        # Verify global options
        self.assertIn("--config", parser.format_help())
        self.assertIn("--output", parser.format_help())
        self.assertIn("--verbose", parser.format_help())
        self.assertIn("--quiet", parser.format_help())
        
        # Verify commands
        self.assertIn("connect", parser.format_help())
        self.assertIn("disconnect", parser.format_help())
        self.assertIn("list-connections", parser.format_help())
        self.assertIn("execute-query", parser.format_help())
    
    def test_run_without_command(self):
        """Test running without a command."""
        # Capture stdout to verify help output
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            # Call the method with no arguments
            exit_code = self.cli_manager.run([])
            
            # Verify the result
            self.assertEqual(exit_code, 0)
            
            # Verify help was printed
            self.assertIn("usage:", captured_output.getvalue())
        finally:
            # Restore stdout
            sys.stdout = sys.__stdout__
    
    def test_run_with_config(self):
        """Test running with a configuration file."""
        # Call the method with a configuration file
        exit_code = self.cli_manager.run(["--config", "test_config.json", "list-connections"])
        
        # Verify the result
        self.assertEqual(exit_code, 0)
        
        # Verify the configuration was loaded
        self.config_manager.load_config.assert_called_once_with("test_config.json")
    
    @patch('getpass.getpass', return_value="test_password")
    def test_handle_connect(self, mock_getpass):
        """Test handling the connect command."""
        # Set up the mock
        self.connection_manager.connect.return_value = (True, None)
        
        # Call the method with PostgreSQL
        args = argparse.Namespace(
            id="test_postgres",
            type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password=None,  # Password will be prompted
            database="test_db",
            save=True
        )
        
        exit_code = self.cli_manager._handle_connect(args)
        
        # Verify the result
        self.assertEqual(exit_code, 0)
        
        # Verify connect was called with the correct arguments
        self.connection_manager.connect.assert_called_once_with(
            connection_id="test_postgres",
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Verify config was saved
        self.config_manager.save_config.assert_called_once()
    
    def test_handle_connect_sqlite(self):
        """Test handling the connect command for SQLite."""
        # Set up the mock
        self.connection_manager.connect.return_value = (True, None)
        
        # Call the method with SQLite
        args = argparse.Namespace(
            id="test_sqlite",
            type="sqlite",
            host=None,
            port=None,
            username=None,
            password=None,
            database="test.db",
            save=False
        )
        
        exit_code = self.cli_manager._handle_connect(args)
        
        # Verify the result
        self.assertEqual(exit_code, 0)
        
        # Verify connect was called with the correct arguments
        self.connection_manager.connect.assert_called_once_with(
            connection_id="test_sqlite",
            db_type="sqlite",
            host="",
            port=0,
            username="",
            password="",
            database="test.db"
        )
    
    def test_handle_connect_missing_args(self):
        """Test handling the connect command with missing arguments."""
        # Call the method with missing arguments
        args = argparse.Namespace(
            id="test_postgres",
            type="postgres",
            host=None,  # Missing
            port=None,  # Missing
            username=None,  # Missing
            password=None,
            database="test_db",
            save=False
        )
        
        exit_code = self.cli_manager._handle_connect(args)
        
        # Verify the result
        self.assertEqual(exit_code, 1)
        
        # Verify connect was not called
        self.connection_manager.connect.assert_not_called()
    
    def test_handle_connect_failure(self):
        """Test handling a failed connect command."""
        # Set up the mock
        self.connection_manager.connect.return_value = (False, "Connection error")
        
        # Call the method with PostgreSQL
        args = argparse.Namespace(
            id="test_postgres",
            type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db",
            save=False
        )
        
        exit_code = self.cli_manager._handle_connect(args)
        
        # Verify the result
        self.assertEqual(exit_code, 1)
    
    def test_handle_disconnect(self):
        """Test handling the disconnect command."""
        # Set up the mock
        self.connection_manager.disconnect.return_value = (True, None)
        
        # Call the method
        args = argparse.Namespace(id="test_connection")
        exit_code = self.cli_manager._handle_disconnect(args)
        
        # Verify the result
        self.assertEqual(exit_code, 0)
        
        # Verify disconnect was called with the correct arguments
        self.connection_manager.disconnect.assert_called_once_with("test_connection")
    
    def test_handle_disconnect_failure(self):
        """Test handling a failed disconnect command."""
        # Set up the mock
        self.connection_manager.disconnect.return_value = (False, "Disconnect error")
        
        # Call the method
        args = argparse.Namespace(id="test_connection")
        exit_code = self.cli_manager._handle_disconnect(args)
        
        # Verify the result
        self.assertEqual(exit_code, 1)
    
    def test_handle_list_connections(self):
        """Test handling the list-connections command."""
        # Set up the mocks
        self.connection_manager.list_connections.return_value = ["connection1", "connection2"]
        self.connection_manager.get_connection_info.side_effect = lambda conn_id: {
            "connection1": {
                "db_type": "postgres",
                "host": "localhost",
                "port": 5432,
                "username": "user1",
                "database": "db1"
            },
            "connection2": {
                "db_type": "mysql",
                "host": "localhost",
                "port": 3306,
                "username": "user2",
                "database": "db2"
            }
        }[conn_id]
        
        # Capture stdout to verify output
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            # Call the method with text output
            args = argparse.Namespace(output="text")
            exit_code = self.cli_manager._handle_list_connections(args)
            
            # Verify the result
            self.assertEqual(exit_code, 0)
            
            # Verify output
            output = captured_output.getvalue()
            self.assertIn("connection1", output)
            self.assertIn("connection2", output)
            self.assertIn("postgres", output)
            self.assertIn("mysql", output)
        finally:
            # Restore stdout
            sys.stdout = sys.__stdout__
    
    def test_handle_list_connections_json(self):
        """Test handling the list-connections command with JSON output."""
        # Set up the mocks
        self.connection_manager.list_connections.return_value = ["connection1", "connection2"]
        self.connection_manager.get_connection_info.side_effect = lambda conn_id: {
            "connection1": {
                "db_type": "postgres",
                "host": "localhost",
                "port": 5432,
                "username": "user1",
                "database": "db1"
            },
            "connection2": {
                "db_type": "mysql",
                "host": "localhost",
                "port": 3306,
                "username": "user2",
                "database": "db2"
            }
        }[conn_id]
        
        # Capture stdout to verify output
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            # Call the method with JSON output
            args = argparse.Namespace(output="json")
            exit_code = self.cli_manager._handle_list_connections(args)
            
            # Verify the result
            self.assertEqual(exit_code, 0)
            
            # Verify output
            import json
            output = json.loads(captured_output.getvalue())
            
            self.assertIn("connections", output)
            self.assertEqual(len(output["connections"]), 2)
            
            # Verify connection1 details
            connection1 = next(c for c in output["connections"] if c["id"] == "connection1")
            self.assertEqual(connection1["info"]["db_type"], "postgres")
            self.assertEqual(connection1["info"]["port"], 5432)
            
            # Verify connection2 details
            connection2 = next(c for c in output["connections"] if c["id"] == "connection2")
            self.assertEqual(connection2["info"]["db_type"], "mysql")
            self.assertEqual(connection2["info"]["port"], 3306)
        finally:
            # Restore stdout
            sys.stdout = sys.__stdout__
    
    def test_handle_list_connections_empty(self):
        """Test handling the list-connections command with no connections."""
        # Set up the mock
        self.connection_manager.list_connections.return_value = []
        
        # Capture stdout to verify output
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            # Call the method
            args = argparse.Namespace(output="text")
            exit_code = self.cli_manager._handle_list_connections(args)
            
            # Verify the result
            self.assertEqual(exit_code, 0)
            
            # Verify output
            self.assertIn("No active connections", captured_output.getvalue())
        finally:
            # Restore stdout
            sys.stdout = sys.__stdout__
    
    def test_handle_execute_query(self):
        """Test handling the execute-query command."""
        # Set up the mock
        self.connection_manager.execute_query.return_value = (True, [("row1",), ("row2",)], None)
        
        # Capture stdout to verify output
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            # Call the method
            args = argparse.Namespace(
                id="test_connection",
                query="SELECT * FROM test_table",
                output="text"
            )
            exit_code = self.cli_manager._handle_execute_query(args)
            
            # Verify the result
            self.assertEqual(exit_code, 0)
            
            # Verify execute_query was called with the correct arguments
            self.connection_manager.execute_query.assert_called_once_with(
                connection_id="test_connection",
                query="SELECT * FROM test_table"
            )
            
            # Verify output
            output = captured_output.getvalue()
            self.assertIn("row1", output)
            self.assertIn("row2", output)
        finally:
            # Restore stdout
            sys.stdout = sys.__stdout__
    
    def test_handle_execute_query_update(self):
        """Test handling the execute-query command with an UPDATE query."""
        # Set up the mock
        self.connection_manager.execute_query.return_value = (True, 5, None)  # 5 rows affected
        
        # Capture stdout to verify output
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            # Call the method
            args = argparse.Namespace(
                id="test_connection",
                query="UPDATE test_table SET column = value",
                output="text"
            )
            exit_code = self.cli_manager._handle_execute_query(args)
            
            # Verify the result
            self.assertEqual(exit_code, 0)
            
            # Verify output
            self.assertIn("Rows affected: 5", captured_output.getvalue())
        finally:
            # Restore stdout
            sys.stdout = sys.__stdout__
    
    def test_handle_execute_query_json(self):
        """Test handling the execute-query command with JSON output."""
        # Set up the mock
        self.connection_manager.execute_query.return_value = (True, [("row1",), ("row2",)], None)
        
        # Capture stdout to verify output
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            # Call the method
            args = argparse.Namespace(
                id="test_connection",
                query="SELECT * FROM test_table",
                output="json"
            )
            exit_code = self.cli_manager._handle_execute_query(args)
            
            # Verify the result
            self.assertEqual(exit_code, 0)
            
            # Verify output
            import json
            output = json.loads(captured_output.getvalue())
            
            self.assertIn("result", output)
            self.assertEqual(len(output["result"]), 2)
            self.assertEqual(output["result"][0][0], "row1")
            self.assertEqual(output["result"][1][0], "row2")
        finally:
            # Restore stdout
            sys.stdout = sys.__stdout__
    
    def test_handle_execute_query_failure(self):
        """Test handling a failed execute-query command."""
        # Set up the mock
        self.connection_manager.execute_query.return_value = (False, None, "Query error")
        
        # Call the method
        args = argparse.Namespace(
            id="test_connection",
            query="SELECT * FROM test_table",
            output="text"
        )
        exit_code = self.cli_manager._handle_execute_query(args)
        
        # Verify the result
        self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()