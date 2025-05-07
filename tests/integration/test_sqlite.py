"""
Tuning Fork - Database Performance Optimization Tool
Integration tests for SQLite
"""

import os
import json
import tempfile
import unittest
import sqlite3
import shutil
import sys

from tuningfork.core.config_manager import ConfigManager
from tuningfork.core.connection_manager import ConnectionManager
from tuningfork.core.cli_manager import CLIManager

class TestSQLiteIntegration(unittest.TestCase):
    """Integration tests for SQLite database."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a temporary directory for test files
        self.test_dir = tempfile.mkdtemp()
        
        # Create a temporary configuration file
        self.config_file = os.path.join(self.test_dir, "config.json")
        with open(self.config_file, "w") as f:
            json.dump({
                "connections": {},
                "default_timeout": 30,
                "backup_directory": os.path.join(self.test_dir, "backups"),
                "report_directory": os.path.join(self.test_dir, "reports"),
                "log_level": "INFO"
            }, f)
        
        # Create a temporary SQLite database
        self.db_file = os.path.join(self.test_dir, "test.db")
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        # Create a test table
        cursor.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        """)
        
        # Insert some test data
        cursor.execute("INSERT INTO test_table VALUES (1, 'Item 1', 100)")
        cursor.execute("INSERT INTO test_table VALUES (2, 'Item 2', 200)")
        cursor.execute("INSERT INTO test_table VALUES (3, 'Item 3', 300)")
        
        conn.commit()
        conn.close()
    
    def tearDown(self):
        """Clean up test fixtures after each test method."""
        # Remove the temporary directory and its contents
        shutil.rmtree(self.test_dir)
    
    def test_end_to_end_sqlite(self):
        """Test the end-to-end workflow with SQLite."""
        # Create instances of the managers
        config_manager = ConfigManager(self.config_file)
        connection_manager = ConnectionManager(config_manager)
        
        try:
            # Connect to the SQLite database
            success, error = connection_manager.connect(
                connection_id="test_sqlite",
                db_type="sqlite",
                host="",
                port=0,
                username="",
                password="",
                database=self.db_file
            )
            
            # Verify the connection was established
            self.assertTrue(success, f"Failed to connect: {error}")
            self.assertIn("test_sqlite", connection_manager.list_connections())
            
            # Verify connection details
            info = connection_manager.get_connection_info("test_sqlite")
            self.assertEqual(info["db_type"], "sqlite")
            self.assertEqual(info["database"], self.db_file)
            
            # Execute a SELECT query
            success, result, error = connection_manager.execute_query(
                connection_id="test_sqlite",
                query="SELECT * FROM test_table WHERE id = ?",
                params=(2,)
            )
            
            # Verify the query result
            self.assertTrue(success, f"Query failed: {error}")
            self.assertIsNone(error)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0][0], 2)
            self.assertEqual(result[0][1], "Item 2")
            self.assertEqual(result[0][2], 200)
            
            # Execute an INSERT query
            success, result, error = connection_manager.execute_query(
                connection_id="test_sqlite",
                query="INSERT INTO test_table VALUES (?, ?, ?)",
                params=(4, "Item 4", 400)
            )
            
            # Verify the query result
            self.assertTrue(success, f"Query failed: {error}")
            self.assertIsNone(error)
            self.assertEqual(result, 1)  # 1 row affected
            
            # Verify the data was inserted
            success, result, error = connection_manager.execute_query(
                connection_id="test_sqlite",
                query="SELECT * FROM test_table WHERE id = 4"
            )
            
            self.assertTrue(success, f"Query failed: {error}")
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0][0], 4)
            self.assertEqual(result[0][1], "Item 4")
            self.assertEqual(result[0][2], 400)
            
            # Execute an UPDATE query
            success, result, error = connection_manager.execute_query(
                connection_id="test_sqlite",
                query="UPDATE test_table SET value = ? WHERE id = ?",
                params=(250, 2)
            )
            
            # Verify the query result
            self.assertTrue(success, f"Query failed: {error}")
            self.assertIsNone(error)
            self.assertEqual(result, 1)  # 1 row affected
            
            # Verify the data was updated
            success, result, error = connection_manager.execute_query(
                connection_id="test_sqlite",
                query="SELECT value FROM test_table WHERE id = 2"
            )
            
            self.assertTrue(success, f"Query failed: {error}")
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0][0], 250)
            
            # Execute a DELETE query
            success, result, error = connection_manager.execute_query(
                connection_id="test_sqlite",
                query="DELETE FROM test_table WHERE id = ?",
                params=(3,)
            )
            
            # Verify the query result
            self.assertTrue(success, f"Query failed: {error}")
            self.assertIsNone(error)
            self.assertEqual(result, 1)  # 1 row affected
            
            # Verify the data was deleted
            success, result, error = connection_manager.execute_query(
                connection_id="test_sqlite",
                query="SELECT * FROM test_table WHERE id = 3"
            )
            
            self.assertTrue(success, f"Query failed: {error}")
            self.assertEqual(len(result), 0)  # No rows found
            
            # Verify the remaining data
            success, result, error = connection_manager.execute_query(
                connection_id="test_sqlite",
                query="SELECT COUNT(*) FROM test_table"
            )
            
            self.assertTrue(success, f"Query failed: {error}")
            self.assertEqual(result[0][0], 3)  # 3 rows remaining
            
            # Test connection status
            is_connected = connection_manager.is_connected("test_sqlite")
            self.assertTrue(is_connected)
            
            # Disconnect
            success, error = connection_manager.disconnect("test_sqlite")
            self.assertTrue(success, f"Failed to disconnect: {error}")
            self.assertIsNone(error)
            
            # Verify the connection was closed
            self.assertNotIn("test_sqlite", connection_manager.list_connections())
            
            # Test connection status after disconnecting
            is_connected = connection_manager.is_connected("test_sqlite")
            self.assertFalse(is_connected)
        finally:
            # Clean up
            connection_manager.disconnect_all()
    
    def test_cli_integration(self):
        """Test the CLI integration with SQLite."""
        # Create instances of the managers
        config_manager = ConfigManager(self.config_file)
        connection_manager = ConnectionManager(config_manager)
        cli_manager = CLIManager(config_manager, connection_manager)
        
        try:
            # Connect to the SQLite database using CLI
            result = cli_manager.run([
                "--config", self.config_file,
                "connect",
                "--id", "test_sqlite",
                "--type", "sqlite",
                "--database", self.db_file
            ])
            
            # Verify the connection was established
            self.assertEqual(result, 0)
            self.assertIn("test_sqlite", connection_manager.list_connections())
            
            # Execute a query using CLI (capture stdout)
            import io
            captured_output = io.StringIO()
            sys.stdout = captured_output
            
            try:
                result = cli_manager.run([
                    "--config", self.config_file,
                    "execute-query",
                    "--id", "test_sqlite",
                    "--query", "SELECT * FROM test_table WHERE id = 1"
                ])
                
                # Verify the query executed successfully
                self.assertEqual(result, 0)
                
                # Verify the output contains the expected data
                output = captured_output.getvalue()
                self.assertIn("Item 1", output)
                self.assertIn("100", output)
            finally:
                # Restore stdout
                sys.stdout = sys.__stdout__
            
            # Disconnect using CLI
            result = cli_manager.run([
                "--config", self.config_file,
                "disconnect",
                "--id", "test_sqlite"
            ])
            
            # Verify the connection was closed
            self.assertEqual(result, 0)
            self.assertNotIn("test_sqlite", connection_manager.list_connections())
        finally:
            # Clean up
            connection_manager.disconnect_all()


if __name__ == "__main__":
    unittest.main()