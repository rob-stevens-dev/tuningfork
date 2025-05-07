"""
Tuning Fork - Database Performance Optimization Tool
Unit tests for ConnectionManager
"""

import unittest
from unittest.mock import patch, MagicMock

from tuningfork.core.config_manager import ConfigManager
from tuningfork.core.connection_manager import ConnectionManager

class TestConnectionManager(unittest.TestCase):
    """Test cases for the ConnectionManager class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock ConfigManager
        self.config_manager = MagicMock(spec=ConfigManager)
        
        # Initialize ConnectionManager with the mock ConfigManager
        self.connection_manager = ConnectionManager(self.config_manager)
    
    def tearDown(self):
        """Clean up test fixtures after each test method."""
        # Disconnect all connections
        self.connection_manager.disconnect_all()
    
    @patch('psycopg2.connect')
    def test_connect_postgres(self, mock_connect):
        """Test connecting to a PostgreSQL database."""
        # Set up the mock
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Call the method
        success, error = self.connection_manager.connect(
            connection_id="test_postgres",
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Verify the result
        self.assertTrue(success)
        self.assertIsNone(error)
        
        # Verify the connection was stored
        self.assertIn("test_postgres", self.connection_manager.connections)
        self.assertEqual(
            self.connection_manager.connections["test_postgres"],
            mock_connection
        )
        
        # Verify the connection details were stored
        connection_details = self.connection_manager.connection_details["test_postgres"]
        self.assertEqual(connection_details["db_type"], "postgres")
        self.assertEqual(connection_details["host"], "localhost")
        self.assertEqual(connection_details["port"], 5432)
        self.assertEqual(connection_details["username"], "test_user")
        self.assertEqual(connection_details["database"], "test_db")
        
        # Verify psycopg2.connect was called with the correct arguments
        mock_connect.assert_called_once_with(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Verify ConfigManager.add_connection_config was called with the correct arguments
        self.config_manager.add_connection_config.assert_called_once()
        call_args = self.config_manager.add_connection_config.call_args[0]
        self.assertEqual(call_args[0], "test_postgres")
        self.assertEqual(call_args[1]["db_type"], "postgres")
        self.assertEqual(call_args[1]["host"], "localhost")
        self.assertEqual(call_args[1]["port"], 5432)
        self.assertEqual(call_args[1]["username"], "test_user")
        self.assertEqual(call_args[1]["database"], "test_db")
    
    @patch('mysql.connector.connect')
    def test_connect_mysql(self, mock_connect):
        """Test connecting to a MySQL database."""
        # Set up the mock
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Call the method
        success, error = self.connection_manager.connect(
            connection_id="test_mysql",
            db_type="mysql",
            host="localhost",
            port=3306,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Verify the result
        self.assertTrue(success)
        self.assertIsNone(error)
        
        # Verify the connection was stored
        self.assertIn("test_mysql", self.connection_manager.connections)
        self.assertEqual(
            self.connection_manager.connections["test_mysql"],
            mock_connection
        )
        
        # Verify mysql.connector.connect was called with the correct arguments
        mock_connect.assert_called_once_with(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db"
        )
    
    @patch('pyodbc.connect')
    def test_connect_mssql(self, mock_connect):
        """Test connecting to a Microsoft SQL Server database."""
        # Set up the mock
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Call the method
        success, error = self.connection_manager.connect(
            connection_id="test_mssql",
            db_type="mssql",
            host="localhost",
            port=1433,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Verify the result
        self.assertTrue(success)
        self.assertIsNone(error)
        
        # Verify the connection was stored
        self.assertIn("test_mssql", self.connection_manager.connections)
        self.assertEqual(
            self.connection_manager.connections["test_mssql"],
            mock_connection
        )
        
        # Verify pyodbc.connect was called with the correct arguments
        expected_conn_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=test_db;"
            "UID=test_user;"
            "PWD=test_password"
        )
        mock_connect.assert_called_once_with(expected_conn_string)
    
    @patch('sqlite3.connect')
    def test_connect_sqlite(self, mock_connect):
        """Test connecting to a SQLite database."""
        # Set up the mock
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Call the method
        success, error = self.connection_manager.connect(
            connection_id="test_sqlite",
            db_type="sqlite",
            host="",
            port=0,
            username="",
            password="",
            database="test.db"
        )
        
        # Verify the result
        self.assertTrue(success)
        self.assertIsNone(error)
        
        # Verify the connection was stored
        self.assertIn("test_sqlite", self.connection_manager.connections)
        self.assertEqual(
            self.connection_manager.connections["test_sqlite"],
            mock_connection
        )
        
        # Verify sqlite3.connect was called with the correct arguments
        mock_connect.assert_called_once_with("test.db")
    
    def test_connect_unsupported_db_type(self):
        """Test connecting to an unsupported database type."""
        # Call the method
        success, error = self.connection_manager.connect(
            connection_id="test_unsupported",
            db_type="unsupported",
            host="localhost",
            port=1234,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Verify the result
        self.assertFalse(success)
        self.assertEqual(error, "Unsupported database type: unsupported")
        
        # Verify the connection was not stored
        self.assertNotIn("test_unsupported", self.connection_manager.connections)
    
    @patch('psycopg2.connect')
    def test_connect_already_exists(self, mock_connect):
        """Test connecting with an existing connection ID."""
        # Set up the mock
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Create an initial connection
        self.connection_manager.connect(
            connection_id="test_postgres",
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Reset the mock
        mock_connect.reset_mock()
        
        # Try to connect with the same ID
        success, error = self.connection_manager.connect(
            connection_id="test_postgres",
            db_type="postgres",
            host="other_host",
            port=5432,
            username="other_user",
            password="other_password",
            database="other_db"
        )
        
        # Verify the result
        self.assertFalse(success)
        self.assertEqual(error, "Connection already exists")
        
        # Verify psycopg2.connect was not called
        mock_connect.assert_not_called()
    
    @patch('psycopg2.connect', side_effect=Exception("Connection error"))
    def test_connect_error(self, mock_connect):
        """Test error handling when connecting."""
        # Call the method
        success, error = self.connection_manager.connect(
            connection_id="test_error",
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Verify the result
        self.assertFalse(success)
        self.assertEqual(error, "Connection error")
        
        # Verify the connection was not stored
        self.assertNotIn("test_error", self.connection_manager.connections)
    
    def test_disconnect(self):
        """Test disconnecting from a database."""
        # Create a mock connection
        mock_connection = MagicMock()
        
        # Add the connection to the connection manager
        self.connection_manager.connections["test_connection"] = mock_connection
        self.connection_manager.connection_details["test_connection"] = {
            "db_type": "postgres",
            "host": "localhost",
            "port": 5432,
            "username": "test_user",
            "database": "test_db"
        }
        
        # Call the method
        success, error = self.connection_manager.disconnect("test_connection")
        
        # Verify the result
        self.assertTrue(success)
        self.assertIsNone(error)
        
        # Verify the connection was removed
        self.assertNotIn("test_connection", self.connection_manager.connections)
        self.assertNotIn("test_connection", self.connection_manager.connection_details)
        
        # Verify the connection was closed
        mock_connection.close.assert_called_once()
    
    def test_disconnect_non_existent(self):
        """Test disconnecting from a non-existent connection."""
        # Call the method
        success, error = self.connection_manager.disconnect("non_existent_connection")
        
        # Verify the result
        self.assertFalse(success)
        self.assertEqual(error, "Connection does not exist")
    
    def test_disconnect_error(self):
        """Test error handling when disconnecting."""
        # Create a mock connection that raises an exception when closed
        mock_connection = MagicMock()
        mock_connection.close.side_effect = Exception("Disconnect error")
        
        # Add the connection to the connection manager
        self.connection_manager.connections["test_connection"] = mock_connection
        self.connection_manager.connection_details["test_connection"] = {
            "db_type": "postgres",
            "host": "localhost",
            "port": 5432,
            "username": "test_user",
            "database": "test_db"
        }
        
        # Call the method
        success, error = self.connection_manager.disconnect("test_connection")
        
        # Verify the result
        self.assertFalse(success)
        self.assertEqual(error, "Disconnect error")
    
    def test_disconnect_all(self):
        """Test disconnecting all connections."""
        # Create mock connections
        mock_connection1 = MagicMock()
        mock_connection2 = MagicMock()
        
        # Add the connections to the connection manager
        self.connection_manager.connections["connection1"] = mock_connection1
        self.connection_manager.connection_details["connection1"] = {
            "db_type": "postgres",
            "host": "localhost",
            "port": 5432,
            "username": "user1",
            "database": "db1"
        }
        
        self.connection_manager.connections["connection2"] = mock_connection2
        self.connection_manager.connection_details["connection2"] = {
            "db_type": "mysql",
            "host": "localhost",
            "port": 3306,
            "username": "user2",
            "database": "db2"
        }
        
        # Call the method
        self.connection_manager.disconnect_all()
        
        # Verify all connections were removed
        self.assertEqual(len(self.connection_manager.connections), 0)
        self.assertEqual(len(self.connection_manager.connection_details), 0)
        
        # Verify all connections were closed
        mock_connection1.close.assert_called_once()
        mock_connection2.close.assert_called_once()
    
    def test_execute_query(self):
        """Test executing a query."""
        # Create a mock connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("row1",), ("row2",)]
        
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Add the connection to the connection manager
        self.connection_manager.connections["test_connection"] = mock_connection
        self.connection_manager.connection_details["test_connection"] = {
            "db_type": "postgres",
            "host": "localhost",
            "port": 5432,
            "username": "test_user",
            "database": "test_db"
        }
        
        # Call the method with a SELECT query
        success, result, error = self.connection_manager.execute_query(
            connection_id="test_connection",
            query="SELECT * FROM test_table"
        )
        
        # Verify the result
        self.assertTrue(success)
        self.assertEqual(result, [("row1",), ("row2",)])
        self.assertIsNone(error)
        
        # Verify the query was executed
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")
        mock_cursor.fetchall.assert_called_once()
        mock_cursor.close.assert_called_once()
        
        # Test with a non-SELECT query
        mock_cursor.reset_mock()
        mock_cursor.rowcount = 5
        
        success, result, error = self.connection_manager.execute_query(
            connection_id="test_connection",
            query="UPDATE test_table SET column = value"
        )
        
        # Verify the result
        self.assertTrue(success)
        self.assertEqual(result, 5)  # rowcount
        self.assertIsNone(error)
        
        # Verify the query was executed and committed
        mock_cursor.execute.assert_called_once_with("UPDATE test_table SET column = value")
        mock_cursor.fetchall.assert_not_called()
        mock_connection.commit.assert_called_once()
    
    def test_execute_query_with_params(self):
        """Test executing a query with parameters."""
        # Create a mock connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("row1",)]
        
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Add the connection to the connection manager
        self.connection_manager.connections["test_connection"] = mock_connection
        self.connection_manager.connection_details["test_connection"] = {
            "db_type": "postgres",
            "host": "localhost",
            "port": 5432,
            "username": "test_user",
            "database": "test_db"
        }
        
        # Call the method with parameters
        success, result, error = self.connection_manager.execute_query(
            connection_id="test_connection",
            query="SELECT * FROM test_table WHERE id = %s",
            params=(1,)
        )
        
        # Verify the result
        self.assertTrue(success)
        self.assertEqual(result, [("row1",)])
        self.assertIsNone(error)
        
        # Verify the query was executed with parameters
        mock_cursor.execute.assert_called_once_with(
            "SELECT * FROM test_table WHERE id = %s",
            (1,)
        )
    
    def test_execute_query_non_existent_connection(self):
        """Test executing a query on a non-existent connection."""
        # Call the method
        success, result, error = self.connection_manager.execute_query(
            connection_id="non_existent_connection",
            query="SELECT * FROM test_table"
        )
        
        # Verify the result
        self.assertFalse(success)
        self.assertIsNone(result)
        self.assertEqual(error, "Connection does not exist")
    
    def test_execute_query_error(self):
        """Test error handling when executing a query."""
        # Create a mock connection and cursor that raises an exception
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Query error")
        
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Add the connection to the connection manager
        self.connection_manager.connections["test_connection"] = mock_connection
        self.connection_manager.connection_details["test_connection"] = {
            "db_type": "postgres",
            "host": "localhost",
            "port": 5432,
            "username": "test_user",
            "database": "test_db"
        }
        
        # Call the method
        success, result, error = self.connection_manager.execute_query(
            connection_id="test_connection",
            query="SELECT * FROM test_table"
        )
        
        # Verify the result
        self.assertFalse(success)
        self.assertIsNone(result)
        self.assertEqual(error, "Query error")
    
    def test_get_connection_info(self):
        """Test getting connection information."""
        # Add a connection to the connection manager
        self.connection_manager.connection_details["test_connection"] = {
            "db_type": "postgres",
            "host": "localhost",
            "port": 5432,
            "username": "test_user",
            "database": "test_db"
        }
        
        # Call the method
        info = self.connection_manager.get_connection_info("test_connection")
        
        # Verify the result
        self.assertEqual(info["db_type"], "postgres")
        self.assertEqual(info["host"], "localhost")
        self.assertEqual(info["port"], 5432)
        self.assertEqual(info["username"], "test_user")
        self.assertEqual(info["database"], "test_db")
    
    def test_get_connection_info_non_existent(self):
        """Test getting information for a non-existent connection."""
        # Call the method
        with self.assertRaises(KeyError):
            self.connection_manager.get_connection_info("non_existent_connection")
    
    def test_list_connections(self):
        """Test listing connections."""
        # Add connections to the connection manager
        self.connection_manager.connections["connection1"] = MagicMock()
        self.connection_manager.connections["connection2"] = MagicMock()
        
        # Call the method
        connections = self.connection_manager.list_connections()
        
        # Verify the result
        self.assertIn("connection1", connections)
        self.assertIn("connection2", connections)
        self.assertEqual(len(connections), 2)
    
    def test_is_connected(self):
        """Test checking if a connection is active."""
        # Create a mock connection and cursor
        mock_cursor = MagicMock()
        
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Add the connection to the connection manager
        self.connection_manager.connections["test_postgres"] = mock_connection
        self.connection_manager.connection_details["test_postgres"] = {
            "db_type": "postgres",
            "host": "localhost",
            "port": 5432,
            "username": "test_user",
            "database": "test_db"
        }
        
        # Call the method
        is_connected = self.connection_manager.is_connected("test_postgres")
        
        # Verify the result
        self.assertTrue(is_connected)
        
        # Verify the test query was executed
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_cursor.fetchone.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    def test_is_connected_non_existent(self):
        """Test checking if a non-existent connection is active."""
        # Call the method
        is_connected = self.connection_manager.is_connected("non_existent_connection")
        
        # Verify the result
        self.assertFalse(is_connected)
    
    def test_is_connected_broken(self):
        """Test checking if a broken connection is active."""
        # Create a mock connection and cursor that raises an exception
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Connection broken")
        
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Add the connection to the connection manager
        self.connection_manager.connections["test_broken"] = mock_connection
        self.connection_manager.connection_details["test_broken"] = {
            "db_type": "postgres",
            "host": "localhost",
            "port": 5432,
            "username": "test_user",
            "database": "test_db"
        }
        
        # Call the method
        is_connected = self.connection_manager.is_connected("test_broken")
        
        # Verify the result
        self.assertFalse(is_connected)
        
        # Verify the connection was removed
        self.assertNotIn("test_broken", self.connection_manager.connections)


if __name__ == "__main__":
    unittest.main()