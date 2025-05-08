"""
Unit tests for the Connection Manager.
"""

import unittest
from unittest import mock

from tuningfork.connection.connection_manager import ConnectionManager
from tuningfork.connection.ssh_manager import SSHManager
from tuningfork.connection.cloud_connection_factory import CloudConnectionFactory


class TestConnectionManager(unittest.TestCase):
    """Test suite for the Connection Manager."""

    def setUp(self):
        """Set up test environment."""
        self.ssh_manager = mock.Mock(spec=SSHManager)
        self.cloud_factory = mock.Mock(spec=CloudConnectionFactory)
        self.connection_manager = ConnectionManager(
            ssh_manager=self.ssh_manager,
            cloud_connection_factory=self.cloud_factory
        )
        
    def tearDown(self):
        """Clean up after tests."""
        self.connection_manager.close_all_connections()

    @mock.patch('psycopg2.connect')
    def test_connect_postgres(self, mock_psycopg2_connect):
        """Test connecting to a PostgreSQL database."""
        # Set up the mock
        mock_connection = mock.Mock()
        mock_psycopg2_connect.return_value = mock_connection
        
        # Call the method under test
        connection_id = self.connection_manager.connect(
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Verify the connection was created
        self.assertIsNotNone(connection_id)
        self.assertIn(connection_id, self.connection_manager._connections)
        
        # Verify the connection details
        connection_details = self.connection_manager._connections[connection_id]
        self.assertEqual(connection_details["db_type"], "postgres")
        self.assertEqual(connection_details["params"]["host"], "localhost")
        self.assertEqual(connection_details["params"]["port"], 5432)
        self.assertEqual(connection_details["params"]["username"], "test_user")
        self.assertEqual(connection_details["params"]["password"], "test_password")
        self.assertEqual(connection_details["params"]["database"], "test_db")
        
        # Verify psycopg2.connect was called with the correct parameters
        mock_psycopg2_connect.assert_called_once()
        
        # Verify transaction is not active by default
        self.assertFalse(connection_details["transaction_active"])
        
        # Close the connection
        self.connection_manager.disconnect(connection_id)
        
        # Verify the connection was closed
        self.assertNotIn(connection_id, self.connection_manager._connections)
        mock_connection.close.assert_called_once()

    @mock.patch('mysql.connector.connect')
    def test_connect_mysql(self, mock_mysql_connect):
        """Test connecting to a MySQL database."""
        # Set up the mock
        mock_connection = mock.Mock()
        mock_mysql_connect.return_value = mock_connection
        
        # Call the method under test
        connection_id = self.connection_manager.connect(
            db_type="mysql",
            host="localhost",
            port=3306,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Verify the connection was created
        self.assertIsNotNone(connection_id)
        self.assertIn(connection_id, self.connection_manager._connections)
        
        # Verify the connection details
        connection_details = self.connection_manager._connections[connection_id]
        self.assertEqual(connection_details["db_type"], "mysql")
        
        # Close the connection
        self.connection_manager.disconnect(connection_id)
        
        # Verify the connection was closed
        self.assertNotIn(connection_id, self.connection_manager._connections)
        mock_connection.close.assert_called_once()

    @mock.patch('pyodbc.connect')
    def test_connect_sqlserver(self, mock_pyodbc_connect):
        """Test connecting to a SQL Server database."""
        # Set up the mock
        mock_connection = mock.Mock()
        mock_pyodbc_connect.return_value = mock_connection
        
        # Call the method under test
        connection_id = self.connection_manager.connect(
            db_type="sqlserver",
            host="localhost",
            port=1433,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Verify the connection was created
        self.assertIsNotNone(connection_id)
        self.assertIn(connection_id, self.connection_manager._connections)
        
        # Verify the connection details
        connection_details = self.connection_manager._connections[connection_id]
        self.assertEqual(connection_details["db_type"], "sqlserver")
        
        # Close the connection
        self.connection_manager.disconnect(connection_id)
        
        # Verify the connection was closed
        self.assertNotIn(connection_id, self.connection_manager._connections)
        mock_connection.close.assert_called_once()

    @mock.patch('sqlite3.connect')
    def test_connect_sqlite(self, mock_sqlite_connect):
        """Test connecting to a SQLite database."""
        # Set up the mock
        mock_connection = mock.Mock()
        mock_sqlite_connect.return_value = mock_connection
        
        # Call the method under test
        connection_id = self.connection_manager.connect(
            db_type="sqlite",
            host="",  # Not used for SQLite
            port=0,   # Not used for SQLite
            username="",  # Not used for SQLite
            password="",  # Not used for SQLite
            database="/path/to/test.db"
        )
        
        # Verify the connection was created
        self.assertIsNotNone(connection_id)
        self.assertIn(connection_id, self.connection_manager._connections)
        
        # Verify the connection details
        connection_details = self.connection_manager._connections[connection_id]
        self.assertEqual(connection_details["db_type"], "sqlite")
        self.assertEqual(connection_details["params"]["database"], "/path/to/test.db")
        
        # Close the connection
        self.connection_manager.disconnect(connection_id)
        
        # Verify the connection was closed
        self.assertNotIn(connection_id, self.connection_manager._connections)
        mock_connection.close.assert_called_once()

    @mock.patch('psycopg2.connect')
    def test_connect_with_ssh_tunnel(self, mock_psycopg2_connect):
        """Test connecting to a database through an SSH tunnel."""
        # Set up the mocks
        mock_connection = mock.Mock()
        mock_psycopg2_connect.return_value = mock_connection
        
        # Set up the SSH tunnel mock
        self.ssh_manager.create_tunnel.return_value = ("127.0.0.1", 10000)
        
        # Call the method under test
        connection_id = self.connection_manager.connect(
            db_type="postgres",
            host="remote.db.example.com",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db",
            use_ssh=True,
            ssh_config={
                "ssh_host": "ssh.example.com",
                "ssh_port": 22,
                "ssh_username": "ssh_user",
                "ssh_password": "ssh_password"
            }
        )
        
        # Verify the SSH tunnel was created
        self.ssh_manager.create_tunnel.assert_called_once()
        
        # Verify the connection was created using the tunneled parameters
        mock_psycopg2_connect.assert_called_once()
        call_args = mock_psycopg2_connect.call_args[1]
        self.assertEqual(call_args["host"], "127.0.0.1")
        self.assertEqual(call_args["port"], 10000)
        
        # Verify the connection details
        connection_details = self.connection_manager._connections[connection_id]
        self.assertEqual(connection_details["params"]["host"], "127.0.0.1")
        self.assertEqual(connection_details["params"]["port"], 10000)
        self.assertIn("ssh_tunnel_id", connection_details["params"])
        
        # Close the connection
        self.connection_manager.disconnect(connection_id)
        
        # Verify the connection and tunnel were closed
        self.assertNotIn(connection_id, self.connection_manager._connections)
        mock_connection.close.assert_called_once()
        self.ssh_manager.close_tunnel.assert_called_once()

    @mock.patch('psycopg2.connect')
    def test_connect_with_cloud(self, mock_psycopg2_connect):
        """Test connecting to a cloud database."""
        # Set up the mocks
        mock_connection = mock.Mock()
        mock_psycopg2_connect.return_value = mock_connection
        
        # Set up the cloud connection mock
        cloud_connection = {
            "connection": {
                "endpoint": "test-db.amazonaws.com",
                "port": 5432,
                "database": "test_db",
                "user": "test_user",
                "password": "test_password",
                "ssl": True
            },
            "cloud_provider": "aws",
            "db_type": "postgres",
        }
        self.cloud_factory.create_connection.return_value = cloud_connection
        
        # Call the method under test
        connection_id = self.connection_manager.connect(
            db_type="postgres",
            host="test-db.amazonaws.com",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db",
            use_cloud=True,
            cloud_config={
                "provider": "aws",
                "credentials": {
                    "region": "us-west-2",
                    "aws_access_key_id": "test_key",
                    "aws_secret_access_key": "test_secret"
                }
            }
        )
        
        # Verify the cloud connection was created
        self.cloud_factory.create_connection.assert_called_once()
        
        # Verify the connection details
        connection_details = self.connection_manager._connections[connection_id]
        self.assertIn("cloud_connection_id", connection_details["params"])
        
        # Close the connection
        self.connection_manager.disconnect(connection_id)
        
        # Verify the connection and cloud connection were closed
        self.assertNotIn(connection_id, self.connection_manager._connections)
        mock_connection.close.assert_called_once()
        self.cloud_factory.close_connection.assert_called_once()

    @mock.patch('psycopg2.pool.ThreadedConnectionPool')
    def test_create_connection_pool(self, mock_pool):
        """Test creating a connection pool."""
        # Set up the mock
        mock_pool_instance = mock.Mock()
        mock_pool.return_value = mock_pool_instance
        
        # Call the method under test
        pool_id = self.connection_manager.create_connection_pool(
            pool_id="test_pool",
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db",
            min_connections=2,
            max_connections=10
        )
        
        # Verify the pool was created
        self.assertEqual(pool_id, "test_pool")
        self.assertIn(pool_id, self.connection_manager._connection_pools)
        
        # Verify the pool details
        pool_details = self.connection_manager._connection_pools[pool_id]
        self.assertEqual(pool_details["db_type"], "postgres")
        self.assertEqual(pool_details["host"], "localhost")
        self.assertEqual(pool_details["port"], 5432)
        self.assertEqual(pool_details["database"], "test_db")
        self.assertEqual(pool_details["min_connections"], 2)
        self.assertEqual(pool_details["max_connections"], 10)
        
        # Verify the pool parameters
        mock_pool.assert_called_once()
        call_args = mock_pool.call_args[1]
        self.assertEqual(call_args["minconn"], 2)
        self.assertEqual(call_args["maxconn"], 10)
        self.assertEqual(call_args["host"], "localhost")
        self.assertEqual(call_args["port"], 5432)
        self.assertEqual(call_args["dbname"], "test_db")
        
        # Close the pool
        self.connection_manager.close_connection_pool(pool_id)
        
        # Verify the pool was closed
        self.assertNotIn(pool_id, self.connection_manager._connection_pools)
        mock_pool_instance.closeall.assert_called_once()

    @mock.patch('psycopg2.pool.ThreadedConnectionPool')
    def test_get_connection_from_pool(self, mock_pool):
        """Test getting a connection from a pool."""
        # Set up the mock
        mock_pool_instance = mock.Mock()
        mock_pool.return_value = mock_pool_instance
        
        # Set up the getconn mock
        mock_connection = mock.Mock()
        mock_pool_instance.getconn.return_value = mock_connection
        
        # Create a pool
        pool_id = self.connection_manager.create_connection_pool(
            pool_id="test_pool",
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db",
            min_connections=2,
            max_connections=10
        )
        
        # Call the method under test
        connection_id, connection = self.connection_manager.get_connection_from_pool(pool_id)
        
        # Verify the connection was retrieved
        self.assertIsNotNone(connection_id)
        self.assertEqual(connection, mock_connection)
        self.assertTrue(connection_id.startswith(pool_id))
        
        # Verify the connection is tracked
        self.assertIn(connection_id, self.connection_manager._connections)
        
        # Verify the connection details
        connection_details = self.connection_manager._connections[connection_id]
        self.assertEqual(connection_details["db_type"], "postgres")
        self.assertEqual(connection_details["pool_id"], pool_id)
        self.assertTrue(connection_details["from_pool"])
        self.assertFalse(connection_details["transaction_active"])
        
        # Return the connection to the pool
        self.connection_manager.return_connection_to_pool(connection_id)
        
        # Verify the connection was returned
        self.assertNotIn(connection_id, self.connection_manager._connections)
        mock_pool_instance.putconn.assert_called_once_with(mock_connection)
        
        # Close the pool
        self.connection_manager.close_connection_pool(pool_id)

    @mock.patch('psycopg2.connect')
    def test_execute_query(self, mock_psycopg2_connect):
        """Test executing a query."""
        # Set up the mocks
        mock_connection = mock.Mock()
        mock_psycopg2_connect.return_value = mock_connection
        
        mock_cursor = mock.Mock()
        mock_connection.cursor.return_value = mock_cursor
        
        mock_cursor.fetchall.return_value = [("row1",), ("row2",)]
        
        # Create a connection
        connection_id = self.connection_manager.connect(
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Call the method under test
        results = self.connection_manager.execute_query(
            connection_id=connection_id,
            query="SELECT * FROM test_table",
            fetchall=True
        )
        
        # Verify the query was executed
        mock_connection.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table", ())
        mock_cursor.fetchall.assert_called_once()
        
        # Verify the results
        self.assertEqual(results, [("row1",), ("row2",)])
        
        # Close the connection
        self.connection_manager.disconnect(connection_id)

    @mock.patch('psycopg2.connect')
    def test_transaction_management(self, mock_psycopg2_connect):
        """Test transaction management."""
        # Set up the mocks
        mock_connection = mock.Mock()
        mock_psycopg2_connect.return_value = mock_connection
        
        # Create a connection
        connection_id = self.connection_manager.connect(
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Begin a transaction
        self.connection_manager.begin_transaction(connection_id)
        
        # Verify the transaction was started
        self.assertTrue(self.connection_manager._connections[connection_id]["transaction_active"])
        mock_connection.begin.assert_called_once()
        
        # Commit the transaction
        self.connection_manager.commit(connection_id)
        
        # Verify the transaction was committed
        self.assertFalse(self.connection_manager._connections[connection_id]["transaction_active"])
        mock_connection.commit.assert_called_once()
        
        # Begin another transaction
        self.connection_manager.begin_transaction(connection_id)
        
        # Rollback the transaction
        self.connection_manager.rollback(connection_id)
        
        # Verify the transaction was rolled back
        self.assertFalse(self.connection_manager._connections[connection_id]["transaction_active"])
        mock_connection.rollback.assert_called_once()
        
        # Close the connection
        self.connection_manager.disconnect(connection_id)

    @mock.patch('psycopg2.connect')
    def test_list_connections(self, mock_psycopg2_connect):
        """Test listing connections."""
        # Set up the mocks
        mock_connection = mock.Mock()
        mock_psycopg2_connect.return_value = mock_connection
        
        # Create some connections
        connection_id1 = self.connection_manager.connect(
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db1"
        )
        
        connection_id2 = self.connection_manager.connect(
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db2"
        )
        
        # Call the method under test
        connections = self.connection_manager.list_connections()
        
        # Verify the connections are listed
        self.assertEqual(len(connections), 2)
        
        # Verify the connection details
        conn1 = next((c for c in connections if c["connection_id"] == connection_id1), None)
        self.assertIsNotNone(conn1)
        self.assertEqual(conn1["db_type"], "postgres")
        
        conn2 = next((c for c in connections if c["connection_id"] == connection_id2), None)
        self.assertIsNotNone(conn2)
        self.assertEqual(conn2["db_type"], "postgres")
        
        # Close the connections
        self.connection_manager.disconnect(connection_id1)
        self.connection_manager.disconnect(connection_id2)

    @mock.patch('psycopg2.pool.ThreadedConnectionPool')
    def test_list_connection_pools(self, mock_pool):
        """Test listing connection pools."""
        # Set up the mock
        mock_pool_instance = mock.Mock()
        mock_pool.return_value = mock_pool_instance
        
        # Create some pools
        pool_id1 = self.connection_manager.create_connection_pool(
            pool_id="test_pool1",
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db1",
            min_connections=1,
            max_connections=5
        )
        
        pool_id2 = self.connection_manager.create_connection_pool(
            pool_id="test_pool2",
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db2",
            min_connections=2,
            max_connections=10
        )
        
        # Call the method under test
        pools = self.connection_manager.list_connection_pools()
        
        # Verify the pools are listed
        self.assertEqual(len(pools), 2)
        
        # Verify the pool details
        pool1 = next((p for p in pools if p["pool_id"] == pool_id1), None)
        self.assertIsNotNone(pool1)
        self.assertEqual(pool1["db_type"], "postgres")
        self.assertEqual(pool1["database"], "test_db1")
        self.assertEqual(pool1["min_connections"], 1)
        self.assertEqual(pool1["max_connections"], 5)
        
        pool2 = next((p for p in pools if p["pool_id"] == pool_id2), None)
        self.assertIsNotNone(pool2)
        self.assertEqual(pool2["db_type"], "postgres")
        self.assertEqual(pool2["database"], "test_db2")
        self.assertEqual(pool2["min_connections"], 2)
        self.assertEqual(pool2["max_connections"], 10)
        
        # Close the pools
        self.connection_manager.close_connection_pool(pool_id1)
        self.connection_manager.close_connection_pool(pool_id2)

    @mock.patch('psycopg2.connect')
    def test_close_all_connections(self, mock_psycopg2_connect):
        """Test closing all connections."""
        # Set up the mocks
        mock_connection = mock.Mock()
        mock_psycopg2_connect.return_value = mock_connection
        
        # Create some connections
        self.connection_manager.connect(
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db1"
        )
        
        self.connection_manager.connect(
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db2"
        )
        
        # Call the method under test
        self.connection_manager.close_all_connections()
        
        # Verify all connections were closed
        self.assertEqual(len(self.connection_manager._connections), 0)
        
        # Verify the connection close method was called twice
        self.assertEqual(mock_connection.close.call_count, 2)


if __name__ == '__main__':
    unittest.main()