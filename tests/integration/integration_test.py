#!/usr/bin/env python3
"""
Integration test for Stage 1.2 (Enhanced Connection Management)

This script tests the integration between the three main components:
- SSH Manager
- Cloud Connection Factory
- Connection Manager

Usage: python integration_test.py
"""

import logging
import unittest
from unittest import mock

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("integration_test")

# Import the components
try:
    from tuningfork.connection.ssh_manager import SSHManager
    from tuningfork.connection.cloud_connection_factory import CloudConnectionFactory
    from tuningfork.connection.connection_manager import ConnectionManager
except ImportError as e:
    logger.error(f"Failed to import components: {str(e)}")
    logger.error("Make sure all components are in the correct location.")
    exit(1)


class TestIntegration(unittest.TestCase):
    """Test suite for the integration of all components."""

    def setUp(self):
        """Set up test environment."""
        # Create instances
        self.ssh_manager = SSHManager()
        self.cloud_factory = CloudConnectionFactory()
        self.connection_manager = ConnectionManager(
            ssh_manager=self.ssh_manager,
            cloud_connection_factory=self.cloud_factory
        )
        
    def tearDown(self):
        """Clean up after tests."""
        # Close all connections and tunnels
        self.connection_manager.close_all_connections()
        self.ssh_manager.close_all_tunnels()
        self.cloud_factory.close_all_connections()

    @mock.patch('psycopg2.connect')
    @mock.patch('paramiko.SSHClient')
    def test_ssh_tunnel_connection(self, mock_ssh_client, mock_psycopg2_connect):
        """Test connecting to a database through an SSH tunnel."""
        # Set up the SSH mock
        mock_client_instance = mock.Mock()
        mock_ssh_client.return_value = mock_client_instance
        
        mock_transport = mock.Mock()
        mock_client_instance.get_transport.return_value = mock_transport
        
        # Create a mock socket for the connection test
        with mock.patch('socket.create_connection'):
            # Create a connection with SSH tunneling
            connection_id = self.connection_manager.connect(
                db_type="postgres",
                host="db.example.com",
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
            
            # Verify the connection was created
            self.assertIsNotNone(connection_id)
            
            # Verify the SSH tunnel was created
            tunnels = self.ssh_manager.list_tunnels()
            self.assertEqual(len(tunnels), 1)
            
            # Verify the connection is in the list
            connections = self.connection_manager.list_connections()
            self.assertEqual(len(connections), 1)
            self.assertEqual(connections[0]["connection_id"], connection_id)
            
            # Close the connection
            self.connection_manager.disconnect(connection_id)
            
            # Verify the connection was closed
            connections = self.connection_manager.list_connections()
            self.assertEqual(len(connections), 0)
            
            # Verify the SSH tunnel was closed
            tunnels = self.ssh_manager.list_tunnels()
            self.assertEqual(len(tunnels), 0)

    @mock.patch('boto3.Session')
    @mock.patch('psycopg2.connect')
    def test_cloud_connection(self, mock_psycopg2_connect, mock_boto3_session):
        """Test connecting to a cloud database."""
        # Set up the boto3 mock
        mock_session = mock.Mock()
        mock_boto3_session.return_value = mock_session
        
        mock_rds_client = mock.Mock()
        mock_session.client.return_value = mock_rds_client
        
        # Create a connection with cloud integration
        connection_id = self.connection_manager.connect(
            db_type="postgres",
            host="db.amazonaws.com",
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
                },
                "options": {
                    "instance_identifier": "test-instance"
                }
            }
        )
        
        # Verify the connection was created
        self.assertIsNotNone(connection_id)
        
        # Verify the cloud connection was created
        connections = self.cloud_factory.list_connections()
        self.assertEqual(len(connections), 1)
        
        # Verify the connection is in the list
        connections = self.connection_manager.list_connections()
        self.assertEqual(len(connections), 1)
        self.assertEqual(connections[0]["connection_id"], connection_id)
        
        # Close the connection
        self.connection_manager.disconnect(connection_id)
        
        # Verify the connection was closed
        connections = self.connection_manager.list_connections()
        self.assertEqual(len(connections), 0)
        
        # Verify the cloud connection was closed
        connections = self.cloud_factory.list_connections()
        self.assertEqual(len(connections), 0)

    @mock.patch('psycopg2.pool.ThreadedConnectionPool')
    def test_connection_pool(self, mock_threaded_pool):
        """Test connection pooling."""
        # Set up the pool mock
        mock_pool_instance = mock.Mock()
        mock_threaded_pool.return_value = mock_pool_instance
        
        # Create a connection pool
        pool_id = self.connection_manager.create_connection_pool(
            pool_id="test_pool",
            db_type="postgres",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db",
            min_connections=2,
            max_connections=5
        )
        
        # Verify the pool was created
        self.assertEqual(pool_id, "test_pool")
        
        # Verify the pool is in the list
        pools = self.connection_manager.list_connection_pools()
        self.assertEqual(len(pools), 1)
        self.assertEqual(pools[0]["pool_id"], pool_id)
        
        # Mock the getconn method
        mock_pool_instance.getconn.return_value = mock.Mock()
        
        # Get a connection from the pool
        connection_id, connection = self.connection_manager.get_connection_from_pool(pool_id)
        
        # Verify the connection was created
        self.assertIsNotNone(connection_id)
        self.assertTrue(connection_id.startswith(pool_id))
        
        # Verify the connection is in the list
        connections = self.connection_manager.list_connections()
        self.assertEqual(len(connections), 1)
        self.assertEqual(connections[0]["connection_id"], connection_id)
        self.assertTrue(connections[0]["from_pool"])
        self.assertEqual(connections[0]["pool_id"], pool_id)
        
        # Return the connection to the pool
        self.connection_manager.return_connection_to_pool(connection_id)
        
        # Verify the connection was removed
        connections = self.connection_manager.list_connections()
        self.assertEqual(len(connections), 0)
        
        # Close the pool
        self.connection_manager.close_connection_pool(pool_id)
        
        # Verify the pool was closed
        pools = self.connection_manager.list_connection_pools()
        self.assertEqual(len(pools), 0)

    @mock.patch('psycopg2.connect')
    def test_transaction_management(self, mock_psycopg2_connect):
        """Test transaction management."""
        # Set up the mock
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
        connections = self.connection_manager.list_connections()
        self.assertEqual(len(connections), 1)
        self.assertTrue(connections[0]["transaction_active"])
        
        # Commit the transaction
        self.connection_manager.commit(connection_id)
        
        # Verify the transaction was committed
        connections = self.connection_manager.list_connections()
        self.assertEqual(len(connections), 1)
        self.assertFalse(connections[0]["transaction_active"])
        
        # Begin another transaction
        self.connection_manager.begin_transaction(connection_id)
        
        # Rollback the transaction
        self.connection_manager.rollback(connection_id)
        
        # Verify the transaction was rolled back
        connections = self.connection_manager.list_connections()
        self.assertEqual(len(connections), 1)
        self.assertFalse(connections[0]["transaction_active"])
        
        # Close the connection
        self.connection_manager.disconnect(connection_id)


if __name__ == "__main__":
    # Run the tests
    logger.info("Running integration tests...")
    unittest.main()