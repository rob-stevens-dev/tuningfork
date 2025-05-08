"""
Unit tests for the SSH Manager.
"""

import os
import socket
import threading
import time
import unittest
from unittest import mock

import paramiko
from paramiko.client import SSHClient

from tuningfork.connection.ssh_manager import SSHManager


class TestSSHManager(unittest.TestCase):
    """Test suite for the SSH Manager."""

    def setUp(self):
        """Set up test environment."""
        self.ssh_manager = SSHManager()
        
        # Mock data for SSH tunneling
        self.tunnel_id = "test_tunnel"
        self.ssh_host = "test.ssh.server"
        self.ssh_port = 22
        self.ssh_username = "test_user"
        self.ssh_password = "test_password"
        self.remote_host = "db.example.com"
        self.remote_port = 5432
        self.local_port = 10000
        
    def tearDown(self):
        """Clean up after tests."""
        self.ssh_manager.close_all_tunnels()

    @mock.patch('paramiko.SSHClient')
    def test_create_tunnel_with_password(self, mock_ssh_client):
        """Test creating an SSH tunnel using password authentication."""
        # Set up the mock
        mock_client_instance = mock.Mock()
        mock_ssh_client.return_value = mock_client_instance
        
        mock_transport = mock.Mock()
        mock_client_instance.get_transport.return_value = mock_transport
        
        # Create a mock socket for the connection test
        mock_socket = mock.patch('socket.create_connection')
        mock_socket.start()
        
        # Call the method under test
        local_host, local_port = self.ssh_manager.create_tunnel(
            tunnel_id=self.tunnel_id,
            ssh_host=self.ssh_host,
            ssh_port=self.ssh_port,
            ssh_username=self.ssh_username,
            ssh_password=self.ssh_password,
            remote_host=self.remote_host,
            remote_port=self.remote_port,
            local_port=self.local_port,
            timeout=1
        )
        
        # Stop the socket mock
        mock_socket.stop()
        
        # Verify the results
        self.assertEqual(local_host, "127.0.0.1")
        self.assertEqual(local_port, self.local_port)
        
        # Verify the SSH client was created and used correctly
        mock_ssh_client.assert_called_once()
        mock_client_instance.set_missing_host_key_policy.assert_called_once()
        mock_client_instance.connect.assert_called_once_with(
            hostname=self.ssh_host,
            port=self.ssh_port,
            username=self.ssh_username,
            password=self.ssh_password,
            timeout=1
        )
        
        # Verify the transport was used correctly
        mock_transport.set_keepalive.assert_called_once()
        
        # Verify the tunnel was stored correctly
        self.assertIn(self.tunnel_id, self.ssh_manager._tunnels)
        self.assertEqual(self.ssh_manager._tunnels[self.tunnel_id]["local_port"], self.local_port)
        self.assertEqual(self.ssh_manager._tunnels[self.tunnel_id]["remote_host"], self.remote_host)
        self.assertEqual(self.ssh_manager._tunnels[self.tunnel_id]["remote_port"], self.remote_port)
        self.assertEqual(self.ssh_manager._tunnels[self.tunnel_id]["ssh_host"], self.ssh_host)

    @mock.patch('paramiko.SSHClient')
    @mock.patch('paramiko.RSAKey.from_private_key_file')
    def test_create_tunnel_with_key_file(self, mock_rsa_key, mock_ssh_client):
        """Test creating an SSH tunnel using key-based authentication."""
        # Set up the mocks
        mock_client_instance = mock.Mock()
        mock_ssh_client.return_value = mock_client_instance
        
        mock_key = mock.Mock()
        mock_rsa_key.return_value = mock_key
        
        mock_transport = mock.Mock()
        mock_client_instance.get_transport.return_value = mock_transport
        
        # Create a mock socket for the connection test
        mock_socket = mock.patch('socket.create_connection')
        mock_socket.start()
        
        # Call the method under test
        local_host, local_port = self.ssh_manager.create_tunnel(
            tunnel_id=self.tunnel_id,
            ssh_host=self.ssh_host,
            ssh_port=self.ssh_port,
            ssh_username=self.ssh_username,
            remote_host=self.remote_host,
            remote_port=self.remote_port,
            local_port=self.local_port,
            ssh_key_file="/path/to/key.pem",
            ssh_key_passphrase="passphrase",
            timeout=1
        )
        
        # Stop the socket mock
        mock_socket.stop()
        
        # Verify the results
        self.assertEqual(local_host, "127.0.0.1")
        self.assertEqual(local_port, self.local_port)
        
        # Verify the RSA key was loaded correctly
        mock_rsa_key.assert_called_once_with("/path/to/key.pem", password="passphrase")
        
        # Verify the SSH client was created and used correctly
        mock_ssh_client.assert_called_once()
        mock_client_instance.set_missing_host_key_policy.assert_called_once()
        mock_client_instance.connect.assert_called_once_with(
            hostname=self.ssh_host,
            port=self.ssh_port,
            username=self.ssh_username,
            pkey=mock_key,
            timeout=1
        )
        
        # Verify the transport was used correctly
        mock_transport.set_keepalive.assert_called_once()
        
        # Verify the tunnel was stored correctly
        self.assertIn(self.tunnel_id, self.ssh_manager._tunnels)

    @mock.patch('paramiko.SSHClient')
    def test_create_tunnel_without_auth(self, mock_ssh_client):
        """Test creating an SSH tunnel without providing authentication."""
        # Call the method under test and verify it raises an error
        with self.assertRaises(ValueError):
            self.ssh_manager.create_tunnel(
                tunnel_id=self.tunnel_id,
                ssh_host=self.ssh_host,
                ssh_port=self.ssh_port,
                ssh_username=self.ssh_username,
                remote_host=self.remote_host,
                remote_port=self.remote_port,
                local_port=self.local_port
            )

    @mock.patch('paramiko.SSHClient')
    def test_close_tunnel(self, mock_ssh_client):
        """Test closing an SSH tunnel."""
        # Set up the mock
        mock_client_instance = mock.Mock()
        mock_ssh_client.return_value = mock_client_instance
        
        mock_transport = mock.Mock()
        mock_client_instance.get_transport.return_value = mock_transport
        
        # Create a mock socket for the connection test
        mock_socket = mock.patch('socket.create_connection')
        mock_socket.start()
        
        # Create a tunnel
        local_host, local_port = self.ssh_manager.create_tunnel(
            tunnel_id=self.tunnel_id,
            ssh_host=self.ssh_host,
            ssh_port=self.ssh_port,
            ssh_username=self.ssh_username,
            remote_host=self.remote_host,
            remote_port=self.remote_port,
            local_port=self.local_port,
            ssh_password=self.ssh_password,
            timeout=1
        )
        
        # Stop the socket mock
        mock_socket.stop()
        
        # Call the method under test
        self.ssh_manager.close_tunnel(self.tunnel_id)
        
        # Verify the SSH client was closed
        mock_client_instance.close.assert_called_once()
        
        # Verify the tunnel was removed
        self.assertNotIn(self.tunnel_id, self.ssh_manager._tunnels)

    def test_close_nonexistent_tunnel(self):
        """Test closing a tunnel that doesn't exist."""
        with self.assertRaises(ValueError):
            self.ssh_manager.close_tunnel("nonexistent_tunnel")

    @mock.patch('paramiko.SSHClient')
    def test_list_tunnels(self, mock_ssh_client):
        """Test listing all tunnels."""
        # Set up the mock
        mock_client_instance = mock.Mock()
        mock_ssh_client.return_value = mock_client_instance
        
        mock_transport = mock.Mock()
        mock_client_instance.get_transport.return_value = mock_transport
        mock_transport.is_active.return_value = True
        
        # Create a mock socket for the connection test
        mock_socket = mock.patch('socket.create_connection')
        mock_socket.start()
        
        # Create tunnels
        self.ssh_manager.create_tunnel(
            tunnel_id="tunnel1",
            ssh_host=self.ssh_host,
            ssh_port=self.ssh_port,
            ssh_username=self.ssh_username,
            remote_host=self.remote_host,
            remote_port=self.remote_port,
            local_port=10001,
            ssh_password=self.ssh_password,
            timeout=1
        )
        
        self.ssh_manager.create_tunnel(
            tunnel_id="tunnel2",
            ssh_host=self.ssh_host,
            ssh_port=self.ssh_port,
            ssh_username=self.ssh_username,
            remote_host="other.example.com",
            remote_port=3306,
            local_port=10002,
            ssh_password=self.ssh_password,
            timeout=1
        )
        
        # Stop the socket mock
        mock_socket.stop()
        
        # Call the method under test
        tunnels = self.ssh_manager.list_tunnels()
        
        # Verify the results
        self.assertEqual(len(tunnels), 2)
        
        # Verify the first tunnel
        tunnel1 = next((t for t in tunnels if t["tunnel_id"] == "tunnel1"), None)
        self.assertIsNotNone(tunnel1)
        self.assertEqual(tunnel1["local_port"], 10001)
        self.assertEqual(tunnel1["remote_host"], self.remote_host)
        self.assertEqual(tunnel1["remote_port"], self.remote_port)
        self.assertEqual(tunnel1["active"], True)
        
        # Verify the second tunnel
        tunnel2 = next((t for t in tunnels if t["tunnel_id"] == "tunnel2"), None)
        self.assertIsNotNone(tunnel2)
        self.assertEqual(tunnel2["local_port"], 10002)
        self.assertEqual(tunnel2["remote_host"], "other.example.com")
        self.assertEqual(tunnel2["remote_port"], 3306)
        self.assertEqual(tunnel2["active"], True)

    @mock.patch('paramiko.SSHClient')
    def test_is_tunnel_active(self, mock_ssh_client):
        """Test checking if a tunnel is active."""
        # Set up the mock
        mock_client_instance = mock.Mock()
        mock_ssh_client.return_value = mock_client_instance
        
        mock_transport = mock.Mock()
        mock_client_instance.get_transport.return_value = mock_transport
        mock_transport.is_active.return_value = True
        
        # Create a mock socket for the connection test
        mock_socket = mock.patch('socket.create_connection')
        mock_socket.start()
        
        # Create a tunnel
        self.ssh_manager.create_tunnel(
            tunnel_id=self.tunnel_id,
            ssh_host=self.ssh_host,
            ssh_port=self.ssh_port,
            ssh_username=self.ssh_username,
            remote_host=self.remote_host,
            remote_port=self.remote_port,
            local_port=self.local_port,
            ssh_password=self.ssh_password,
            timeout=1
        )
        
        # Stop the socket mock
        mock_socket.stop()
        
        # Call the method under test
        is_active = self.ssh_manager.is_tunnel_active(self.tunnel_id)
        
        # Verify the results
        self.assertTrue(is_active)
        self.assertTrue(mock_transport.is_active.called)
        
        # Test with a nonexistent tunnel
        is_active = self.ssh_manager.is_tunnel_active("nonexistent_tunnel")
        self.assertFalse(is_active)

    @mock.patch('paramiko.SSHClient')
    def test_close_all_tunnels(self, mock_ssh_client):
        """Test closing all tunnels."""
        # Set up the mock
        mock_client_instance = mock.Mock()
        mock_ssh_client.return_value = mock_client_instance
        
        mock_transport = mock.Mock()
        mock_client_instance.get_transport.return_value = mock_transport
        
        # Create a mock socket for the connection test
        mock_socket = mock.patch('socket.create_connection')
        mock_socket.start()
        
        # Create tunnels
        self.ssh_manager.create_tunnel(
            tunnel_id="tunnel1",
            ssh_host=self.ssh_host,
            ssh_port=self.ssh_port,
            ssh_username=self.ssh_username,
            remote_host=self.remote_host,
            remote_port=self.remote_port,
            local_port=10001,
            ssh_password=self.ssh_password,
            timeout=1
        )
        
        self.ssh_manager.create_tunnel(
            tunnel_id="tunnel2",
            ssh_host=self.ssh_host,
            ssh_port=self.ssh_port,
            ssh_username=self.ssh_username,
            remote_host="other.example.com",
            remote_port=3306,
            local_port=10002,
            ssh_password=self.ssh_password,
            timeout=1
        )
        
        # Stop the socket mock
        mock_socket.stop()
        
        # Call the method under test
        self.ssh_manager.close_all_tunnels()
        
        # Verify all tunnels were closed
        self.assertEqual(len(self.ssh_manager._tunnels), 0)
        
        # Verify the SSH clients were closed
        self.assertEqual(mock_client_instance.close.call_count, 2)

    def test_find_available_port(self):
        """Test finding an available port."""
        # Call the method under test
        port = self.ssh_manager._find_available_port()
        
        # Verify the result is a valid port number
        self.assertIsInstance(port, int)
        self.assertGreater(port, 0)
        self.assertLess(port, 65536)
        
        # Verify the port is actually available
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("127.0.0.1", port))
        except socket.error:
            self.fail("Port is not available")
        finally:
            s.close()

    @mock.patch('paramiko.SSHClient')
    @mock.patch('paramiko.Transport')
    def test_start_tunnel(self, mock_transport, mock_ssh_client):
        """Test the tunnel forwarding thread."""
        # Set up the mocks
        mock_transport_instance = mock.Mock()
        mock_transport.return_value = mock_transport_instance
        
        # Set up the transport to be active once then inactive
        mock_transport_instance.is_active.side_effect = [True, False]
        
        # Create a test thread function
        def test_tunnel():
            self.ssh_manager._start_tunnel(
                mock_transport_instance,
                "127.0.0.1",
                self.local_port,
                self.remote_host,
                self.remote_port
            )
        
        # Start the tunnel thread
        thread = threading.Thread(target=test_tunnel)
        thread.daemon = True
        thread.start()
        
        # Give the thread time to execute
        time.sleep(0.1)
        
        # Verify the port forwarding was requested
        mock_transport_instance.request_port_forward.assert_called_once_with(
            "127.0.0.1",
            self.local_port,
            self.remote_host,
            self.remote_port
        )
        
        # Wait for the thread to complete
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())
        
        # Verify the port forwarding was canceled
        mock_transport_instance.cancel_port_forward.assert_called_once_with(
            "127.0.0.1",
            self.local_port
        )


if __name__ == '__main__':
    unittest.main()