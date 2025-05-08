"""
SSH Manager for database connections.

This module provides SSH tunneling functionality for connecting to databases
that are not directly accessible but can be reached through an SSH server.
"""

import logging
import socket
import threading
import time
from typing import Dict, Optional, Tuple, Any, List, Union

import paramiko
from paramiko.ssh_exception import SSHException

# Set up logging
logger = logging.getLogger(__name__)


class SSHManager:
    """Manages SSH connections and tunneling for database access."""

    def __init__(self):
        """Initialize the SSH Manager."""
        self._tunnels: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def create_tunnel(
        self,
        tunnel_id: str,
        ssh_host: str,
        ssh_port: int,
        ssh_username: str,
        remote_host: str,
        remote_port: int,
        local_port: Optional[int] = None,
        ssh_password: Optional[str] = None,
        ssh_key_file: Optional[str] = None,
        ssh_key_passphrase: Optional[str] = None,
        timeout: int = 10,
    ) -> Tuple[str, int]:
        """
        Create an SSH tunnel to access a remote database server.

        Args:
            tunnel_id: Unique identifier for this tunnel
            ssh_host: SSH server hostname or IP
            ssh_port: SSH server port
            ssh_username: SSH username
            remote_host: Remote database hostname or IP (from SSH server perspective)
            remote_port: Remote database port
            local_port: Local port to forward to remote port (if None, a random available port will be used)
            ssh_password: SSH password (if not using key-based auth)
            ssh_key_file: Path to SSH private key file
            ssh_key_passphrase: Passphrase for SSH private key (if needed)
            timeout: Connection timeout in seconds

        Returns:
            Tuple of (local_host, local_port) that can be used to connect to the remote database

        Raises:
            ValueError: If invalid parameters are provided
            SSHException: If SSH connection fails
            TimeoutError: If connection times out
        """
        with self._lock:
            if tunnel_id in self._tunnels:
                logger.warning(f"Tunnel {tunnel_id} already exists. Returning existing connection.")
                return "127.0.0.1", self._tunnels[tunnel_id]["local_port"]

            # Validate authentication parameters
            if not ssh_password and not ssh_key_file:
                raise ValueError("Either ssh_password or ssh_key_file must be provided")

            # Find an available local port if not specified
            if local_port is None:
                local_port = self._find_available_port()

            # Create SSH client
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            try:
                # Connect to SSH server
                if ssh_key_file:
                    try:
                        pkey = paramiko.RSAKey.from_private_key_file(
                            ssh_key_file, password=ssh_key_passphrase
                        )
                        client.connect(
                            hostname=ssh_host,
                            port=ssh_port,
                            username=ssh_username,
                            pkey=pkey,
                            timeout=timeout,
                        )
                    except (paramiko.ssh_exception.PasswordRequiredException, SSHException) as e:
                        if "private key file is encrypted" in str(e) and not ssh_key_passphrase:
                            raise ValueError("SSH key requires a passphrase") from e
                        raise
                else:
                    client.connect(
                        hostname=ssh_host,
                        port=ssh_port,
                        username=ssh_username,
                        password=ssh_password,
                        timeout=timeout,
                    )

                # Start the tunnel
                transport = client.get_transport()
                transport.set_keepalive(60)  # Send keepalive packet every 60 seconds

                # Create the tunnel thread
                tunnel_thread = threading.Thread(
                    target=self._start_tunnel,
                    args=(transport, "127.0.0.1", local_port, remote_host, remote_port),
                    daemon=True,
                )
                tunnel_thread.start()

                # Wait for tunnel to become active
                start_time = time.time()
                while time.time() - start_time < timeout:
                    try:
                        with socket.create_connection(("127.0.0.1", local_port), timeout=1):
                            break
                    except (socket.timeout, ConnectionRefusedError):
                        time.sleep(0.1)
                        continue
                else:
                    # Tunnel creation timed out
                    client.close()
                    raise TimeoutError(f"Timeout establishing SSH tunnel on local port {local_port}")

                # Store the tunnel details
                self._tunnels[tunnel_id] = {
                    "client": client,
                    "transport": transport,
                    "thread": tunnel_thread,
                    "local_port": local_port,
                    "remote_host": remote_host,
                    "remote_port": remote_port,
                    "ssh_host": ssh_host,
                }

                logger.info(
                    f"Created SSH tunnel {tunnel_id}: local 127.0.0.1:{local_port} -> "
                    f"{ssh_host} -> {remote_host}:{remote_port}"
                )
                return "127.0.0.1", local_port

            except Exception as e:
                if client:
                    client.close()
                logger.error(f"Failed to create SSH tunnel: {str(e)}")
                raise

    def _start_tunnel(
        self, transport: paramiko.Transport, local_host: str, local_port: int, remote_host: str, remote_port: int
    ) -> None:
        """
        Start the actual SSH tunnel forwarding.

        Args:
            transport: Paramiko transport object
            local_host: Local host to bind to
            local_port: Local port to bind to
            remote_host: Remote host to connect to
            remote_port: Remote port to connect to
        """
        try:
            transport.request_port_forward(local_host, local_port, remote_host, remote_port)
            while transport.is_active():
                time.sleep(1)
        except Exception as e:
            logger.error(f"Error in SSH tunnel: {str(e)}")
        finally:
            try:
                transport.cancel_port_forward(local_host, local_port)
            except Exception:
                pass
            logger.info(f"SSH tunnel closed: {local_host}:{local_port} -> {remote_host}:{remote_port}")

    def close_tunnel(self, tunnel_id: str) -> None:
        """
        Close an SSH tunnel.

        Args:
            tunnel_id: ID of the tunnel to close

        Raises:
            ValueError: If tunnel_id is not found
        """
        with self._lock:
            if tunnel_id not in self._tunnels:
                raise ValueError(f"Tunnel {tunnel_id} not found")

            tunnel = self._tunnels[tunnel_id]
            try:
                transport = tunnel["transport"]
                if transport.is_active():
                    transport.cancel_port_forward("127.0.0.1", tunnel["local_port"])
                tunnel["client"].close()
                logger.info(f"Closed SSH tunnel {tunnel_id}")
            except Exception as e:
                logger.error(f"Error closing SSH tunnel {tunnel_id}: {str(e)}")
            finally:
                del self._tunnels[tunnel_id]

    def list_tunnels(self) -> List[Dict[str, Union[str, int]]]:
        """
        List all active SSH tunnels.

        Returns:
            List of dictionaries with tunnel details
        """
        with self._lock:
            return [
                {
                    "tunnel_id": tid,
                    "local_port": details["local_port"],
                    "remote_host": details["remote_host"],
                    "remote_port": details["remote_port"],
                    "ssh_host": details["ssh_host"],
                    "active": details["transport"].is_active(),
                }
                for tid, details in self._tunnels.items()
            ]

    def is_tunnel_active(self, tunnel_id: str) -> bool:
        """
        Check if an SSH tunnel is active.

        Args:
            tunnel_id: ID of the tunnel to check

        Returns:
            True if tunnel is active, False otherwise
        """
        with self._lock:
            if tunnel_id not in self._tunnels:
                return False
            return self._tunnels[tunnel_id]["transport"].is_active()

    def _find_available_port(self) -> int:
        """
        Find an available local port to use for tunneling.

        Returns:
            Available port number
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    def close_all_tunnels(self) -> None:
        """Close all active SSH tunnels."""
        with self._lock:
            tunnel_ids = list(self._tunnels.keys())
            
        for tunnel_id in tunnel_ids:
            try:
                self.close_tunnel(tunnel_id)
            except Exception as e:
                logger.error(f"Error closing tunnel {tunnel_id}: {str(e)}")

    def __del__(self) -> None:
        """Ensure all tunnels are closed when the object is destroyed."""
        try:
            self.close_all_tunnels()
        except Exception:
            pass