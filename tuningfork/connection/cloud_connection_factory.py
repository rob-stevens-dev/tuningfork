"""
Cloud Connection Factory for database connections.

This module provides functionality to create connections to databases hosted on
cloud platforms (AWS, Azure, GCP) with appropriate authentication and security.
"""

import logging
import importlib
from typing import Dict, Any, Optional, Tuple, List, Union

# Set up logging
logger = logging.getLogger(__name__)


class CloudConnectionFactory:
    """Factory for creating connections to cloud database services."""

    # Supported cloud providers
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"

    # Supported database types per provider
    SUPPORTED_DB_TYPES = {
        AWS: ["postgres", "mysql", "mariadb", "oracle", "sqlserver"],
        AZURE: ["postgres", "mysql", "mariadb", "sqlserver"],
        GCP: ["postgres", "mysql", "sqlserver"],
    }

    def __init__(self):
        """Initialize the Cloud Connection Factory."""
        self._connections: Dict[str, Dict[str, Any]] = {}

    def create_connection(
        self,
        connection_id: str,
        cloud_provider: str,
        db_type: str,
        credentials: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create a connection to a cloud database.

        Args:
            connection_id: Unique identifier for this connection
            cloud_provider: Cloud provider (aws, azure, gcp)
            db_type: Database type (postgres, mysql, etc.)
            credentials: Authentication credentials for the cloud provider
            options: Additional connection options

        Returns:
            Dictionary with connection details

        Raises:
            ValueError: If unsupported cloud provider or database type
            ImportError: If required dependencies are not installed
            Exception: For other connection errors
        """
        # Normalize provider and db_type
        cloud_provider = cloud_provider.lower()
        db_type = db_type.lower()

        # Validate cloud provider
        if cloud_provider not in [self.AWS, self.AZURE, self.GCP]:
            raise ValueError(
                f"Unsupported cloud provider: {cloud_provider}. "
                f"Supported providers: {list(self.SUPPORTED_DB_TYPES.keys())}"
            )

        # Validate database type for the provider
        if db_type not in self.SUPPORTED_DB_TYPES[cloud_provider]:
            raise ValueError(
                f"Unsupported database type {db_type} for {cloud_provider}. "
                f"Supported types: {self.SUPPORTED_DB_TYPES[cloud_provider]}"
            )

        # Initialize options
        if options is None:
            options = {}

        # Check if connection already exists
        if connection_id in self._connections:
            logger.warning(f"Connection {connection_id} already exists. Returning existing connection.")
            return self._connections[connection_id]

        # Create connection based on cloud provider
        try:
            if cloud_provider == self.AWS:
                connection = self._create_aws_connection(db_type, credentials, options)
            elif cloud_provider == self.AZURE:
                connection = self._create_azure_connection(db_type, credentials, options)
            elif cloud_provider == self.GCP:
                connection = self._create_gcp_connection(db_type, credentials, options)
            else:
                # This should never happen due to earlier validation
                raise ValueError(f"Unsupported cloud provider: {cloud_provider}")

            # Store the connection
            self._connections[connection_id] = {
                "connection": connection,
                "cloud_provider": cloud_provider,
                "db_type": db_type,
                "credentials": self._redact_sensitive_info(credentials),
                "options": options,
            }

            logger.info(f"Created cloud connection {connection_id} to {cloud_provider} {db_type} database")
            return self._connections[connection_id]

        except ImportError as e:
            logger.error(f"Missing dependency for {cloud_provider} connection: {str(e)}")
            raise ImportError(
                f"Missing dependency for {cloud_provider} connection. "
                f"Please install required packages: {self._get_required_packages(cloud_provider)}"
            ) from e
        except Exception as e:
            logger.error(f"Failed to create {cloud_provider} connection: {str(e)}")
            raise

    def _create_aws_connection(self, db_type: str, credentials: Dict[str, Any], options: Dict[str, Any]) -> Any:
        """
        Create a connection to an AWS database service.

        Args:
            db_type: Database type
            credentials: AWS credentials
            options: Additional connection options

        Returns:
            Connection object

        Raises:
            ImportError: If required dependencies are not installed
            Exception: For connection errors
        """
        # Import required modules
        try:
            import boto3
        except ImportError:
            raise ImportError("Required package 'boto3' not installed")

        # Create connection based on database type
        connection = None
        
        # Check for credentials
        required_keys = ["region"]
        
        # Add access keys if using key-based auth
        if "use_iam_role" not in credentials or not credentials["use_iam_role"]:
            required_keys.extend(["aws_access_key_id", "aws_secret_access_key"])
        
        # Validate required credentials
        for key in required_keys:
            if key not in credentials:
                raise ValueError(f"Missing required AWS credential: {key}")

        # Create the appropriate AWS client
        session_kwargs = {
            "region_name": credentials["region"]
        }
        
        # Add AWS credentials if provided
        if "aws_access_key_id" in credentials and "aws_secret_access_key" in credentials:
            session_kwargs.update({
                "aws_access_key_id": credentials["aws_access_key_id"],
                "aws_secret_access_key": credentials["aws_secret_access_key"]
            })
            
            # Add session token if provided
            if "aws_session_token" in credentials:
                session_kwargs["aws_session_token"] = credentials["aws_session_token"]

        # Create AWS session
        session = boto3.Session(**session_kwargs)

        if db_type in ["postgres", "mysql", "mariadb"]:
            # RDS and Aurora use the same client
            client = session.client("rds")
            
            # Get endpoint information if instance identifier is provided
            if "instance_identifier" in options:
                try:
                    instance = client.describe_db_instances(
                        DBInstanceIdentifier=options["instance_identifier"]
                    )["DBInstances"][0]
                    
                    # Update options with endpoint information
                    if "endpoint" not in options:
                        options["endpoint"] = instance["Endpoint"]["Address"]
                    if "port" not in options:
                        options["port"] = instance["Endpoint"]["Port"]
                except Exception as e:
                    logger.warning(f"Failed to get RDS instance details: {str(e)}")
            
            # For connecting to the database, we'll use a database-specific connector
            # which will be handled by the ConnectionManager
            connection = {
                "client": client,
                "endpoint": options.get("endpoint"),
                "port": options.get("port"),
                "database": options.get("database"),
                "user": options.get("user"),
                "password": options.get("password"),
                "ssl": options.get("ssl", True),
            }
            
        elif db_type == "sqlserver":
            # Similar to above but for SQL Server RDS
            client = session.client("rds")
            
            # Get endpoint information
            if "instance_identifier" in options:
                try:
                    instance = client.describe_db_instances(
                        DBInstanceIdentifier=options["instance_identifier"]
                    )["DBInstances"][0]
                    
                    # Update options with endpoint information
                    if "endpoint" not in options:
                        options["endpoint"] = instance["Endpoint"]["Address"]
                    if "port" not in options:
                        options["port"] = instance["Endpoint"]["Port"]
                except Exception as e:
                    logger.warning(f"Failed to get RDS instance details: {str(e)}")
            
            connection = {
                "client": client,
                "endpoint": options.get("endpoint"),
                "port": options.get("port"),
                "database": options.get("database"),
                "user": options.get("user"),
                "password": options.get("password"),
                "ssl": options.get("ssl", True),
            }
            
        elif db_type == "oracle":
            # Oracle on RDS
            client = session.client("rds")
            
            # Get endpoint information
            if "instance_identifier" in options:
                try:
                    instance = client.describe_db_instances(
                        DBInstanceIdentifier=options["instance_identifier"]
                    )["DBInstances"][0]
                    
                    # Update options with endpoint information
                    if "endpoint" not in options:
                        options["endpoint"] = instance["Endpoint"]["Address"]
                    if "port" not in options:
                        options["port"] = instance["Endpoint"]["Port"]
                except Exception as e:
                    logger.warning(f"Failed to get RDS instance details: {str(e)}")
            
            connection = {
                "client": client,
                "endpoint": options.get("endpoint"),
                "port": options.get("port"),
                "database": options.get("database"),
                "user": options.get("user"),
                "password": options.get("password"),
                "ssl": options.get("ssl", True),
            }
        
        # For AWS DocumentDB (MongoDB compatible)
        elif db_type == "documentdb":
            client = session.client("docdb")
            
            # Get cluster endpoint information
            if "cluster_identifier" in options:
                try:
                    cluster = client.describe_db_clusters(
                        DBClusterIdentifier=options["cluster_identifier"]
                    )["DBClusters"][0]
                    
                    # Update options with endpoint information
                    if "endpoint" not in options:
                        options["endpoint"] = cluster["Endpoint"]
                    if "port" not in options:
                        options["port"] = cluster["Port"]
                except Exception as e:
                    logger.warning(f"Failed to get DocumentDB cluster details: {str(e)}")
            
            connection = {
                "client": client,
                "endpoint": options.get("endpoint"),
                "port": options.get("port"),
                "database": options.get("database"),
                "user": options.get("user"),
                "password": options.get("password"),
                "ssl": options.get("ssl", True),
            }

        return connection

    def _create_azure_connection(self, db_type: str, credentials: Dict[str, Any], options: Dict[str, Any]) -> Any:
        """
        Create a connection to an Azure database service.

        Args:
            db_type: Database type
            credentials: Azure credentials
            options: Additional connection options

        Returns:
            Connection object

        Raises:
            ImportError: If required dependencies are not installed
            Exception: For connection errors
        """
        # Import required modules
        try:
            from azure.identity import DefaultAzureCredential, ClientSecretCredential
        except ImportError:
            raise ImportError("Required package 'azure-identity' not installed")

        # Check authentication method
        auth_method = credentials.get("auth_method", "default")
        azure_credential = None

        if auth_method == "default":
            # Use DefaultAzureCredential for managed identities, environment variables, etc.
            azure_credential = DefaultAzureCredential()
        elif auth_method == "service_principal":
            # Use service principal authentication
            required_keys = ["tenant_id", "client_id", "client_secret"]
            for key in required_keys:
                if key not in credentials:
                    raise ValueError(f"Missing required Azure credential for service principal: {key}")
                    
            azure_credential = ClientSecretCredential(
                tenant_id=credentials["tenant_id"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"]
            )
        else:
            raise ValueError(f"Unsupported Azure authentication method: {auth_method}")

        # Create connection based on database type
        connection = None

        if db_type == "postgres":
            try:
                # For PostgreSQL, we typically connect directly rather than using the management SDK
                # The management SDK would be used for administrative operations
                from azure.mgmt.rdbms import postgresql
                
                # Import required package for management operations
                try:
                    mgmt_client = postgresql.PostgreSQLManagementClient(
                        credential=azure_credential,
                        subscription_id=credentials["subscription_id"]
                    )
                except ImportError:
                    logger.warning("Azure PostgreSQL management client not available. Install 'azure-mgmt-rdbms'.")
                    mgmt_client = None
                
                # Connection details for database connector
                connection = {
                    "mgmt_client": mgmt_client,
                    "credential": azure_credential,
                    "server": options.get("server"),
                    "database": options.get("database"),
                    "user": options.get("user"),
                    "password": options.get("password"),
                    "ssl": options.get("ssl", True),
                }
                
            except ImportError:
                logger.warning("Azure PostgreSQL management SDK not installed. Using direct connection only.")
                connection = {
                    "server": options.get("server"),
                    "database": options.get("database"),
                    "user": options.get("user"),
                    "password": options.get("password"),
                    "ssl": options.get("ssl", True),
                }
                
        elif db_type == "mysql" or db_type == "mariadb":
            try:
                # For MySQL, similar approach as PostgreSQL
                from azure.mgmt.rdbms import mysql
                
                # Import required package for management operations
                try:
                    mgmt_client = mysql.MySQLManagementClient(
                        credential=azure_credential,
                        subscription_id=credentials["subscription_id"]
                    )
                except ImportError:
                    logger.warning("Azure MySQL management client not available. Install 'azure-mgmt-rdbms'.")
                    mgmt_client = None
                
                # Connection details for database connector
                connection = {
                    "mgmt_client": mgmt_client,
                    "credential": azure_credential,
                    "server": options.get("server"),
                    "database": options.get("database"),
                    "user": options.get("user"),
                    "password": options.get("password"),
                    "ssl": options.get("ssl", True),
                }
                
            except ImportError:
                logger.warning("Azure MySQL management SDK not installed. Using direct connection only.")
                connection = {
                    "server": options.get("server"),
                    "database": options.get("database"),
                    "user": options.get("user"),
                    "password": options.get("password"),
                    "ssl": options.get("ssl", True),
                }
                
        elif db_type == "sqlserver":
            try:
                # For SQL Server, we use the SQL management client
                from azure.mgmt.sql import SqlManagementClient
                
                mgmt_client = SqlManagementClient(
                    credential=azure_credential,
                    subscription_id=credentials["subscription_id"]
                )
                
                connection = {
                    "mgmt_client": mgmt_client,
                    "credential": azure_credential,
                    "server": options.get("server"),
                    "database": options.get("database"),
                    "user": options.get("user"),
                    "password": options.get("password"),
                    "authentication": options.get("authentication", "SqlPassword"),
                    "ssl": options.get("ssl", True),
                }
                
            except ImportError:
                logger.warning("Azure SQL management SDK not installed. Using direct connection only.")
                connection = {
                    "server": options.get("server"),
                    "database": options.get("database"),
                    "user": options.get("user"),
                    "password": options.get("password"),
                    "authentication": options.get("authentication", "SqlPassword"),
                    "ssl": options.get("ssl", True),
                }

        return connection

    def _create_gcp_connection(self, db_type: str, credentials: Dict[str, Any], options: Dict[str, Any]) -> Any:
        """
        Create a connection to a GCP database service.

        Args:
            db_type: Database type
            credentials: GCP credentials
            options: Additional connection options

        Returns:
            Connection object

        Raises:
            ImportError: If required dependencies are not installed
            Exception: For connection errors
        """
        # Import required modules
        try:
            from google.cloud import sql_v1
            from google.oauth2 import service_account
        except ImportError:
            raise ImportError("Required packages 'google-cloud-sql' and 'google-auth' not installed")

        # Initialize GCP credentials
        gcp_credential = None
        
        # Check authentication method
        auth_method = credentials.get("auth_method", "service_account")
        
        if auth_method == "service_account":
            # Service account authentication
            if "service_account_file" in credentials:
                gcp_credential = service_account.Credentials.from_service_account_file(
                    credentials["service_account_file"]
                )
            elif "service_account_info" in credentials:
                gcp_credential = service_account.Credentials.from_service_account_info(
                    credentials["service_account_info"]
                )
            else:
                raise ValueError("Service account credentials missing. Provide either 'service_account_file' or 'service_account_info'")
        elif auth_method == "application_default":
            # Application Default Credentials
            from google.auth import default
            gcp_credential, project_id = default()
        else:
            raise ValueError(f"Unsupported GCP authentication method: {auth_method}")

        # Set project ID
        project_id = credentials.get("project_id")
        if project_id is None and auth_method == "application_default":
            # Already set above
            pass
        elif project_id is None:
            raise ValueError("Missing required GCP credential: project_id")

        # Create Cloud SQL Admin API client
        sql_client = sql_v1.SqlInstancesServiceClient(credentials=gcp_credential)

        # Create connection based on database type
        connection = None

        if db_type == "postgres":
            # For PostgreSQL, we use the Cloud SQL Admin API
            if "instance_name" in options:
                instance_path = f"projects/{project_id}/instances/{options['instance_name']}"
                
                # Get instance details if needed
                try:
                    instance = sql_client.get(name=instance_path)
                    # Extract connection information if not provided
                    if "host" not in options:
                        # The IP is typically available in the settings or can be retrieved separately
                        # For simplicity, we'll assume it's provided in options
                        pass
                except Exception as e:
                    logger.warning(f"Failed to get Cloud SQL instance details: {str(e)}")
            
            # Connection details for database connector
            connection = {
                "sql_client": sql_client,
                "credential": gcp_credential,
                "project_id": project_id,
                "instance_name": options.get("instance_name"),
                "host": options.get("host"),
                "database": options.get("database"),
                "user": options.get("user"),
                "password": options.get("password"),
                "ssl": options.get("ssl", True),
            }
            
        elif db_type == "mysql":
            # For MySQL, similar approach as PostgreSQL
            if "instance_name" in options:
                instance_path = f"projects/{project_id}/instances/{options['instance_name']}"
                
                # Get instance details if needed
                try:
                    instance = sql_client.get(name=instance_path)
                    # Extract connection information if not provided
                    if "host" not in options:
                        # The IP is typically available in the settings or can be retrieved separately
                        # For simplicity, we'll assume it's provided in options
                        pass
                except Exception as e:
                    logger.warning(f"Failed to get Cloud SQL instance details: {str(e)}")
            
            # Connection details for database connector
            connection = {
                "sql_client": sql_client,
                "credential": gcp_credential,
                "project_id": project_id,
                "instance_name": options.get("instance_name"),
                "host": options.get("host"),
                "database": options.get("database"),
                "user": options.get("user"),
                "password": options.get("password"),
                "ssl": options.get("ssl", True),
            }
                
        elif db_type == "sqlserver":
            # For SQL Server, similar approach as PostgreSQL
            if "instance_name" in options:
                instance_path = f"projects/{project_id}/instances/{options['instance_name']}"
                
                # Get instance details if needed
                try:
                    instance = sql_client.get(name=instance_path)
                    # Extract connection information if not provided
                    if "host" not in options:
                        # The IP is typically available in the settings or can be retrieved separately
                        # For simplicity, we'll assume it's provided in options
                        pass
                except Exception as e:
                    logger.warning(f"Failed to get Cloud SQL instance details: {str(e)}")
            
            # Connection details for database connector
            connection = {
                "sql_client": sql_client,
                "credential": gcp_credential,
                "project_id": project_id,
                "instance_name": options.get("instance_name"),
                "host": options.get("host"),
                "database": options.get("database"),
                "user": options.get("user"),
                "password": options.get("password"),
                "ssl": options.get("ssl", True),
            }
            
        return connection

    def close_connection(self, connection_id: str) -> None:
        """
        Close a cloud database connection.

        Args:
            connection_id: ID of the connection to close

        Raises:
            ValueError: If connection_id is not found
        """
        if connection_id not in self._connections:
            raise ValueError(f"Connection {connection_id} not found")

        try:
            connection = self._connections[connection_id]["connection"]
            # Close connection based on cloud provider and db type
            cloud_provider = self._connections[connection_id]["cloud_provider"]
            db_type = self._connections[connection_id]["db_type"]
            
            # Most cloud connections don't need explicit closure as they're stateless
            # API clients, but we'll handle any that do
            
            logger.info(f"Closed cloud connection {connection_id}")
            
        except Exception as e:
            logger.error(f"Error closing cloud connection {connection_id}: {str(e)}")
        finally:
            del self._connections[connection_id]

    def list_connections(self) -> List[Dict[str, str]]:
        """
        List all active cloud connections.

        Returns:
            List of dictionaries with connection details
        """
        return [
            {
                "connection_id": cid,
                "cloud_provider": details["cloud_provider"],
                "db_type": details["db_type"],
            }
            for cid, details in self._connections.items()
        ]

    def get_connection(self, connection_id: str) -> Dict[str, Any]:
        """
        Get details for a specific connection.

        Args:
            connection_id: ID of the connection to get

        Returns:
            Dictionary with connection details

        Raises:
            ValueError: If connection_id is not found
        """
        if connection_id not in self._connections:
            raise ValueError(f"Connection {connection_id} not found")
        
        return self._connections[connection_id]

    def close_all_connections(self) -> None:
        """Close all active cloud connections."""
        connection_ids = list(self._connections.keys())
        
        for connection_id in connection_ids:
            try:
                self.close_connection(connection_id)
            except Exception as e:
                logger.error(f"Error closing connection {connection_id}: {str(e)}")

    def _redact_sensitive_info(self, credentials: Dict[str, Any]) -> Dict[str, Any]:
        """
        Redact sensitive information from credentials.

        Args:
            credentials: Dictionary with credentials

        Returns:
            Dictionary with sensitive information redacted
        """
        redacted = credentials.copy()
        
        # List of sensitive keys to redact
        sensitive_keys = [
            "aws_secret_access_key", "aws_session_token", "client_secret", 
            "password", "private_key", "service_account_info", "api_key",
            "token", "secret"
        ]
        
        # Redact all sensitive keys
        for key in sensitive_keys:
            if key in redacted:
                redacted[key] = "********"
                
        return redacted

    def _get_required_packages(self, cloud_provider: str) -> List[str]:
        """
        Get the required packages for a cloud provider.

        Args:
            cloud_provider: Cloud provider name

        Returns:
            List of required package names
        """
        if cloud_provider == self.AWS:
            return ["boto3"]
        elif cloud_provider == self.AZURE:
            return ["azure-identity", "azure-mgmt-rdbms", "azure-mgmt-sql"]
        elif cloud_provider == self.GCP:
            return ["google-cloud-sql", "google-auth"]
        else:
            return []

    def __del__(self) -> None:
        """Ensure all connections are closed when the object is destroyed."""
        try:
            self.close_all_connections()
        except Exception:
            pass