"""
Unit tests for the Cloud Connection Factory.
"""

import unittest
from unittest import mock

from tuningfork.connection.cloud_connection_factory import CloudConnectionFactory


class TestCloudConnectionFactory(unittest.TestCase):
    """Test suite for the Cloud Connection Factory."""

    def setUp(self):
        """Set up test environment."""
        self.cloud_factory = CloudConnectionFactory()
        
        # Test data for AWS connections
        self.aws_connection_id = "aws_connection"
        self.aws_credentials = {
            "region": "us-west-2",
            "aws_access_key_id": "test_access_key",
            "aws_secret_access_key": "test_secret_key"
        }
        self.aws_options = {
            "endpoint": "test-db.amazonaws.com",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "test_password"
        }
        
        # Test data for Azure connections
        self.azure_connection_id = "azure_connection"
        self.azure_credentials = {
            "auth_method": "service_principal",
            "tenant_id": "test_tenant_id",
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "subscription_id": "test_subscription_id"
        }
        self.azure_options = {
            "server": "test-db.database.windows.net",
            "database": "test_db",
            "user": "test_user",
            "password": "test_password"
        }
        
        # Test data for GCP connections
        self.gcp_connection_id = "gcp_connection"
        self.gcp_credentials = {
            "auth_method": "service_account",
            "project_id": "test-project",
            "service_account_info": {
                "type": "service_account",
                "project_id": "test-project",
                "private_key_id": "test_key_id",
                "private_key": "test_private_key",
                "client_email": "test@example.com",
                "client_id": "test_client_id"
            }
        }
        self.gcp_options = {
            "instance_name": "test-instance",
            "host": "10.0.0.1",
            "database": "test_db",
            "user": "test_user",
            "password": "test_password"
        }
    
    def tearDown(self):
        """Clean up after tests."""
        self.cloud_factory.close_all_connections()

    @mock.patch('boto3.Session')
    def test_create_aws_connection_postgres(self, mock_boto3_session):
        """Test creating an AWS connection for PostgreSQL."""
        # Set up the mock
        mock_session = mock.Mock()
        mock_boto3_session.return_value = mock_session
        
        mock_rds_client = mock.Mock()
        mock_session.client.return_value = mock_rds_client
        
        # Call the method under test
        connection_details = self.cloud_factory.create_connection(
            connection_id=self.aws_connection_id,
            cloud_provider=CloudConnectionFactory.AWS,
            db_type="postgres",
            credentials=self.aws_credentials,
            options=self.aws_options
        )
        
        # Verify the results
        self.assertEqual(connection_details["cloud_provider"], CloudConnectionFactory.AWS)
        self.assertEqual(connection_details["db_type"], "postgres")
        
        # Verify boto3 session was created with the right parameters
        mock_boto3_session.assert_called_once_with(
            region_name=self.aws_credentials["region"],
            aws_access_key_id=self.aws_credentials["aws_access_key_id"],
            aws_secret_access_key=self.aws_credentials["aws_secret_access_key"]
        )
        
        # Verify RDS client was created
        mock_session.client.assert_called_once_with("rds")
        
        # Verify connection stored in factory
        self.assertIn(self.aws_connection_id, self.cloud_factory._connections)
        stored_connection = self.cloud_factory._connections[self.aws_connection_id]
        self.assertEqual(stored_connection["cloud_provider"], CloudConnectionFactory.AWS)
        self.assertEqual(stored_connection["db_type"], "postgres")

    @mock.patch('boto3.Session')
    def test_create_aws_connection_mysql(self, mock_boto3_session):
        """Test creating an AWS connection for MySQL."""
        # Set up the mock
        mock_session = mock.Mock()
        mock_boto3_session.return_value = mock_session
        
        mock_rds_client = mock.Mock()
        mock_session.client.return_value = mock_rds_client
        
        # Call the method under test
        connection_details = self.cloud_factory.create_connection(
            connection_id=self.aws_connection_id,
            cloud_provider=CloudConnectionFactory.AWS,
            db_type="mysql",
            credentials=self.aws_credentials,
            options=self.aws_options
        )
        
        # Verify the results
        self.assertEqual(connection_details["cloud_provider"], CloudConnectionFactory.AWS)
        self.assertEqual(connection_details["db_type"], "mysql")
        
        # Verify boto3 session was created with the right parameters
        mock_boto3_session.assert_called_once_with(
            region_name=self.aws_credentials["region"],
            aws_access_key_id=self.aws_credentials["aws_access_key_id"],
            aws_secret_access_key=self.aws_credentials["aws_secret_access_key"]
        )
        
        # Verify RDS client was created
        mock_session.client.assert_called_once_with("rds")

    @mock.patch('boto3.Session')
    def test_create_aws_connection_with_instance_details(self, mock_boto3_session):
        """Test creating an AWS connection with instance details."""
        # Set up the mock
        mock_session = mock.Mock()
        mock_boto3_session.return_value = mock_session
        
        mock_rds_client = mock.Mock()
        mock_session.client.return_value = mock_rds_client
        
        # Set up the RDS instance response
        instance_response = {
            "DBInstances": [
                {
                    "Endpoint": {
                        "Address": "test-db.amazonaws.com",
                        "Port": 5432
                    }
                }
            ]
        }
        mock_rds_client.describe_db_instances.return_value = instance_response
        
        # Update options with instance identifier
        options = self.aws_options.copy()
        options["instance_identifier"] = "test-instance"
        del options["endpoint"]
        del options["port"]
        
        # Call the method under test
        connection_details = self.cloud_factory.create_connection(
            connection_id=self.aws_connection_id,
            cloud_provider=CloudConnectionFactory.AWS,
            db_type="postgres",
            credentials=self.aws_credentials,
            options=options
        )
        
        # Verify the results
        self.assertEqual(connection_details["cloud_provider"], CloudConnectionFactory.AWS)
        self.assertEqual(connection_details["db_type"], "postgres")
        
        # Verify RDS describe_db_instances was called
        mock_rds_client.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier="test-instance"
        )
        
        # Verify connection details have the endpoint from the RDS instance
        connection = connection_details["connection"]
        self.assertEqual(connection["endpoint"], "test-db.amazonaws.com")
        self.assertEqual(connection["port"], 5432)

    @mock.patch('azure.identity.ClientSecretCredential')
    @mock.patch('azure.mgmt.rdbms.postgresql.PostgreSQLManagementClient')
    def test_create_azure_connection_postgres(self, mock_postgres_client, mock_credential):
        """Test creating an Azure connection for PostgreSQL."""
        # Set up the mocks
        mock_credential_instance = mock.Mock()
        mock_credential.return_value = mock_credential_instance
        
        mock_client_instance = mock.Mock()
        mock_postgres_client.return_value = mock_client_instance
        
        # Call the method under test
        connection_details = self.cloud_factory.create_connection(
            connection_id=self.azure_connection_id,
            cloud_provider=CloudConnectionFactory.AZURE,
            db_type="postgres",
            credentials=self.azure_credentials,
            options=self.azure_options
        )
        
        # Verify the results
        self.assertEqual(connection_details["cloud_provider"], CloudConnectionFactory.AZURE)
        self.assertEqual(connection_details["db_type"], "postgres")
        
        # Verify credential was created with the right parameters
        mock_credential.assert_called_once_with(
            tenant_id=self.azure_credentials["tenant_id"],
            client_id=self.azure_credentials["client_id"],
            client_secret=self.azure_credentials["client_secret"]
        )
        
        # Verify management client was created
        mock_postgres_client.assert_called_once_with(
            credential=mock_credential_instance,
            subscription_id=self.azure_credentials["subscription_id"]
        )
        
        # Verify connection stored in factory
        self.assertIn(self.azure_connection_id, self.cloud_factory._connections)
        stored_connection = self.cloud_factory._connections[self.azure_connection_id]
        self.assertEqual(stored_connection["cloud_provider"], CloudConnectionFactory.AZURE)
        self.assertEqual(stored_connection["db_type"], "postgres")

    @mock.patch('azure.identity.DefaultAzureCredential')
    def test_create_azure_connection_with_default_credential(self, mock_default_credential):
        """Test creating an Azure connection with default credentials."""
        # Set up the mock
        mock_credential_instance = mock.Mock()
        mock_default_credential.return_value = mock_credential_instance
        
        # Update credentials to use default auth method
        credentials = {
            "auth_method": "default",
            "subscription_id": "test_subscription_id"
        }
        
        # Call the method under test with a patched import
        with mock.patch('azure.mgmt.rdbms.mysql.MySQLManagementClient'):
            connection_details = self.cloud_factory.create_connection(
                connection_id=self.azure_connection_id,
                cloud_provider=CloudConnectionFactory.AZURE,
                db_type="mysql",
                credentials=credentials,
                options=self.azure_options
            )
        
        # Verify the results
        self.assertEqual(connection_details["cloud_provider"], CloudConnectionFactory.AZURE)
        self.assertEqual(connection_details["db_type"], "mysql")
        
        # Verify default credential was created
        mock_default_credential.assert_called_once()

    @mock.patch('google.oauth2.service_account.Credentials')
    def test_create_gcp_connection_postgres(self, mock_credentials):
        """Test creating a GCP connection for PostgreSQL."""
        # Set up the mocks
        mock_credential_instance = mock.Mock()
        mock_credentials.from_service_account_info.return_value = mock_credential_instance
        
        # Use generic Mock instead of SqlInstancesServiceClient with create=True
        # This avoids the error trying to access google.cloud.sql_v1 which doesn't exist
        with mock.patch('google.cloud.sql_v1', create=True) as mock_sql_v1:
            mock_client_class = mock.Mock()
            mock_sql_v1.SqlInstancesServiceClient = mock_client_class
            
            mock_client_instance = mock.Mock()
            mock_client_class.return_value = mock_client_instance
            
            # Call the method under test
            connection_details = self.cloud_factory.create_connection(
                connection_id=self.gcp_connection_id,
                cloud_provider=CloudConnectionFactory.GCP,
                db_type="postgres",
                credentials=self.gcp_credentials,
                options=self.gcp_options
            )
            
            # Verify the results
            self.assertEqual(connection_details["cloud_provider"], CloudConnectionFactory.GCP)
            self.assertEqual(connection_details["db_type"], "postgres")
            
            # Verify credential was created with the right parameters
            mock_credentials.from_service_account_info.assert_called_once_with(
                self.gcp_credentials["service_account_info"]
            )
            
            # Verify connection stored in factory
            self.assertIn(self.gcp_connection_id, self.cloud_factory._connections)
            stored_connection = self.cloud_factory._connections[self.gcp_connection_id]
            self.assertEqual(stored_connection["cloud_provider"], CloudConnectionFactory.GCP)
            self.assertEqual(stored_connection["db_type"], "postgres")

    @mock.patch('google.oauth2.service_account.Credentials')
    def test_create_gcp_connection_with_service_account_file(self, mock_credentials):
        """Test creating a GCP connection with service account file."""
        # Set up the mock
        mock_credential_instance = mock.Mock()
        mock_credentials.from_service_account_file.return_value = mock_credential_instance
        
        # Update credentials to use service account file
        credentials = {
            "auth_method": "service_account",
            "project_id": "test-project",
            "service_account_file": "/path/to/service-account.json"
        }
        
        # Call the method under test with a patched import
        with mock.patch('google.cloud.sql_v1', create=True) as mock_sql_v1:
            mock_client_class = mock.Mock()
            mock_sql_v1.SqlInstancesServiceClient = mock_client_class
            
            mock_client_instance = mock.Mock()
            mock_client_class.return_value = mock_client_instance
            
            connection_details = self.cloud_factory.create_connection(
                connection_id=self.gcp_connection_id,
                cloud_provider=CloudConnectionFactory.GCP,
                db_type="mysql",
                credentials=credentials,
                options=self.gcp_options
            )
        
        # Verify the results
        self.assertEqual(connection_details["cloud_provider"], CloudConnectionFactory.GCP)
        self.assertEqual(connection_details["db_type"], "mysql")
        
        # Verify credential was created with the right parameters
        mock_credentials.from_service_account_file.assert_called_once_with(
            credentials["service_account_file"]
        )

    def test_create_connection_with_unsupported_provider(self):
        """Test creating a connection with an unsupported cloud provider."""
        with self.assertRaises(ValueError):
            self.cloud_factory.create_connection(
                connection_id="test_connection",
                cloud_provider="unsupported",
                db_type="postgres",
                credentials={},
                options={}
            )

    def test_create_connection_with_unsupported_db_type(self):
        """Test creating a connection with an unsupported database type."""
        with self.assertRaises(ValueError):
            self.cloud_factory.create_connection(
                connection_id="test_connection",
                cloud_provider=CloudConnectionFactory.AWS,
                db_type="unsupported",
                credentials=self.aws_credentials,
                options={}
            )

    def test_close_connection(self):
        """Test closing a cloud connection."""
        # Create a connection to close
        with mock.patch('boto3.Session'):
            self.cloud_factory.create_connection(
                connection_id=self.aws_connection_id,
                cloud_provider=CloudConnectionFactory.AWS,
                db_type="postgres",
                credentials=self.aws_credentials,
                options=self.aws_options
            )
        
        # Verify the connection exists
        self.assertIn(self.aws_connection_id, self.cloud_factory._connections)
        
        # Call the method under test
        self.cloud_factory.close_connection(self.aws_connection_id)
        
        # Verify the connection was removed
        self.assertNotIn(self.aws_connection_id, self.cloud_factory._connections)

    def test_close_nonexistent_connection(self):
        """Test closing a connection that doesn't exist."""
        with self.assertRaises(ValueError):
            self.cloud_factory.close_connection("nonexistent_connection")

    def test_list_connections(self):
        """Test listing all connections."""
        # Create some connections
        with mock.patch('boto3.Session'):
            self.cloud_factory.create_connection(
                connection_id="aws_postgres",
                cloud_provider=CloudConnectionFactory.AWS,
                db_type="postgres",
                credentials=self.aws_credentials,
                options=self.aws_options
            )
            
            self.cloud_factory.create_connection(
                connection_id="aws_mysql",
                cloud_provider=CloudConnectionFactory.AWS,
                db_type="mysql",
                credentials=self.aws_credentials,
                options=self.aws_options
            )
        
        # Call the method under test
        connections = self.cloud_factory.list_connections()
        
        # Verify the results
        self.assertEqual(len(connections), 2)
        
        # Verify the first connection
        conn1 = next((c for c in connections if c["connection_id"] == "aws_postgres"), None)
        self.assertIsNotNone(conn1)
        self.assertEqual(conn1["cloud_provider"], CloudConnectionFactory.AWS)
        self.assertEqual(conn1["db_type"], "postgres")
        
        # Verify the second connection
        conn2 = next((c for c in connections if c["connection_id"] == "aws_mysql"), None)
        self.assertIsNotNone(conn2)
        self.assertEqual(conn2["cloud_provider"], CloudConnectionFactory.AWS)
        self.assertEqual(conn2["db_type"], "mysql")

    def test_get_connection(self):
        """Test getting a specific connection."""
        # Create a connection
        with mock.patch('boto3.Session'):
            self.cloud_factory.create_connection(
                connection_id=self.aws_connection_id,
                cloud_provider=CloudConnectionFactory.AWS,
                db_type="postgres",
                credentials=self.aws_credentials,
                options=self.aws_options
            )
        
        # Call the method under test
        connection = self.cloud_factory.get_connection(self.aws_connection_id)
        
        # Verify the results
        self.assertEqual(connection["cloud_provider"], CloudConnectionFactory.AWS)
        self.assertEqual(connection["db_type"], "postgres")

    def test_get_nonexistent_connection(self):
        """Test getting a connection that doesn't exist."""
        with self.assertRaises(ValueError):
            self.cloud_factory.get_connection("nonexistent_connection")

    def test_close_all_connections(self):
        """Test closing all connections."""
        # Create some connections
        with mock.patch('boto3.Session'):
            self.cloud_factory.create_connection(
                connection_id="aws_postgres",
                cloud_provider=CloudConnectionFactory.AWS,
                db_type="postgres",
                credentials=self.aws_credentials,
                options=self.aws_options
            )
            
            self.cloud_factory.create_connection(
                connection_id="aws_mysql",
                cloud_provider=CloudConnectionFactory.AWS,
                db_type="mysql",
                credentials=self.aws_credentials,
                options=self.aws_options
            )
        
        # Verify the connections exist
        self.assertEqual(len(self.cloud_factory._connections), 2)
        
        # Call the method under test
        self.cloud_factory.close_all_connections()
        
        # Verify all connections were closed
        self.assertEqual(len(self.cloud_factory._connections), 0)

    def test_redact_sensitive_info(self):
        """Test redacting sensitive information from credentials."""
        # Create credentials with sensitive information
        credentials = {
            "api_key": "sensitive_api_key",
            "aws_secret_access_key": "sensitive_secret_key",
            "password": "sensitive_password",
            "region": "us-west-2",  # Not sensitive
            "project_id": "test-project"  # Not sensitive
        }
        
        # Call the method under test
        redacted = self.cloud_factory._redact_sensitive_info(credentials)
        
        # Verify sensitive information was redacted
        self.assertEqual(redacted["api_key"], "********")
        self.assertEqual(redacted["aws_secret_access_key"], "********")
        self.assertEqual(redacted["password"], "********")
        
        # Verify non-sensitive information was not redacted
        self.assertEqual(redacted["region"], "us-west-2")
        self.assertEqual(redacted["project_id"], "test-project")

    def test_get_required_packages(self):
        """Test getting the required packages for a cloud provider."""
        # Call the method under test for each provider
        aws_packages = self.cloud_factory._get_required_packages(CloudConnectionFactory.AWS)
        azure_packages = self.cloud_factory._get_required_packages(CloudConnectionFactory.AZURE)
        gcp_packages = self.cloud_factory._get_required_packages(CloudConnectionFactory.GCP)
        
        # Verify the results
        self.assertEqual(aws_packages, ["boto3"])
        self.assertEqual(azure_packages, ["azure-identity", "azure-mgmt-rdbms", "azure-mgmt-sql"])
        self.assertEqual(gcp_packages, ["google-cloud-sql", "google-auth"])
        
        # Verify for an unknown provider
        unknown_packages = self.cloud_factory._get_required_packages("unknown")
        self.assertEqual(unknown_packages, [])


if __name__ == '__main__':
    unittest.main()