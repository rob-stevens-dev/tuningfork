#!/usr/bin/env python3
"""
Verification script for Stage 1.2 (Enhanced Connection Management)

This script performs basic validation of all Stage 1.2 components to ensure 
they are properly implemented and ready for integration with the rest of the system.

Usage: python verify_stage_1_2.py
"""

import importlib
import logging
import os
import sys
import unittest
from unittest import mock

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("verification")

def check_imports():
    """Verify that all required modules can be imported."""
    logger.info("Checking imports...")
    
    # Add the project root directory to the Python path
    # This assumes the verification script is running from scripts/ directory
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, project_root)
    
    try:
        # Try importing from the tuningfork module structure
        from tuningfork.connection import ssh_manager, cloud_connection_factory, connection_manager
        logger.info(f"✓ Successfully imported modules from tuningfork.connection package")
        return True
    except ImportError as e:
        logger.warning(f"Could not import from tuningfork.connection package: {str(e)}")
        
        # Attempt to import from direct file paths
        module_paths = [
            os.path.join(project_root, "tuningfork", "connection"),
            os.path.join(project_root, "src", "tuningfork", "connection"),
            os.path.join(project_root, "src"),
            os.path.join(project_root),
        ]
        
        for path in module_paths:
            if os.path.exists(path):
                logger.info(f"Found potential module path: {path}")
                sys.path.insert(0, path)
        
        # Now try to import the modules directly
        required_modules = [
            "ssh_manager", 
            "cloud_connection_factory", 
            "connection_manager"
        ]
        
        missing_modules = []
        for module in required_modules:
            try:
                importlib.import_module(module)
                logger.info(f"✓ Successfully imported {module}")
            except ImportError as e:
                missing_modules.append(module)
                logger.error(f"✗ Failed to import {module}: {str(e)}")
        
        if missing_modules:
            logger.error(f"Missing modules: {', '.join(missing_modules)}")
            logger.error("Please ensure all modules are in the correct directory or PYTHONPATH")
            logger.error("\nYour current Python path is:")
            for p in sys.path:
                logger.error(f"  - {p}")
            logger.error("\nPlease ensure your module files are in one of these locations or adjust your PYTHONPATH")
            logger.error("\nExpected file locations:")
            logger.error(f"  - {os.path.join(project_root, 'tuningfork', 'connection', 'ssh_manager.py')}")
            logger.error(f"  - {os.path.join(project_root, 'tuningfork', 'connection', 'cloud_connection_factory.py')}")
            logger.error(f"  - {os.path.join(project_root, 'tuningfork', 'connection', 'connection_manager.py')}")
            return False
        
        return True

def verify_ssh_manager():
    """Verify SSH Manager functionality."""
    logger.info("Verifying SSH Manager...")
    
    try:
        # Try different import approaches
        try:
            from tuningfork.connection.ssh_manager import SSHManager
        except ImportError:
            try:
                import sys
                logger.info("Attempting to import SSHManager using sys.modules...")
                if 'ssh_manager' in sys.modules:
                    SSHManager = sys.modules['ssh_manager'].SSHManager
                else:
                    from ssh_manager import SSHManager
            except ImportError:
                # Last resort - dynamically load the module from the file
                logger.info("Attempting to import SSHManager directly from file...")
                import importlib.util
                # Try to find the ssh_manager.py file
                for root, dirs, files in os.walk(os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))):
                    if 'ssh_manager.py' in files:
                        logger.info(f"Found ssh_manager.py at {root}")
                        module_path = os.path.join(root, 'ssh_manager.py')
                        spec = importlib.util.spec_from_file_location("ssh_manager", module_path)
                        ssh_manager_module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(ssh_manager_module)
                        SSHManager = ssh_manager_module.SSHManager
                        break
        
        # Create an instance
        ssh_manager = SSHManager()
        logger.info("✓ Successfully created SSHManager instance")
        
        # Mock testing to verify core functionality
        with mock.patch('paramiko.SSHClient') as mock_ssh_client:
            mock_instance = mock.Mock()
            mock_ssh_client.return_value = mock_instance
            
            mock_transport = mock.Mock()
            mock_instance.get_transport.return_value = mock_transport
            
            # Use a mock socket to bypass actual connection
            with mock.patch('socket.create_connection'):
                # Create a tunnel
                local_host, local_port = ssh_manager.create_tunnel(
                    tunnel_id="test_tunnel",
                    ssh_host="example.com",
                    ssh_port=22,
                    ssh_username="test_user",
                    ssh_password="test_password",
                    remote_host="db.example.com",
                    remote_port=5432,
                    local_port=10000,
                    timeout=1
                )
                
                logger.info("✓ Successfully created SSH tunnel")
                
                # List tunnels
                tunnels = ssh_manager.list_tunnels()
                if len(tunnels) == 1 and tunnels[0]["tunnel_id"] == "test_tunnel":
                    logger.info("✓ Successfully listed SSH tunnels")
                else:
                    logger.error("✗ Failed to list SSH tunnels")
                    return False
                
                # Check tunnel status
                is_active = ssh_manager.is_tunnel_active("test_tunnel")
                logger.info(f"✓ Successfully checked tunnel status: {is_active}")
                
                # Close tunnel
                ssh_manager.close_tunnel("test_tunnel")
                logger.info("✓ Successfully closed SSH tunnel")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to verify SSH Manager: {str(e)}")
        logger.error(f"Exception details: {type(e).__name__}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def verify_cloud_connection_factory():
    """Verify Cloud Connection Factory functionality."""
    logger.info("Verifying Cloud Connection Factory...")
    
    try:
        # Try different import approaches
        try:
            from tuningfork.connection.cloud_connection_factory import CloudConnectionFactory
        except ImportError:
            try:
                import sys
                logger.info("Attempting to import CloudConnectionFactory using sys.modules...")
                if 'cloud_connection_factory' in sys.modules:
                    CloudConnectionFactory = sys.modules['cloud_connection_factory'].CloudConnectionFactory
                else:
                    from cloud_connection_factory import CloudConnectionFactory
            except ImportError:
                # Last resort - dynamically load the module from the file
                logger.info("Attempting to import CloudConnectionFactory directly from file...")
                import importlib.util
                # Try to find the cloud_connection_factory.py file
                for root, dirs, files in os.walk(os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))):
                    if 'cloud_connection_factory.py' in files:
                        logger.info(f"Found cloud_connection_factory.py at {root}")
                        module_path = os.path.join(root, 'cloud_connection_factory.py')
                        spec = importlib.util.spec_from_file_location("cloud_connection_factory", module_path)
                        cloud_factory_module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(cloud_factory_module)
                        CloudConnectionFactory = cloud_factory_module.CloudConnectionFactory
                        break
        
        # Create an instance
        cloud_factory = CloudConnectionFactory()
        logger.info("✓ Successfully created CloudConnectionFactory instance")
        
        # Mock AWS for testing
        with mock.patch('boto3.Session') as mock_boto3_session:
            mock_session = mock.Mock()
            mock_boto3_session.return_value = mock_session
            
            mock_rds_client = mock.Mock()
            mock_session.client.return_value = mock_rds_client
            
            # Create a connection
            cloud_factory.create_connection(
                connection_id="test_aws",
                cloud_provider=CloudConnectionFactory.AWS,
                db_type="postgres",
                credentials={
                    "region": "us-west-2",
                    "aws_access_key_id": "test_key",
                    "aws_secret_access_key": "test_secret"
                },
                options={
                    "endpoint": "test-db.amazonaws.com",
                    "port": 5432,
                    "database": "test_db",
                    "user": "test_user",
                    "password": "test_password"
                }
            )
            
            logger.info("✓ Successfully created AWS connection")
            
            # List connections
            connections = cloud_factory.list_connections()
            if len(connections) == 1 and connections[0]["connection_id"] == "test_aws":
                logger.info("✓ Successfully listed cloud connections")
            else:
                logger.error("✗ Failed to list cloud connections")
                return False
            
            # Get connection
            connection = cloud_factory.get_connection("test_aws")
            if connection["cloud_provider"] == CloudConnectionFactory.AWS and connection["db_type"] == "postgres":
                logger.info("✓ Successfully retrieved cloud connection")
            else:
                logger.error("✗ Failed to retrieve cloud connection")
                return False
            
            # Close connection
            cloud_factory.close_connection("test_aws")
            logger.info("✓ Successfully closed cloud connection")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to verify Cloud Connection Factory: {str(e)}")
        logger.error(f"Exception details: {type(e).__name__}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def run_unit_tests():
    """Run unit tests for Stage 1.2 components."""
    logger.info("Running unit tests...")
    
    # Modified to look in the correct location for test files
    test_modules = [
        "tests.connection.test_ssh_manager",
        "tests.connection.test_cloud_connection_factory",
        "tests.connection.test_connection_manager"
    ]
    
    success = True
    for module in test_modules:
        try:
            logger.info(f"Running tests from {module}...")
            test_module = importlib.import_module(module)
            suite = unittest.TestLoader().loadTestsFromModule(test_module)
            result = unittest.TextTestRunner(verbosity=2).run(suite)
            
            if result.wasSuccessful():
                logger.info(f"✓ All tests passed for {module}")
            else:
                logger.error(f"✗ Tests failed for {module}")
                success = False
                
        except ImportError as e:
            logger.error(f"✗ Failed to import test module {module}: {str(e)}")
            success = False
    
    return success

def verify_connection_manager():
    """Verify Connection Manager functionality."""
    logger.info("Verifying Connection Manager...")
    
    try:
        # Import from the correct module path
        from tuningfork.connection.connection_manager import ConnectionManager
        
        # Create an instance
        connection_manager = ConnectionManager()
        logger.info("✓ Successfully created ConnectionManager instance")
        
        # Mock database modules
        with mock.patch('psycopg2.connect') as mock_pg_connect:
            mock_connection = mock.Mock()
            mock_pg_connect.return_value = mock_connection
            mock_cursor = mock.Mock()
            mock_connection.cursor.return_value = mock_cursor
            
            # Create a connection
            connection_id = connection_manager.connect(
                db_type="postgres",
                host="localhost",
                port=5432,
                username="test_user",
                password="test_password",
                database="test_db"
            )
            
            logger.info(f"✓ Successfully created database connection: {connection_id}")
            
            # List connections
            connections = connection_manager.list_connections()
            if len(connections) == 1 and connections[0]["connection_id"] == connection_id:
                logger.info("✓ Successfully listed database connections")
            else:
                logger.error("✗ Failed to list database connections")
                return False
            
            # Execute a query
            connection_manager.execute_query(
                connection_id=connection_id,
                query="SELECT 1",
                fetchall=True
            )
            
            logger.info("✓ Successfully executed database query")
            
            # Disconnect
            connection_manager.disconnect(connection_id)
            logger.info("✓ Successfully closed database connection")
        
        # Verify connection pooling
        with mock.patch('psycopg2.pool.ThreadedConnectionPool') as mock_pool:
            mock_pool_instance = mock.Mock()
            mock_pool.return_value = mock_pool_instance
            
            # Create a connection pool
            pool_id = connection_manager.create_connection_pool(
                pool_id="test_pool",
                db_type="postgres",
                host="localhost",
                port=5432,
                username="test_user",
                password="test_password",
                database="test_db",
                min_connections=1,
                max_connections=5
            )
            
            logger.info(f"✓ Successfully created connection pool: {pool_id}")
            
            # List pools
            pools = connection_manager.list_connection_pools()
            if len(pools) == 1 and pools[0]["pool_id"] == pool_id:
                logger.info("✓ Successfully listed connection pools")
            else:
                logger.error("✗ Failed to list connection pools")
                return False
            
            # Close pool
            connection_manager.close_connection_pool(pool_id)
            logger.info("✓ Successfully closed connection pool")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to verify Connection Manager: {str(e)}")
        logger.error(f"Exception details: {type(e).__name__}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def verify_dependencies():
    """Verify that all external dependencies are installed."""
    logger.info("Verifying dependencies...")
    
    required_packages = [
        "paramiko",         # For SSH tunneling
        "boto3",            # For AWS
        "psycopg2",         # For PostgreSQL
        "mysql-connector-python",  # For MySQL
        "pyodbc"            # For SQL Server
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            importlib.import_module(package.split('-')[0])  # Handle hyphenated package names
            logger.info(f"✓ Successfully imported {package}")
        except ImportError:
            missing_packages.append(package)
            logger.warning(f"✗ Package {package} is not installed")
    
    if missing_packages:
        logger.warning(f"Missing packages: {', '.join(missing_packages)}")
        logger.info("You can install missing packages with:")
        logger.info(f"pip install {' '.join(missing_packages)}")
        logger.info("Note: Some packages may be optional depending on your use case")
    
    # Return True even if some packages are missing, as they might be optional
    return True

def main():
    """Main verification function."""
    logger.info("Starting Stage 1.2 verification")
    
    all_checks_passed = True
    
    # Check imports
    if not check_imports():
        logger.error("Import check failed. Please fix these issues before continuing.")
        return False
    
    # Verify dependencies
    verify_dependencies()  # Just informational, doesn't affect overall result
    
    # Verify SSH Manager
    if not verify_ssh_manager():
        logger.error("SSH Manager verification failed.")
        all_checks_passed = False
    
    # Verify Cloud Connection Factory
    if not verify_cloud_connection_factory():
        logger.error("Cloud Connection Factory verification failed.")
        all_checks_passed = False
    
    # Verify Connection Manager
    if not verify_connection_manager():
        logger.error("Connection Manager verification failed.")
        all_checks_passed = False
    
    # Run unit tests
    if not run_unit_tests():
        logger.error("Unit tests failed.")
        all_checks_passed = False
    
    if all_checks_passed:
        logger.info("==================================================")
        logger.info("✓ All verification checks passed!")
        logger.info("Stage 1.2 is complete and ready for integration.")
        logger.info("==================================================")
        return True
    else:
        logger.error("==================================================")
        logger.error("✗ Verification failed. Please address the issues above.")
        logger.error("==================================================")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)