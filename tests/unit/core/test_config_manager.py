"""
Tuning Fork - Database Performance Optimization Tool
Unit tests for ConfigManager
"""

import os
import json
import tempfile
import unittest
from tuningfork.core.config_manager import ConfigManager

class TestConfigManager(unittest.TestCase):
    """Test cases for the ConfigManager class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a temporary file for testing
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
        self.temp_file.close()
        
        # Sample configuration
        self.sample_config = {
            "connections": {
                "test_postgres": {
                    "db_type": "postgres",
                    "host": "localhost",
                    "port": 5432,
                    "username": "test_user",
                    "database": "test_db"
                }
            },
            "default_timeout": 30,
            "backup_directory": "./backups",
            "report_directory": "./reports",
            "log_level": "INFO"
        }
        
        # Write sample configuration to the temporary file
        with open(self.temp_file.name, 'w') as f:
            json.dump(self.sample_config, f)
    
    def tearDown(self):
        """Clean up test fixtures after each test method."""
        # Remove the temporary file
        os.unlink(self.temp_file.name)
    
    def test_init_without_file(self):
        """Test initialization without a configuration file."""
        config_manager = ConfigManager()
        self.assertIsNotNone(config_manager.config)
        self.assertIn("connections", config_manager.config)
        self.assertIn("default_timeout", config_manager.config)
    
    def test_init_with_file(self):
        """Test initialization with a configuration file."""
        config_manager = ConfigManager(self.temp_file.name)
        self.assertEqual(config_manager.config, self.sample_config)
    
    def test_load_config(self):
        """Test loading configuration from a file."""
        config_manager = ConfigManager()
        config_manager.load_config(self.temp_file.name)
        self.assertEqual(config_manager.config, self.sample_config)
    
    def test_load_config_file_not_found(self):
        """Test loading configuration from a non-existent file."""
        config_manager = ConfigManager()
        with self.assertRaises(FileNotFoundError):
            config_manager.load_config("non_existent_file.json")
    
    def test_load_config_invalid_json(self):
        """Test loading configuration from an invalid JSON file."""
        # Create a file with invalid JSON
        invalid_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
        invalid_file.write(b"This is not valid JSON")
        invalid_file.close()
        
        config_manager = ConfigManager()
        with self.assertRaises(json.JSONDecodeError):
            config_manager.load_config(invalid_file.name)
        
        # Clean up
        os.unlink(invalid_file.name)
    
    def test_save_config(self):
        """Test saving configuration to a file."""
        config_manager = ConfigManager()
        config_manager.config = self.sample_config
        
        # Save to a new file
        new_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
        new_file.close()
        
        config_manager.save_config(new_file.name)
        
        # Read the saved file
        with open(new_file.name, 'r') as f:
            saved_config = json.load(f)
        
        self.assertEqual(saved_config, self.sample_config)
        
        # Clean up
        os.unlink(new_file.name)
    
    def test_get_value(self):
        """Test getting configuration values."""
        config_manager = ConfigManager(self.temp_file.name)
        
        # Test simple key
        self.assertEqual(config_manager.get_value("default_timeout"), 30)
        
        # Test nested key with dot notation
        self.assertEqual(
            config_manager.get_value("connections.test_postgres.host"), 
            "localhost"
        )
        
        # Test with default value
        self.assertEqual(
            config_manager.get_value("non_existent_key", "default"), 
            "default"
        )
    
    def test_set_value(self):
        """Test setting configuration values."""
        config_manager = ConfigManager()
        
        # Test simple key
        config_manager.set_value("new_key", "value")
        self.assertEqual(config_manager.get_value("new_key"), "value")
        
        # Test nested key with dot notation
        config_manager.set_value("nested.key", "nested_value")
        self.assertEqual(config_manager.get_value("nested.key"), "nested_value")
        
        # Test updating existing value
        config_manager.set_value("new_key", "updated_value")
        self.assertEqual(config_manager.get_value("new_key"), "updated_value")
    
    def test_get_connection_config(self):
        """Test getting connection configuration."""
        config_manager = ConfigManager(self.temp_file.name)
        
        # Test existing connection
        connection_config = config_manager.get_connection_config("test_postgres")
        self.assertEqual(connection_config["host"], "localhost")
        self.assertEqual(connection_config["port"], 5432)
        
        # Test non-existent connection
        with self.assertRaises(KeyError):
            config_manager.get_connection_config("non_existent_connection")
    
    def test_add_connection_config(self):
        """Test adding connection configuration."""
        config_manager = ConfigManager()
        
        # Add a new connection
        new_connection = {
            "db_type": "mysql",
            "host": "localhost",
            "port": 3306,
            "username": "mysql_user",
            "database": "mysql_db"
        }
        config_manager.add_connection_config("test_mysql", new_connection)
        
        # Verify the connection was added
        self.assertEqual(
            config_manager.get_connection_config("test_mysql"),
            new_connection
        )
    
    def test_remove_connection_config(self):
        """Test removing connection configuration."""
        config_manager = ConfigManager(self.temp_file.name)
        
        # Verify the connection exists
        self.assertIn("test_postgres", config_manager.list_connections())
        
        # Remove the connection
        config_manager.remove_connection_config("test_postgres")
        
        # Verify the connection was removed
        with self.assertRaises(KeyError):
            config_manager.get_connection_config("test_postgres")
        
        # Test removing non-existent connection
        with self.assertRaises(KeyError):
            config_manager.remove_connection_config("non_existent_connection")
    
    def test_list_connections(self):
        """Test listing connections."""
        config_manager = ConfigManager(self.temp_file.name)
        
        # Verify the connection list
        connections = config_manager.list_connections()
        self.assertIn("test_postgres", connections)
        self.assertEqual(len(connections), 1)
        
        # Add a new connection
        new_connection = {
            "db_type": "mysql",
            "host": "localhost",
            "port": 3306,
            "username": "mysql_user",
            "database": "mysql_db"
        }
        config_manager.add_connection_config("test_mysql", new_connection)
        
        # Verify the updated connection list
        connections = config_manager.list_connections()
        self.assertIn("test_postgres", connections)
        self.assertIn("test_mysql", connections)
        self.assertEqual(len(connections), 2)


if __name__ == "__main__":
    unittest.main()