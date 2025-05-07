"""
Tuning Fork - Database Performance Optimization Tool
End-to-end integration tests
"""

import os
import json
import tempfile
import unittest
import sqlite3
import shutil
import sys
import io

from tuningfork.core.config_manager import ConfigManager
from tuningfork.core.connection_manager import ConnectionManager
from tuningfork.core.cli_manager import CLIManager

class TestEndToEnd(unittest.TestCase):
    """End-to-end integration tests for the Tuning Fork tool."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a temporary directory for test files
        self.test_dir = tempfile.mkdtemp()
        
        # Create directory structure
        self.backup_dir = os.path.join(self.test_dir, "backups")
        self.report_dir = os.path.join(self.test_dir, "reports")
        os.makedirs(self.backup_dir, exist_ok=True)
        os.makedirs(self.report_dir, exist_ok=True)
        
        # Create a temporary configuration file
        self.config_file = os.path.join(self.test_dir, "config.json")
        with open(self.config_file, "w") as f:
            json.dump({
                "connections": {},
                "default_timeout": 30,
                "backup_directory": self.backup_dir,
                "report_directory": self.report_dir,
                "log_level": "INFO"
            }, f)
        
        # Create a temporary SQLite database
        self.db_file = os.path.join(self.test_dir, "test.db")
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        # Create test tables
        # Users table
        cursor.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Orders table
        cursor.execute("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        
        # Order items table
        cursor.execute("""
            CREATE TABLE order_items (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                product_name TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                price REAL NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id)
            )
        """)
        
        # Insert sample data
        # Users
        for i in range(1, 101):
            cursor.execute(
                "INSERT INTO users (username, email) VALUES (?, ?)",
                (f"user{i}", f"user{i}@example.com")
            )
        
        # Orders
        for i in range(1, 251):
            user_id = (i % 100) + 1  # Distribute orders among users
            status = "completed" if i % 4 != 0 else "pending"
            amount = i * 10.5
            cursor.execute(
                "INSERT INTO orders (user_id, amount, status) VALUES (?, ?, ?)",
                (user_id, amount, status)
            )
        
        # Order items
        for i in range(1, 501):
            order_id = (i % 250) + 1  # Distribute items among orders
            product_name = f"Product {(i % 20) + 1}"
            quantity = (i % 5) + 1
            price = (i % 10) * 5.25 + 10
            cursor.execute(
                "INSERT INTO order_items (order_id, product_name, quantity, price) VALUES (?, ?, ?, ?)",
                (order_id, product_name, quantity, price)
            )
        
        conn.commit()
        conn.close()
    
    def tearDown(self):
        """Clean up test fixtures after each test method."""
        # Remove the temporary directory and its contents
        shutil.rmtree(self.test_dir)
    
    def test_basic_workflow(self):
        """Test a basic workflow with the tool."""
        # Create instances of the managers
        config_manager = ConfigManager(self.config_file)
        connection_manager = ConnectionManager(config_manager)
        cli_manager = CLIManager(config_manager, connection_manager)
        
        try:
            # Capture stdout for CLI command outputs
            original_stdout = sys.stdout
            captured_output = io.StringIO()
            sys.stdout = captured_output
            
            try:
                # 1. Connect to the SQLite database
                result = cli_manager.run([
                    "--config", self.config_file,
                    "connect",
                    "--id", "test_db",
                    "--type", "sqlite",
                    "--database", self.db_file,
                    "--save"
                ])
                
                # Verify the connection was established
                self.assertEqual(result, 0)
                self.assertIn("test_db", connection_manager.list_connections())
                
                # Clear captured output
                captured_output.truncate(0)
                captured_output.seek(0)
                
                # 2. List active connections
                result = cli_manager.run([
                    "--config", self.config_file,
                    "list-connections"
                ])
                
                # Verify the command executed successfully
                self.assertEqual(result, 0)
                output = captured_output.getvalue()
                self.assertIn("test_db", output)
                self.assertIn("sqlite", output)
                
                # Clear captured output
                captured_output.truncate(0)
                captured_output.seek(0)
                
                # 3. Execute a query to get table counts
                result = cli_manager.run([
                    "--config", self.config_file,
                    "execute-query",
                    "--id", "test_db",
                    "--query", "SELECT 'users' AS table_name, COUNT(*) AS row_count FROM users UNION ALL SELECT 'orders', COUNT(*) FROM orders UNION ALL SELECT 'order_items', COUNT(*) FROM order_items"
                ])
                
                # Verify the query executed successfully
                self.assertEqual(result, 0)
                output = captured_output.getvalue()
                self.assertIn("users", output)
                self.assertIn("100", output)  # 100 users
                self.assertIn("orders", output)
                self.assertIn("250", output)  # 250 orders
                self.assertIn("order_items", output)
                self.assertIn("500", output)  # 500 order items
                
                # Clear captured output
                captured_output.truncate(0)
                captured_output.seek(0)
                
                # 4. Execute a query to analyze user-order relationships
                result = cli_manager.run([
                    "--config", self.config_file,
                    "execute-query",
                    "--id", "test_db",
                    "--query", "SELECT u.id, u.username, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.username ORDER BY total_amount DESC LIMIT 5"
                ])
                
                # Verify the query executed successfully
                self.assertEqual(result, 0)
                output = captured_output.getvalue()
                # Should show top 5 users by total order amount
                self.assertIn("order_count", output)
                self.assertIn("total_amount", output)
                
                # Clear captured output
                captured_output.truncate(0)
                captured_output.seek(0)
                
                # 5. Execute a query to analyze order statuses
                result = cli_manager.run([
                    "--config", self.config_file,
                    "execute-query",
                    "--id", "test_db",
                    "--query", "SELECT status, COUNT(*) AS count, AVG(amount) AS avg_amount FROM orders GROUP BY status"
                ])
                
                # Verify the query executed successfully
                self.assertEqual(result, 0)
                output = captured_output.getvalue()
                self.assertIn("completed", output)
                self.assertIn("pending", output)
                
                # Clear captured output
                captured_output.truncate(0)
                captured_output.seek(0)
                
                # 6. Execute a complex join query
                result = cli_manager.run([
                    "--config", self.config_file,
                    "execute-query",
                    "--id", "test_db",
                    "--query", "SELECT u.username, o.id AS order_id, o.amount, COUNT(oi.id) AS item_count, SUM(oi.quantity * oi.price) AS items_total FROM users u JOIN orders o ON u.id = o.user_id JOIN order_items oi ON o.id = oi.order_id GROUP BY u.username, o.id, o.amount ORDER BY o.amount DESC LIMIT 5"
                ])
                
                # Verify the query executed successfully
                self.assertEqual(result, 0)
                output = captured_output.getvalue()
                self.assertIn("username", output)
                self.assertIn("order_id", output)
                self.assertIn("amount", output)
                self.assertIn("item_count", output)
                
                # Clear captured output
                captured_output.truncate(0)
                captured_output.seek(0)
                
                # 7. Disconnect from the database
                result = cli_manager.run([
                    "--config", self.config_file,
                    "disconnect",
                    "--id", "test_db"
                ])
                
                # Verify the command executed successfully
                self.assertEqual(result, 0)
                self.assertNotIn("test_db", connection_manager.list_connections())
                
            finally:
                # Restore stdout
                sys.stdout = original_stdout
                
        finally:
            # Clean up
            connection_manager.disconnect_all()
    
    def test_config_persistence(self):
        """Test the configuration persistence."""
        # Create initial configuration
        config_manager = ConfigManager(self.config_file)
        
        # Set some values
        config_manager.set_value("test_key", "test_value")
        config_manager.set_value("nested.key", "nested_value")
        
        # Add a connection configuration (without actually connecting)
        connection_config = {
            "db_type": "sqlite",
            "host": "",
            "port": 0,
            "username": "",
            "database": self.db_file
        }
        config_manager.add_connection_config("test_connection", connection_config)
        
        # Save the configuration
        config_manager.save_config()
        
        # Create a new ConfigManager instance to load the saved configuration
        new_config_manager = ConfigManager(self.config_file)
        
        # Verify the values were persisted
        self.assertEqual(new_config_manager.get_value("test_key"), "test_value")
        self.assertEqual(new_config_manager.get_value("nested.key"), "nested_value")
        
        # Verify the connection configuration was persisted
        connections = new_config_manager.list_connections()
        self.assertIn("test_connection", connections)
        
        connection_config = new_config_manager.get_connection_config("test_connection")
        self.assertEqual(connection_config["db_type"], "sqlite")
        self.assertEqual(connection_config["database"], self.db_file)
    
    def test_reconnection(self):
        """Test reconnecting to a previously configured database."""
        # Create initial configuration
        config_manager = ConfigManager(self.config_file)
        connection_manager = ConnectionManager(config_manager)
        
        try:
            # Connect to the database
            success, error = connection_manager.connect(
                connection_id="test_db",
                db_type="sqlite",
                host="",
                port=0,
                username="",
                password="",
                database=self.db_file
            )
            
            # Verify the connection was established
            self.assertTrue(success, f"Failed to connect: {error}")
            self.assertIn("test_db", connection_manager.list_connections())
            
            # Save the configuration
            config_manager.save_config()
            
            # Disconnect
            success, error = connection_manager.disconnect("test_db")
            self.assertTrue(success, f"Failed to disconnect: {error}")
            
            # Create new instances
            new_config_manager = ConfigManager(self.config_file)
            new_connection_manager = ConnectionManager(new_config_manager)
            
            # Get the saved connection configuration
            connection_config = new_config_manager.get_connection_config("test_db")
            
            # Reconnect using the saved configuration
            success, error = new_connection_manager.connect(
                connection_id="test_db",
                db_type=connection_config["db_type"],
                host=connection_config.get("host", ""),
                port=connection_config.get("port", 0),
                username=connection_config.get("username", ""),
                password="",  # Password is not stored in config
                database=connection_config["database"]
            )
            
            # Verify the reconnection was successful
            self.assertTrue(success, f"Failed to reconnect: {error}")
            self.assertIn("test_db", new_connection_manager.list_connections())
            
            # Test the connection by executing a simple query
            success, result, error = new_connection_manager.execute_query(
                connection_id="test_db",
                query="SELECT COUNT(*) FROM users"
            )
            
            # Verify the query result
            self.assertTrue(success, f"Query failed: {error}")
            self.assertEqual(result[0][0], 100)  # 100 users
            
        finally:
            # Clean up
            connection_manager.disconnect_all()
            if 'new_connection_manager' in locals():
                new_connection_manager.disconnect_all()


if __name__ == "__main__":
    unittest.main()