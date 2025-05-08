"""
Verification Test for Stage 2.1 of TuningFork.

This test ensures that all components of Stage 2.1 (Resource Analyzer) are correctly
implemented and working together as expected. It serves as a checkpoint before
moving to the next stage.
"""

import os
import sys
import json
import unittest
from pathlib import Path
import tempfile
import shutil
import importlib
import time

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# First, verify all required modules can be imported
try:
    from tuningfork.analyzers.resource_analyzer import (
        ResourceAnalyzer,
        PostgreSQLResourceAnalyzer,
        MySQLResourceAnalyzer,
        MSSQLResourceAnalyzer,
        SQLiteResourceAnalyzer,
        DBResourceAnalyzer
    )
    from tuningfork.analyzers.base_analyzer import BaseAnalyzer
    from tuningfork.connection.connection_manager import ConnectionManager, Connection
    from tuningfork.core.config_manager import ConfigManager
    from tuningfork.models.recommendation import Recommendation, RecommendationPriority, RecommendationType
    from tuningfork.util.exceptions import ResourceAnalysisError, ConnectionError, ConfigurationError, TuningForkError
        
    # All required imports successful
    import_success = True
except ImportError as e:
    import_success = False
    import_error = str(e)


class TestStage21Verification(unittest.TestCase):
    """Verification test for Stage 2.1 of TuningFork."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        # Check if imports were successful
        cls.import_success = import_success
        if not import_success:
            return
        
        # Create temporary directory for test files
        cls.test_dir = tempfile.mkdtemp()
        
        # Create configuration file
        cls.config_file = os.path.join(cls.test_dir, "config.json")
        with open(cls.config_file, "w") as f:
            json.dump({
                "storage_directory": cls.test_dir,
                "connections": {
                    "test_sqlite": {
                        "type": "sqlite",
                        "database": ":memory:",
                        "timeout": 30
                    }
                }
            }, f, indent=2)
        
        # Initialize components
        cls.config_manager = ConfigManager(cls.config_file)
        cls.connection_manager = ConnectionManager(cls.config_manager)
        cls.resource_analyzer = ResourceAnalyzer(cls.connection_manager, cls.config_manager)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after tests."""
        if hasattr(cls, 'connection_manager'):
            cls.connection_manager.close_all_connections()
        
        # Remove temporary directory
        if hasattr(cls, 'test_dir') and os.path.exists(cls.test_dir):
            shutil.rmtree(cls.test_dir)
    
    def setUp(self):
        """Set up each test case."""
        if not self.import_success:
            self.skipTest(f"Import failed: {import_error}")
        
        self.connection_id = "test_sqlite"
        
        # Connect to SQLite
        if self.connection_id not in self.connection_manager.connections:
            self.connection_manager.connect(self.connection_id)
        
        # Setup test database
        connection = self.connection_manager.get_connection(self.connection_id)
        cursor = connection.cursor()
        
        # Create test table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        """)
        
        # Insert some test data
        cursor.execute("DELETE FROM test_table")
        cursor.executemany(
            "INSERT INTO test_table (name, value) VALUES (?, ?)",
            [("item1", 100), ("item2", 200), ("item3", 300)]
        )
        
        connection.commit()
        cursor.close()
    
    def test_01_module_structure(self):
        """Verify module structure and imports."""
        # If we got here, imports were successful
        self.assertTrue(self.import_success, "All required modules should be importable")
        
        # Check inheritance hierarchy
        self.assertTrue(issubclass(ResourceAnalyzer, BaseAnalyzer), 
                        "ResourceAnalyzer should inherit from BaseAnalyzer")
        
        self.assertTrue(issubclass(PostgreSQLResourceAnalyzer, DBResourceAnalyzer),
                        "PostgreSQLResourceAnalyzer should inherit from DBResourceAnalyzer")
        
        self.assertTrue(issubclass(MySQLResourceAnalyzer, DBResourceAnalyzer),
                        "MySQLResourceAnalyzer should inherit from DBResourceAnalyzer")
        
        self.assertTrue(issubclass(MSSQLResourceAnalyzer, DBResourceAnalyzer),
                        "MSSQLResourceAnalyzer should inherit from DBResourceAnalyzer")
        
        self.assertTrue(issubclass(SQLiteResourceAnalyzer, DBResourceAnalyzer),
                        "SQLiteResourceAnalyzer should inherit from DBResourceAnalyzer")
    
    def test_02_connection_management(self):
        """Verify connection management works correctly."""
        # Check connection was created
        connection = self.connection_manager.get_connection(self.connection_id)
        self.assertIsNotNone(connection, "Connection should be created successfully")
        
        # Check connection properties
        self.assertEqual(connection.connection_id, self.connection_id)
        self.assertEqual(connection.db_type, "sqlite")
        self.assertTrue(connection.is_connected(), "Connection should be active")
        
        # Test closing connection
        result = self.connection_manager.close_connection(self.connection_id)
        self.assertTrue(result, "Closing connection should return True")
        
        # Connection should no longer be available
        connection = self.connection_manager.get_connection(self.connection_id)
        self.assertIsNone(connection, "Connection should be None after closing")
        
        # Reconnect for further tests
        connection = self.connection_manager.connect(self.connection_id)
        self.assertIsNotNone(connection, "Should be able to reconnect")
    
    def test_03_analyze_resources(self):
        """Verify resource analysis functionality."""
        resource_data = self.resource_analyzer.analyze_resources(self.connection_id)
        
        # Check basic structure
        self.assertIsInstance(resource_data, dict, "Resource data should be a dictionary")
        self.assertIn("system", resource_data, "Resource data should contain system info")
        self.assertIn("sqlite", resource_data, "Resource data should contain database-specific info")
        
        # Check system info
        system_info = resource_data["system"]
        self.assertIn("physical_cores", system_info)
        self.assertIn("memory_total", system_info)
        self.assertIn("disk_partitions", system_info)
        
        # Check SQLite info
        sqlite_info = resource_data["sqlite"]
        self.assertIn("version", sqlite_info)
        self.assertIn("database_info", sqlite_info)
        self.assertIn("tables", sqlite_info)
        
        # Check table detection
        tables = sqlite_info["tables"]
        self.assertTrue(len(tables) > 0, "Should detect at least one table")
        
        # Find our test table
        test_table = next((t for t in tables if t["name"] == "test_table"), None)
        self.assertIsNotNone(test_table, "Should detect our test table")
        self.assertEqual(test_table["row_count"], 3, "Should detect correct row count")
    
    def test_04_analyze_configuration(self):
        """Verify configuration analysis functionality."""
        config_data = self.resource_analyzer.analyze_configuration(self.connection_id)
        
        # Check basic structure
        self.assertIsInstance(config_data, dict, "Configuration data should be a dictionary")
        self.assertIn("pragmas", config_data, "Config data should contain pragmas for SQLite")
        
        # Check pragmas
        pragmas = config_data["pragmas"]
        self.assertIn("journal_mode", pragmas, "Should detect journal_mode")
        self.assertIn("synchronous", pragmas, "Should detect synchronous setting")
        self.assertIn("cache_size", pragmas, "Should detect cache_size")
    
    def test_05_monitor_resource_utilization(self):
        """Verify resource utilization monitoring functionality."""
        # Use a short duration for testing
        utilization_data = self.resource_analyzer.monitor_resource_utilization(
            self.connection_id, duration=2, interval=1
        )
        
        # Check basic structure
        self.assertIsInstance(utilization_data, dict, "Utilization data should be a dictionary")
        self.assertIn("system", utilization_data, "Should include system utilization")
        self.assertIn("sqlite", utilization_data, "Should include database-specific utilization")
        
        # Check system utilization
        system_util = utilization_data["system"]
        self.assertIn("cpu", system_util, "Should monitor CPU")
        self.assertIn("memory", system_util, "Should monitor memory")
        self.assertIn("disk_io", system_util, "Should monitor disk I/O")
        self.assertIn("network_io", system_util, "Should monitor network I/O")
        
        # Check measurements
        self.assertTrue(len(system_util["cpu"]) > 0, "Should have CPU measurements")
        self.assertTrue(len(system_util["memory"]) > 0, "Should have memory measurements")
        
        # Each measurement should have a timestamp
        for measurement in system_util["cpu"]:
            self.assertIn("timestamp", measurement, "Each measurement should have a timestamp")
            self.assertIn("overall_percent", measurement, "CPU measurement should include overall percent")
    
    def test_06_generate_recommendations(self):
        """Verify recommendation generation functionality."""
        # First, collect all the necessary data
        self.resource_analyzer.analyze_resources(self.connection_id)
        self.resource_analyzer.analyze_configuration(self.connection_id)
        self.resource_analyzer.monitor_resource_utilization(self.connection_id, duration=2, interval=1)
        
        # Generate recommendations
        recommendations = self.resource_analyzer.generate_resource_recommendations(self.connection_id)
        
        # Check recommendations
        self.assertIsInstance(recommendations, list, "Recommendations should be a list")
        self.assertTrue(len(recommendations) > 0, "Should generate at least one recommendation")
        
        # Check first recommendation
        first_rec = recommendations[0]
        self.assertIsInstance(first_rec, Recommendation, "Should return Recommendation objects")
        self.assertTrue(first_rec.title, "Recommendation should have a title")
        self.assertTrue(first_rec.description, "Recommendation should have a description")
        self.assertIsInstance(first_rec.priority, RecommendationPriority, "Should have valid priority")
        self.assertIsInstance(first_rec.type, RecommendationType, "Should have valid type")
        self.assertTrue(first_rec.implementation_script, "Should have implementation script")
    
    def test_07_data_persistence(self):
        """Verify data persistence functionality."""
        # First, collect and generate data
        self.resource_analyzer.analyze_resources(self.connection_id)
        self.resource_analyzer.analyze_configuration(self.connection_id)
        self.resource_analyzer.monitor_resource_utilization(self.connection_id, duration=2, interval=1)
        original_recommendations = self.resource_analyzer.generate_resource_recommendations(self.connection_id)
        
        # Save analysis data
        output_file = os.path.join(self.test_dir, "analysis_data.json")
        self.resource_analyzer.save_analysis_data(self.connection_id, output_file)
        
        # Check file was created
        self.assertTrue(os.path.exists(output_file), "Analysis data file should be created")
        
        # Load and verify
        with open(output_file, "r") as f:
            saved_data = json.load(f)
        
        self.assertEqual(saved_data["connection_id"], self.connection_id, "Should save correct connection ID")
        self.assertIn("resource_data", saved_data, "Should save resource data")
        self.assertIn("config_data", saved_data, "Should save config data")
        self.assertIn("utilization_data", saved_data, "Should save utilization data")
        
        # Check recommendation storage and retrieval
        new_analyzer = ResourceAnalyzer(self.connection_manager, self.config_manager)
        loaded_recs = new_analyzer.get_recommendations(self.connection_id)
        
        self.assertEqual(len(loaded_recs), len(original_recommendations), "Should load same number of recommendations")
        self.assertEqual(loaded_recs[0].id, original_recommendations[0].id, "Should load same recommendations")
    
    def test_08_error_handling(self):
        """Verify error handling functionality."""
        # Test with nonexistent connection
        with self.assertRaises(ResourceAnalysisError):
            self.resource_analyzer.analyze_resources("nonexistent_connection")
        
        # Test generating recommendations without data
        with self.assertRaises(ResourceAnalysisError):
            # Use a new connection ID that hasn't been analyzed
            new_connection_id = "test_error_handling"
            self.resource_analyzer.generate_resource_recommendations(new_connection_id)
        
        # Test saving data without analysis
        with self.assertRaises(ResourceAnalysisError):
            self.resource_analyzer.save_analysis_data("nonexistent_connection", "test_output.json")
    
    def test_09_database_type_detection(self):
        """Verify database type detection and appropriate analyzer selection."""
        # Get private method using name mangling workaround
        method = getattr(self.resource_analyzer, "_get_resource_analyzer_for_db_type")
        
        # Test each database type
        pg_analyzer = method("postgresql")
        self.assertIsInstance(pg_analyzer, PostgreSQLResourceAnalyzer, "Should return PostgreSQL analyzer")
        
        mysql_analyzer = method("mysql")
        self.assertIsInstance(mysql_analyzer, MySQLResourceAnalyzer, "Should return MySQL analyzer")
        
        mssql_analyzer = method("mssql")
        self.assertIsInstance(mssql_analyzer, MSSQLResourceAnalyzer, "Should return MSSQL analyzer")
        
        sqlite_analyzer = method("sqlite")
        self.assertIsInstance(sqlite_analyzer, SQLiteResourceAnalyzer, "Should return SQLite analyzer")
        
        # Test case insensitivity
        pg_analyzer2 = method("PostgreSQL")
        self.assertIsInstance(pg_analyzer2, PostgreSQLResourceAnalyzer, "Should handle case-insensitive type")
        
        # Test invalid type
        with self.assertRaises(ValueError):
            method("invalid_type")
    
    def test_10_full_workflow(self):
        """Verify the entire workflow from connection to recommendations."""
        # This test simulates the typical usage pattern
        
        # Step 1: Connect to database
        connection = self.connection_manager.connect(self.connection_id)
        self.assertTrue(connection.is_connected(), "Connection should be active")
        
        # Step 2: Analyze resources
        resource_data = self.resource_analyzer.analyze_resources(self.connection_id)
        self.assertIsNotNone(resource_data, "Should get resource data")
        
        # Step 3: Analyze configuration
        config_data = self.resource_analyzer.analyze_configuration(self.connection_id)
        self.assertIsNotNone(config_data, "Should get config data")
        
        # Step 4: Monitor resource utilization (short duration for test)
        utilization_data = self.resource_analyzer.monitor_resource_utilization(
            self.connection_id, duration=2, interval=1
        )
        self.assertIsNotNone(utilization_data, "Should get utilization data")
        
        # Step 5: Generate recommendations
        recommendations = self.resource_analyzer.generate_resource_recommendations(self.connection_id)
        self.assertTrue(len(recommendations) > 0, "Should generate recommendations")
        
        # Step 6: Save analysis data
        output_file = os.path.join(self.test_dir, "full_workflow_analysis.json")
        self.resource_analyzer.save_analysis_data(self.connection_id, output_file)
        self.assertTrue(os.path.exists(output_file), "Should save analysis data")
        
        # Step 7: Read recommendations
        loaded_recs = self.resource_analyzer.get_recommendations(self.connection_id)
        self.assertEqual(len(loaded_recs), len(recommendations), "Should retrieve same recommendations")
        
        # Workflow completed successfully
        print("\n✅ Full workflow verification successful")


if __name__ == "__main__":
    unittest.main(verbosity=2)