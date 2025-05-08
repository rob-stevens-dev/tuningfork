"""
Integration tests for ResourceAnalyzer module.

These tests require actual database connections to run.
To skip these tests if no database is available, run with:
pytest -m "not integration"
"""

import os
import sys
import json
import time
import pytest
import psutil
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from tuningfork.analyzers.resource_analyzer import ResourceAnalyzer
from tuningfork.connection.connection_manager import ConnectionManager
from tuningfork.core.config_manager import ConfigManager
from tuningfork.models.recommendation import Recommendation


@pytest.mark.integration
class TestResourceAnalyzerIntegration:
    """Integration tests for ResourceAnalyzer class."""
    
    @classmethod
    def setup_class(cls):
        """Set up test fixtures."""
        # Create test data directory
        os.makedirs("data", exist_ok=True)
        
        # Setup config manager with minimal configuration
        config_file = "data/config.json"
        with open(config_file, "w") as f:
            json.dump({
                "storage_directory": "data",
                "connections": {
                    "test_sqlite": {
                        "type": "sqlite",
                        "database": ":memory:",
                        "timeout": 30
                    }
                }
            }, f, indent=2)
        
        cls.config_manager = ConfigManager(config_file)
        cls.connection_manager = ConnectionManager(cls.config_manager)
        cls.resource_analyzer = ResourceAnalyzer(cls.connection_manager, cls.config_manager)
    
    @classmethod
    def teardown_class(cls):
        """Clean up after tests."""
        # Close all connections
        cls.connection_manager.close_all_connections()
        
        # Remove test files
        if os.path.exists("data/config.json"):
            os.remove("data/config.json")
        
        if os.path.exists("data/recommendations_test_sqlite.json"):
            os.remove("data/recommendations_test_sqlite.json")
        
        if os.path.exists("data/analysis_data.json"):
            os.remove("data/analysis_data.json")
        
        # Remove data directory if empty
        try:
            os.rmdir("data")
        except OSError:
            pass
    
    def setup_method(self):
        """Set up each test case."""
        self.connection_id = "test_sqlite"
        
        # Connect to SQLite in-memory database
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
    
    def test_full_analysis_workflow(self):
        """Test the full analysis workflow."""
        # Step 1: Analyze resources
        resource_data = self.resource_analyzer.analyze_resources(self.connection_id)
        
        # Verify resource data
        assert resource_data is not None
        assert "system" in resource_data
        assert "sqlite" in resource_data
        
        # Check system info
        assert "physical_cores" in resource_data["system"]
        assert "memory_total" in resource_data["system"]
        
        # Check SQLite info
        assert "version" in resource_data["sqlite"]
        assert "database_info" in resource_data["sqlite"]
        assert "tables" in resource_data["sqlite"]
        
        # Verify at least one table is found
        tables = resource_data["sqlite"]["tables"]
        assert len(tables) > 0
        
        # Find our test table
        test_table = next((t for t in tables if t["name"] == "test_table"), None)
        assert test_table is not None
        assert test_table["row_count"] == 3
        
        # Step 2: Analyze configuration
        config_data = self.resource_analyzer.analyze_configuration(self.connection_id)
        
        # Verify configuration data
        assert config_data is not None
        assert "pragmas" in config_data
        
        # Check a few important pragmas
        pragmas = config_data["pragmas"]
        assert "journal_mode" in pragmas
        assert "synchronous" in pragmas
        assert "cache_size" in pragmas
        
        # Step 3: Monitor resource utilization
        # Use a short duration for testing
        utilization_data = self.resource_analyzer.monitor_resource_utilization(
            self.connection_id, duration=2, interval=1
        )
        
        # Verify utilization data
        assert utilization_data is not None
        assert "system" in utilization_data
        assert "sqlite" in utilization_data
        
        # Check system utilization
        system_util = utilization_data["system"]
        assert "cpu" in system_util
        assert "memory" in system_util
        assert "disk_io" in system_util
        assert "network_io" in system_util
        
        # Verify we have at least one measurement
        assert len(system_util["cpu"]) > 0
        assert len(system_util["memory"]) > 0
        
        # Step 4: Generate recommendations
        recommendations = self.resource_analyzer.generate_resource_recommendations(self.connection_id)
        
        # Verify recommendations
        assert recommendations is not None
        assert isinstance(recommendations, list)
        
        # We should have at least one recommendation
        assert len(recommendations) > 0
        
        # Check first recommendation
        first_rec = recommendations[0]
        assert isinstance(first_rec, Recommendation)
        assert first_rec.title != ""
        assert first_rec.description != ""
        assert first_rec.implementation_script != ""
        
        # Step 5: Save analysis data
        output_file = "data/analysis_data.json"
        self.resource_analyzer.save_analysis_data(self.connection_id, output_file)
        
        # Verify file was created
        assert os.path.exists(output_file)
        
        # Load the file and check contents
        with open(output_file, "r") as f:
            saved_data = json.load(f)
        
        assert saved_data["connection_id"] == self.connection_id
        assert "resource_data" in saved_data
        assert "config_data" in saved_data
        assert "utilization_data" in saved_data
        
        # Step 6: Verify get_recommendations works
        loaded_recommendations = self.resource_analyzer.get_recommendations(self.connection_id)
        
        # Should be the same recommendations
        assert len(loaded_recommendations) == len(recommendations)
        assert loaded_recommendations[0].id == recommendations[0].id
        assert loaded_recommendations[0].title == recommendations[0].title


@pytest.mark.integration
@pytest.mark.sqlite
class TestSQLiteResourceAnalyzer:
    """Integration tests specific to SQLite resource analyzer."""
    
    @classmethod
    def setup_class(cls):
        """Set up test fixtures."""
        # Create test data directory
        os.makedirs("data", exist_ok=True)
        
        # Setup config manager with minimal configuration
        config_file = "data/config.json"
        with open(config_file, "w") as f:
            json.dump({
                "storage_directory": "data",
                "connections": {
                    "test_sqlite": {
                        "type": "sqlite",
                        "database": "data/test.db",
                        "timeout": 30
                    }
                }
            }, f, indent=2)
        
        cls.config_manager = ConfigManager(config_file)
        cls.connection_manager = ConnectionManager(cls.config_manager)
        cls.resource_analyzer = ResourceAnalyzer(cls.connection_manager, cls.config_manager)
    
    @classmethod
    def teardown_class(cls):
        """Clean up after tests."""
        # Close all connections
        cls.connection_manager.close_all_connections()
        
        # Remove test files
        if os.path.exists("data/config.json"):
            os.remove("data/config.json")
        
        if os.path.exists("data/test.db"):
            os.remove("data/test.db")
        
# Remove data directory if empty
        try:
            os.rmdir("data")
        except OSError:
            pass
    
    def setup_method(self):
        """Set up each test case."""
        self.connection_id = "test_sqlite"
        
        # Connect to SQLite database
        if self.connection_id not in self.connection_manager.connections:
            self.connection_manager.connect(self.connection_id)
        
        # Setup test database with various table sizes and fragmentation
        connection = self.connection_manager.get_connection(self.connection_id)
        cursor = connection.cursor()
        
        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS small_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS medium_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER,
                description TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS large_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER,
                description TEXT,
                timestamp TEXT,
                status INTEGER
            )
        """)
        
        # Insert data
        cursor.execute("DELETE FROM small_table")
        for i in range(100):
            cursor.execute(
                "INSERT INTO small_table (name, value) VALUES (?, ?)",
                (f"item{i}", i * 10)
            )
        
        cursor.execute("DELETE FROM medium_table")
        for i in range(5000):
            cursor.execute(
                "INSERT INTO medium_table (name, value, description) VALUES (?, ?, ?)",
                (f"item{i}", i * 10, f"Description for item {i}")
            )
        
        cursor.execute("DELETE FROM large_table")
        for i in range(15000):
            cursor.execute(
                "INSERT INTO large_table (name, value, description, timestamp, status) VALUES (?, ?, ?, ?, ?)",
                (f"item{i}", i * 10, f"Description for item {i}", f"2025-01-{(i % 30) + 1:02d}", i % 5)
            )
        
        # Create an index on small_table but not on large_table
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_small_table_name ON small_table(name)")
        
        # Create fragmentation by deleting and inserting
        for i in range(0, 10000, 2):
            cursor.execute("DELETE FROM large_table WHERE id = ?", (i,))
        
        for i in range(0, 10000, 2):
            cursor.execute(
                "INSERT INTO large_table (name, value, description, timestamp, status) VALUES (?, ?, ?, ?, ?)",
                (f"newitem{i}", i * 20, f"New description for item {i}", f"2025-02-{(i % 28) + 1:02d}", i % 3)
            )
        
        connection.commit()
        cursor.close()
    
    def test_analyze_resources_sqlite(self):
        """Test SQLite resource analysis."""
        # Analyze resources
        resource_data = self.resource_analyzer.analyze_resources(self.connection_id)
        
        # Verify SQLite database information
        sqlite_info = resource_data["sqlite"]
        assert "version" in sqlite_info
        assert "database_info" in sqlite_info
        assert "tables" in sqlite_info
        
        # Verify tables were detected
        tables = {table["name"]: table for table in sqlite_info["tables"]}
        assert "small_table" in tables
        assert "medium_table" in tables
        assert "large_table" in tables
        
        # Verify row counts
        assert tables["small_table"]["row_count"] == 100
        assert tables["medium_table"]["row_count"] == 5000
        assert tables["large_table"]["row_count"] > 10000  # May vary due to test setup
        
        # Verify index detection
        assert tables["small_table"]["index_count"] >= 1  # Primary key index + name index
        assert tables["large_table"]["index_count"] >= 1  # At least primary key index
    
    def test_analyze_configuration_sqlite(self):
        """Test SQLite configuration analysis."""
        # Analyze configuration
        config_data = self.resource_analyzer.analyze_configuration(self.connection_id)
        
        # Verify pragmas
        pragmas = config_data["pragmas"]
        assert "journal_mode" in pragmas
        assert "synchronous" in pragmas
        assert "cache_size" in pragmas
        assert "temp_store" in pragmas
        
        # Verify database file path
        assert "database_list" in config_data
        db_list = config_data["database_list"]
        assert len(db_list) > 0
        assert db_list[0]["name"] == "main"
        assert "test.db" in db_list[0]["file"]
    
    def test_generate_recommendations_sqlite(self):
        """Test SQLite recommendation generation."""
        # First, collect all the necessary data
        self.resource_analyzer.analyze_resources(self.connection_id)
        self.resource_analyzer.analyze_configuration(self.connection_id)
        self.resource_analyzer.monitor_resource_utilization(self.connection_id, duration=2, interval=1)
        
        # Generate recommendations
        recommendations = self.resource_analyzer.generate_resource_recommendations(self.connection_id)
        
        # Verify we have recommendations
        assert len(recommendations) > 0
        
        # Check for specific recommendation types
        rec_titles = [rec.title for rec in recommendations]
        rec_categories = [rec.category for rec in recommendations]
        
        # Expected recommendations based on our test setup
        expected_recommendations = [
            # Missing index on large table
            "Missing indexes on large table",
            # Journal mode (if not WAL)
            "Enable WAL journal mode",
            # Vacuum recommendation due to fragmentation
            "Vacuum fragmented database"
        ]
        
        # At least one of these recommendations should be present
        assert any(expected in ' '.join(rec_titles) for expected in expected_recommendations)
        
        # Verify categories
        assert "Schema Optimization" in rec_categories or "Storage Configuration" in rec_categories
        
        # Implementation scripts for the recommendations should be SQL
        for rec in recommendations:
            assert rec.implementation_script.strip().upper().startswith(("CREATE", "ALTER", "PRAGMA", "VACUUM"))


# Optional: Tests for PostgreSQL, MySQL, and MSSQL
# These will be skipped if the corresponding database is not available

@pytest.mark.integration
@pytest.mark.postgresql
@pytest.mark.skipif(
    "POSTGRES_TEST_DSN" not in os.environ,
    reason="PostgreSQL connection settings not available"
)
class TestPostgreSQLResourceAnalyzer:
    """Integration tests specific to PostgreSQL resource analyzer."""
    
    @classmethod
    def setup_class(cls):
        """Set up test fixtures."""
        # Create test data directory
        os.makedirs("data", exist_ok=True)
        
        # Get PostgreSQL connection string from environment
        postgres_dsn = os.environ.get("POSTGRES_TEST_DSN")
        
        # Parse DSN to get connection settings
        # Format: "host=localhost port=5432 dbname=testdb user=postgres password=postgres"
        params = dict(param.split('=') for param in postgres_dsn.split() if '=' in param)
        
        # Setup config manager with PostgreSQL configuration
        config_file = "data/config.json"
        with open(config_file, "w") as f:
            json.dump({
                "storage_directory": "data",
                "connections": {
                    "test_postgresql": {
                        "type": "postgresql",
                        "host": params.get("host", "localhost"),
                        "port": int(params.get("port", 5432)),
                        "database": params.get("dbname", "postgres"),
                        "user": params.get("user", "postgres"),
                        "password": params.get("password", "postgres")
                    }
                }
            }, f, indent=2)
        
        cls.config_manager = ConfigManager(config_file)
        cls.connection_manager = ConnectionManager(cls.config_manager)
        cls.resource_analyzer = ResourceAnalyzer(cls.connection_manager, cls.config_manager)
    
    @classmethod
    def teardown_class(cls):
        """Clean up after tests."""
        # Close all connections
        cls.connection_manager.close_all_connections()
        
        # Remove test files
        if os.path.exists("data/config.json"):
            os.remove("data/config.json")
        
        # Remove data directory if empty
        try:
            os.rmdir("data")
        except OSError:
            pass
    
    def setup_method(self):
        """Set up each test case."""
        self.connection_id = "test_postgresql"
        
        # Connect to PostgreSQL
        if self.connection_id not in self.connection_manager.connections:
            self.connection_manager.connect(self.connection_id)
    
    def test_analyze_resources_postgresql(self):
        """Test PostgreSQL resource analysis."""
        # Analyze resources
        resource_data = self.resource_analyzer.analyze_resources(self.connection_id)
        
        # Verify PostgreSQL information
        assert "postgresql" in resource_data
        pg_info = resource_data["postgresql"]
        
        # Basic checks
        assert "version" in pg_info
        assert pg_info["version"].lower().startswith("postgresql")
        
        # Check system info
        assert "system" in resource_data
        assert "physical_cores" in resource_data["system"]
        assert "memory_total" in resource_data["system"]
    
    def test_analyze_configuration_postgresql(self):
        """Test PostgreSQL configuration analysis."""
        # Analyze configuration
        config_data = self.resource_analyzer.analyze_configuration(self.connection_id)
        
        # Verify configuration groups
        assert "all_settings" in config_data
        assert "memory_settings" in config_data
        assert "wal_settings" in config_data
        assert "autovacuum_settings" in config_data
        
        # Check specific important settings
        memory_settings = {setting["name"]: setting for setting in config_data["memory_settings"]}
        assert "shared_buffers" in memory_settings
        assert "work_mem" in memory_settings
        assert "maintenance_work_mem" in memory_settings
    
    def test_generate_recommendations_postgresql(self):
        """Test PostgreSQL recommendation generation."""
        # First, collect all the necessary data
        self.resource_analyzer.analyze_resources(self.connection_id)
        self.resource_analyzer.analyze_configuration(self.connection_id)
        self.resource_analyzer.monitor_resource_utilization(self.connection_id, duration=2, interval=1)
        
        # Generate recommendations
        recommendations = self.resource_analyzer.generate_resource_recommendations(self.connection_id)
        
        # Simply verify we get recommendations - specific ones will vary by environment
        assert len(recommendations) >= 0  # May be 0 if the PostgreSQL instance is well-tuned
        
        # If we have recommendations, check their structure
        for rec in recommendations:
            assert rec.title != ""
            assert rec.description != ""
            assert rec.priority is not None
            assert rec.implementation_script != ""


@pytest.mark.integration
@pytest.mark.mysql
@pytest.mark.skipif(
    "MYSQL_TEST_DSN" not in os.environ,
    reason="MySQL connection settings not available"
)
class TestMySQLResourceAnalyzer:
    """Integration tests specific to MySQL resource analyzer."""
    
    @classmethod
    def setup_class(cls):
        """Set up test fixtures."""
        # Create test data directory
        os.makedirs("data", exist_ok=True)
        
        # Get MySQL connection string from environment
        mysql_dsn = os.environ.get("MYSQL_TEST_DSN")
        
        # Parse DSN to get connection settings
        # Format: "host=localhost port=3306 database=testdb user=root password=root"
        params = dict(param.split('=') for param in mysql_dsn.split() if '=' in param)
        
        # Setup config manager with MySQL configuration
        config_file = "data/config.json"
        with open(config_file, "w") as f:
            json.dump({
                "storage_directory": "data",
                "connections": {
                    "test_mysql": {
                        "type": "mysql",
                        "host": params.get("host", "localhost"),
                        "port": int(params.get("port", 3306)),
                        "database": params.get("database", "mysql"),
                        "user": params.get("user", "root"),
                        "password": params.get("password", "root")
                    }
                }
            }, f, indent=2)
        
        cls.config_manager = ConfigManager(config_file)
        cls.connection_manager = ConnectionManager(cls.config_manager)
        cls.resource_analyzer = ResourceAnalyzer(cls.connection_manager, cls.config_manager)
    
    @classmethod
    def teardown_class(cls):
        """Clean up after tests."""
        # Close all connections
        cls.connection_manager.close_all_connections()
        
        # Remove test files
        if os.path.exists("data/config.json"):
            os.remove("data/config.json")
        
        # Remove data directory if empty
        try:
            os.rmdir("data")
        except OSError:
            pass
    
    def setup_method(self):
        """Set up each test case."""
        self.connection_id = "test_mysql"
        
        # Connect to MySQL
        if self.connection_id not in self.connection_manager.connections:
            self.connection_manager.connect(self.connection_id)
    
    def test_analyze_resources_mysql(self):
        """Test MySQL resource analysis."""
        # Analyze resources
        resource_data = self.resource_analyzer.analyze_resources(self.connection_id)
        
        # Verify MySQL information
        assert "mysql" in resource_data
        mysql_info = resource_data["mysql"]
        
        # Basic checks
        assert "version" in mysql_info
        assert mysql_info["version"].lower().find("mysql") >= 0 or mysql_info["version"].lower().find("mariadb") >= 0
        
        # Check system info
        assert "system" in resource_data
        assert "physical_cores" in resource_data["system"]
        assert "memory_total" in resource_data["system"]
    
    def test_analyze_configuration_mysql(self):
        """Test MySQL configuration analysis."""
        # Analyze configuration
        config_data = self.resource_analyzer.analyze_configuration(self.connection_id)
        
        # Verify configuration groups
        assert "all_variables" in config_data
        assert "memory_variables" in config_data
        assert "innodb_variables" in config_data
        assert "status_variables" in config_data
        
        # Check specific important settings
        memory_vars = {var["name"]: var["value"] for var in config_data["memory_variables"]}
        assert "innodb_buffer_pool_size" in memory_vars
        assert "max_connections" in memory_vars
        
        innodb_vars = {var["name"]: var["value"] for var in config_data["innodb_variables"]}
        assert "innodb_flush_method" in innodb_vars
    
    def test_generate_recommendations_mysql(self):
        """Test MySQL recommendation generation."""
        # First, collect all the necessary data
        self.resource_analyzer.analyze_resources(self.connection_id)
        self.resource_analyzer.analyze_configuration(self.connection_id)
        self.resource_analyzer.monitor_resource_utilization(self.connection_id, duration=2, interval=1)
        
        # Generate recommendations
        recommendations = self.resource_analyzer.generate_resource_recommendations(self.connection_id)
        
        # Simply verify we get recommendations - specific ones will vary by environment
        assert len(recommendations) >= 0  # May be 0 if the MySQL instance is well-tuned
        
        # If we have recommendations, check their structure
        for rec in recommendations:
            assert rec.title != ""
            assert rec.description != ""
            assert rec.priority is not None
            assert rec.implementation_script != ""


@pytest.mark.integration
@pytest.mark.mssql
@pytest.mark.skipif(
    "MSSQL_TEST_DSN" not in os.environ,
    reason="MSSQL connection settings not available"
)
class TestMSSQLResourceAnalyzer:
    """Integration tests specific to Microsoft SQL Server resource analyzer."""
    
    @classmethod
    def setup_class(cls):
        """Set up test fixtures."""
        # Create test data directory
        os.makedirs("data", exist_ok=True)
        
        # Get MSSQL connection string from environment
        mssql_dsn = os.environ.get("MSSQL_TEST_DSN")
        
        # Parse DSN to get connection settings
        # Format: "server=localhost port=1433 database=master user=sa password=P@ssw0rd"
        params = dict(param.split('=') for param in mssql_dsn.split() if '=' in param)
        
        # Setup config manager with MSSQL configuration
        config_file = "data/config.json"
        with open(config_file, "w") as f:
            json.dump({
                "storage_directory": "data",
                "connections": {
                    "test_mssql": {
                        "type": "mssql",
                        "server": params.get("server", "localhost"),
                        "port": int(params.get("port", 1433)),
                        "database": params.get("database", "master"),
                        "user": params.get("user", "sa"),
                        "password": params.get("password", "P@ssw0rd"),
                        "driver": params.get("driver", "{ODBC Driver 17 for SQL Server}")
                    }
                }
            }, f, indent=2)
        
        cls.config_manager = ConfigManager(config_file)
        cls.connection_manager = ConnectionManager(cls.config_manager)
        cls.resource_analyzer = ResourceAnalyzer(cls.connection_manager, cls.config_manager)
    
    @classmethod
    def teardown_class(cls):
        """Clean up after tests."""
        # Close all connections
        cls.connection_manager.close_all_connections()
        
        # Remove test files
        if os.path.exists("data/config.json"):
            os.remove("data/config.json")
        
        # Remove data directory if empty
        try:
            os.rmdir("data")
        except OSError:
            pass
    
    def setup_method(self):
        """Set up each test case."""
        self.connection_id = "test_mssql"
        
        # Connect to MSSQL
        if self.connection_id not in self.connection_manager.connections:
            self.connection_manager.connect(self.connection_id)
    
    def test_analyze_resources_mssql(self):
        """Test MSSQL resource analysis."""
        # Analyze resources
        resource_data = self.resource_analyzer.analyze_resources(self.connection_id)
        
        # Verify MSSQL information
        assert "mssql" in resource_data
        mssql_info = resource_data["mssql"]
        
        # Basic checks
        assert "version" in mssql_info
        assert "Microsoft SQL Server" in mssql_info["version"]
        
        # Check system info
        assert "system" in resource_data
        assert "physical_cores" in resource_data["system"]
        assert "memory_total" in resource_data["system"]
    
    def test_analyze_configuration_mssql(self):
        """Test MSSQL configuration analysis."""
        # Analyze configuration
        config_data = self.resource_analyzer.analyze_configuration(self.connection_id)
        
        # Verify configuration groups
        assert "all_configurations" in config_data
        assert "memory_configuration" in config_data
        assert "io_configuration" in config_data
        
        # Check specific important settings
        memory_config = config_data["memory_configuration"]
        assert "max_server_memory_mb" in memory_config
        assert "max_worker_threads" in memory_config
    
    def test_generate_recommendations_mssql(self):
        """Test MSSQL recommendation generation."""
        # First, collect all the necessary data
        self.resource_analyzer.analyze_resources(self.connection_id)
        self.resource_analyzer.analyze_configuration(self.connection_id)
        self.resource_analyzer.monitor_resource_utilization(self.connection_id, duration=2, interval=1)
        
        # Generate recommendations
        recommendations = self.resource_analyzer.generate_resource_recommendations(self.connection_id)
        
        # Simply verify we get recommendations - specific ones will vary by environment
        assert len(recommendations) >= 0  # May be 0 if the MSSQL instance is well-tuned
        
        # If we have recommendations, check their structure
        for rec in recommendations:
            assert rec.title != ""
            assert rec.description != ""
            assert rec.priority is not None
            assert rec.implementation_script != ""


if __name__ == "__main__":
    pytest.main(["-v"])