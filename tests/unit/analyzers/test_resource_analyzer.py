"""
Unit tests for ResourceAnalyzer module.
"""

import os
import json
import unittest
from unittest.mock import MagicMock, patch, call
from datetime import datetime

from tuningfork.analyzers.resource_analyzer import ResourceAnalyzer, PostgreSQLResourceAnalyzer
from tuningfork.connection.connection_manager import ConnectionManager, Connection
from tuningfork.core.config_manager import ConfigManager
from tuningfork.models.recommendation import Recommendation, RecommendationPriority, RecommendationType
from tuningfork.util.exceptions import ResourceAnalysisError


class TestResourceAnalyzer(unittest.TestCase):
    """Unit tests for ResourceAnalyzer class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.connection_manager = MagicMock(spec=ConnectionManager)
        self.config_manager = MagicMock(spec=ConfigManager)
        self.analyzer = ResourceAnalyzer(self.connection_manager, self.config_manager)
        
        # Setup mock connection
        self.mock_connection = MagicMock(spec=Connection)
        self.mock_connection.db_type = "postgresql"
        self.connection_manager.get_connection.return_value = self.mock_connection
        
        # Mock config manager
        self.config_manager.get_value.return_value = "data"
        
        # Create test data directory
        os.makedirs("data", exist_ok=True)
    
    def tearDown(self):
        """Clean up after tests."""
        # Remove test file if it exists
        test_file = os.path.join("data", "recommendations_test_conn.json")
        if os.path.exists(test_file):
            os.remove(test_file)
        
        # Remove data directory if empty
        try:
            os.rmdir("data")
        except OSError:
            pass
    
    @patch('tuningfork.analyzers.resource_analyzer.PostgreSQLResourceAnalyzer')
    def test_analyze_resources(self, mock_pg_analyzer_class):
        """Test analyze_resources method."""
        # Setup mock PostgreSQL analyzer
        mock_pg_analyzer = MagicMock()
        mock_pg_analyzer_class.return_value = mock_pg_analyzer
        
        # Mock the analyze_server_resources method
        expected_result = {"cpu": 4, "memory": 16}
        mock_pg_analyzer.analyze_server_resources.return_value = expected_result
        
        # Call analyze_resources
        result = self.analyzer.analyze_resources("test_conn")
        
        # Verify the result
        self.assertEqual(result, expected_result)
        self.assertEqual(self.analyzer.resource_data["test_conn"], expected_result)
        
        # Verify method calls
        self.connection_manager.get_connection.assert_called_once_with("test_conn")
        mock_pg_analyzer.analyze_server_resources.assert_called_once_with(self.mock_connection)
    
    def test_analyze_resources_connection_not_found(self):
        """Test analyze_resources with connection not found."""
        # Mock connection manager to return None
        self.connection_manager.get_connection.return_value = None
        
        # Call analyze_resources and verify it raises an exception
        with self.assertRaises(ResourceAnalysisError):
            self.analyzer.analyze_resources("nonexistent_conn")
    
    @patch('tuningfork.analyzers.resource_analyzer.PostgreSQLResourceAnalyzer')
    def test_analyze_configuration(self, mock_pg_analyzer_class):
        """Test analyze_configuration method."""
        # Setup mock PostgreSQL analyzer
        mock_pg_analyzer = MagicMock()
        mock_pg_analyzer_class.return_value = mock_pg_analyzer
        
        # Mock the analyze_db_configuration method
        expected_result = {"setting1": "value1", "setting2": "value2"}
        mock_pg_analyzer.analyze_db_configuration.return_value = expected_result
        
        # Call analyze_configuration
        result = self.analyzer.analyze_configuration("test_conn")
        
        # Verify the result
        self.assertEqual(result, expected_result)
        self.assertEqual(self.analyzer.config_data["test_conn"], expected_result)
        
        # Verify method calls
        self.connection_manager.get_connection.assert_called_once_with("test_conn")
        mock_pg_analyzer.analyze_db_configuration.assert_called_once_with(self.mock_connection)
    
    @patch('tuningfork.analyzers.resource_analyzer.PostgreSQLResourceAnalyzer')
    def test_monitor_resource_utilization(self, mock_pg_analyzer_class):
        """Test monitor_resource_utilization method."""
        # Setup mock PostgreSQL analyzer
        mock_pg_analyzer = MagicMock()
        mock_pg_analyzer_class.return_value = mock_pg_analyzer
        
        # Mock the monitor_resource_utilization method
        expected_result = {"cpu_usage": [10, 20, 30], "memory_usage": [50, 60, 70]}
        mock_pg_analyzer.monitor_resource_utilization.return_value = expected_result
        
        # Call monitor_resource_utilization
        result = self.analyzer.monitor_resource_utilization("test_conn", duration=10, interval=2)
        
        # Verify the result
        self.assertEqual(result, expected_result)
        self.assertEqual(self.analyzer.utilization_data["test_conn"], expected_result)
        
        # Verify method calls
        self.connection_manager.get_connection.assert_called_once_with("test_conn")
        mock_pg_analyzer.monitor_resource_utilization.assert_called_once_with(
            self.mock_connection, 10, 2
        )
    
    @patch('tuningfork.analyzers.resource_analyzer.PostgreSQLResourceAnalyzer')
    def test_generate_resource_recommendations(self, mock_pg_analyzer_class):
        """Test generate_resource_recommendations method."""
        # Setup mock PostgreSQL analyzer
        mock_pg_analyzer = MagicMock()
        mock_pg_analyzer_class.return_value = mock_pg_analyzer
        
        # Setup test data
        self.analyzer.resource_data["test_conn"] = {"cpu": 4, "memory": 16}
        self.analyzer.config_data["test_conn"] = {"setting1": "value1", "setting2": "value2"}
        self.analyzer.utilization_data["test_conn"] = {"cpu_usage": [10, 20, 30]}
        
        # Mock the generate_recommendations method
        rec1 = Recommendation(
            title="Test Recommendation 1",
            description="Description 1",
            priority=RecommendationPriority.HIGH,
            type=RecommendationType.CONFIGURATION,
            implementation_script="test script 1",
            expected_benefit="benefit 1",
            risk_level="Low",
            risk_details="risk details 1",
            estimated_time="5 minutes",
            category="Test Category"
        )
        rec2 = Recommendation(
            title="Test Recommendation 2",
            description="Description 2",
            priority=RecommendationPriority.MEDIUM,
            type=RecommendationType.SCHEMA,
            implementation_script="test script 2",
            expected_benefit="benefit 2",
            risk_level="Medium",
            risk_details="risk details 2",
            estimated_time="10 minutes",
            category="Test Category"
        )
        expected_result = [rec1, rec2]
        mock_pg_analyzer.generate_recommendations.return_value = expected_result
        
        # Call generate_resource_recommendations
        result = self.analyzer.generate_resource_recommendations("test_conn")
        
        # Verify the result
        self.assertEqual(result, expected_result)
        self.assertEqual(self.analyzer.resource_recommendations, expected_result)
        
        # Verify method calls
        self.connection_manager.get_connection.assert_called_once_with("test_conn")
        mock_pg_analyzer.generate_recommendations.assert_called_once_with(
            self.mock_connection,
            self.analyzer.resource_data["test_conn"],
            self.analyzer.config_data["test_conn"],
            self.analyzer.utilization_data["test_conn"]
        )
    
    def test_generate_resource_recommendations_missing_data(self):
        """Test generate_resource_recommendations with missing data."""
        # Call generate_resource_recommendations and verify it raises an exception
        with self.assertRaises(ResourceAnalysisError):
            self.analyzer.generate_resource_recommendations("test_conn")
    
    @patch('tuningfork.analyzers.resource_analyzer.PostgreSQLResourceAnalyzer')
    def test_get_recommendations(self, mock_pg_analyzer_class):
        """Test get_recommendations method."""
        # Setup test recommendations
        rec1 = Recommendation(
            title="Test Recommendation 1",
            description="Description 1",
            priority=RecommendationPriority.HIGH,
            type=RecommendationType.CONFIGURATION,
            implementation_script="test script 1",
            expected_benefit="benefit 1",
            risk_level="Low",
            risk_details="risk details 1",
            estimated_time="5 minutes",
            category="Test Category"
        )
        rec2 = Recommendation(
            title="Test Recommendation 2",
            description="Description 2",
            priority=RecommendationPriority.MEDIUM,
            type=RecommendationType.SCHEMA,
            implementation_script="test script 2",
            expected_benefit="benefit 2",
            risk_level="Medium",
            risk_details="risk details 2",
            estimated_time="10 minutes",
            category="Test Category"
        )
        self.analyzer.resource_recommendations = [rec1, rec2]
        
        # Call get_recommendations
        result = self.analyzer.get_recommendations("test_conn")
        
        # Verify the result
        self.assertEqual(result, [rec1, rec2])
    
    def test_get_recommendations_empty(self):
        """Test get_recommendations with no recommendations."""
        # Mock _load_recommendations to return empty list
        self.analyzer._load_recommendations = MagicMock(return_value=[])
        
        # Call get_recommendations and verify it raises an exception
        with self.assertRaises(ResourceAnalysisError):
            self.analyzer.get_recommendations("test_conn")
    
    @patch('json.dump')
    def test_save_analysis_data(self, mock_json_dump):
        """Test save_analysis_data method."""
        # Setup test data
        self.analyzer.resource_data["test_conn"] = {"cpu": 4, "memory": 16}
        self.analyzer.config_data["test_conn"] = {"setting1": "value1", "setting2": "value2"}
        self.analyzer.utilization_data["test_conn"] = {"cpu_usage": [10, 20, 30]}
        
        # Call save_analysis_data
        self.analyzer.save_analysis_data("test_conn", "test_output.json")
        
        # Verify json.dump was called with the expected data
        # Note: We can't check the exact contents because timestamp will vary
        mock_json_dump.assert_called_once()
        args, kwargs = mock_json_dump.call_args
        data = args[0]
        
        self.assertEqual(data["connection_id"], "test_conn")
        self.assertEqual(data["resource_data"], {"cpu": 4, "memory": 16})
        self.assertEqual(data["config_data"], {"setting1": "value1", "setting2": "value2"})
        self.assertEqual(data["utilization_data"], {"cpu_usage": [10, 20, 30]})
        self.assertIn("timestamp", data)
        
        # Verify file was opened for writing
        self.assertEqual(kwargs["indent"], 2)
    
    def test_save_analysis_data_no_data(self):
        """Test save_analysis_data with no data."""
        # Call save_analysis_data and verify it raises an exception
        with self.assertRaises(ResourceAnalysisError):
            self.analyzer.save_analysis_data("test_conn", "test_output.json")
    
    @patch('json.dump')
    def test_store_recommendations(self, mock_json_dump):
        """Test _store_recommendations method."""
        # Setup test recommendations
        rec1 = Recommendation(
            title="Test Recommendation 1",
            description="Description 1",
            priority=RecommendationPriority.HIGH,
            type=RecommendationType.CONFIGURATION,
            implementation_script="test script 1",
            expected_benefit="benefit 1",
            risk_level="Low",
            risk_details="risk details 1",
            estimated_time="5 minutes",
            category="Test Category"
        )
        rec2 = Recommendation(
            title="Test Recommendation 2",
            description="Description 2",
            priority=RecommendationPriority.MEDIUM,
            type=RecommendationType.SCHEMA,
            implementation_script="test script 2",
            expected_benefit="benefit 2",
            risk_level="Medium",
            risk_details="risk details 2",
            estimated_time="10 minutes",
            category="Test Category"
        )
        recommendations = [rec1, rec2]
        
        # Call _store_recommendations
        self.analyzer._store_recommendations("test_conn", recommendations)
        
        # Verify json.dump was called with the expected data
        mock_json_dump.assert_called_once()
        args, kwargs = mock_json_dump.call_args
        data = args[0]
        
        self.assertEqual(data["connection_id"], "test_conn")
        self.assertEqual(len(data["recommendations"]), 2)
        self.assertEqual(data["recommendations"][0]["title"], "Test Recommendation 1")
        self.assertEqual(data["recommendations"][1]["title"], "Test Recommendation 2")
        self.assertIn("timestamp", data)
        
        # Verify file was opened for writing
        self.assertEqual(kwargs["indent"], 2)
    
    @patch('json.load')
    @patch('os.path.exists')
    def test_load_recommendations(self, mock_exists, mock_json_load):
        """Test _load_recommendations method."""
        # Setup mocks
        mock_exists.return_value = True
        
        rec1_dict = {
            "id": "rec1",
            "title": "Test Recommendation 1",
            "description": "Description 1",
            "priority": "high",
            "type": "configuration",
            "implementation_script": "test script 1",
            "expected_benefit": "benefit 1",
            "risk_level": "Low",
            "risk_details": "risk details 1",
            "estimated_time": "5 minutes",
            "category": "Test Category",
            "timestamp": datetime.now().isoformat(),
            "implemented": False
        }
        rec2_dict = {
            "id": "rec2",
            "title": "Test Recommendation 2",
            "description": "Description 2",
            "priority": "medium",
            "type": "schema",
            "implementation_script": "test script 2",
            "expected_benefit": "benefit 2",
            "risk_level": "Medium",
            "risk_details": "risk details 2",
            "estimated_time": "10 minutes",
            "category": "Test Category",
            "timestamp": datetime.now().isoformat(),
            "implemented": False
        }
        mock_json_load.return_value = {
            "connection_id": "test_conn",
            "timestamp": datetime.now().isoformat(),
            "recommendations": [rec1_dict, rec2_dict]
        }
        
        # Call _load_recommendations
        result = self.analyzer._load_recommendations("test_conn")
        
        # Verify results
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].id, "rec1")
        self.assertEqual(result[0].title, "Test Recommendation 1")
        self.assertEqual(result[1].id, "rec2")
        self.assertEqual(result[1].title, "Test Recommendation 2")
    
    @patch('os.path.exists')
    def test_load_recommendations_no_file(self, mock_exists):
        """Test _load_recommendations with no file."""
        # Setup mock
        mock_exists.return_value = False
        
        # Call _load_recommendations
        result = self.analyzer._load_recommendations("test_conn")
        
        # Verify empty list is returned
        self.assertEqual(result, [])
    
    def test_get_resource_analyzer_for_db_type(self):
        """Test _get_resource_analyzer_for_db_type method."""
        # Test PostgreSQL
        analyzer = self.analyzer._get_resource_analyzer_for_db_type("postgresql")
        self.assertIsInstance(analyzer, PostgreSQLResourceAnalyzer)
        
        # Test MySQL
        analyzer = self.analyzer._get_resource_analyzer_for_db_type("mysql")
        self.assertEqual(analyzer.__class__.__name__, "MySQLResourceAnalyzer")
        
        # Test MSSQL
        analyzer = self.analyzer._get_resource_analyzer_for_db_type("mssql")
        self.assertEqual(analyzer.__class__.__name__, "MSSQLResourceAnalyzer")
        
        # Test SQLite
        analyzer = self.analyzer._get_resource_analyzer_for_db_type("sqlite")
        self.assertEqual(analyzer.__class__.__name__, "SQLiteResourceAnalyzer")
        
        # Test unsupported database type
        with self.assertRaises(ValueError):
            self.analyzer._get_resource_analyzer_for_db_type("unsupported")


class TestPostgreSQLResourceAnalyzer(unittest.TestCase):
    """Unit tests for PostgreSQLResourceAnalyzer class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.analyzer = PostgreSQLResourceAnalyzer()
        self.mock_connection = MagicMock(spec=Connection)
        self.mock_cursor = MagicMock()
        self.mock_connection.cursor.return_value = self.mock_cursor
    
    def test_convert_pg_size_to_bytes(self):
        """Test _convert_pg_size_to_bytes method."""
        # Test various size formats
        self.assertEqual(self.analyzer._convert_pg_size_to_bytes("8kB"), 8 * 1024)
        self.assertEqual(self.analyzer._convert_pg_size_to_bytes("16MB"), 16 * 1024 * 1024)
        self.assertEqual(self.analyzer._convert_pg_size_to_bytes("2GB"), 2 * 1024 * 1024 * 1024)
        self.assertEqual(self.analyzer._convert_pg_size_to_bytes("1TB"), 1024 * 1024 * 1024 * 1024)
        
        # Test without unit (should be bytes)
        self.assertEqual(self.analyzer._convert_pg_size_to_bytes("1024"), 1024)
        
        # Test without the 'B' suffix
        self.assertEqual(self.analyzer._convert_pg_size_to_bytes("4M"), 4 * 1024 * 1024)
    
    def test_format_bytes_to_pg_size(self):
        """Test _format_bytes_to_pg_size method."""
        # Test various byte values
        self.assertEqual(self.analyzer._format_bytes_to_pg_size(2048), "2kB")
        self.assertEqual(self.analyzer._format_bytes_to_pg_size(4 * 1024 * 1024), "4MB")
        self.assertEqual(self.analyzer._format_bytes_to_pg_size(6 * 1024 * 1024 * 1024), "6GB")
        self.assertEqual(
            self.analyzer._format_bytes_to_pg_size(3 * 1024 * 1024 * 1024 * 1024), "3TB"
        )
        
        # Test values less than 1024
        self.assertEqual(self.analyzer._format_bytes_to_pg_size(512), "512")
    
    def test_format_bytes_rate(self):
        """Test _format_bytes_rate method."""
        # Test various byte rates
        self.assertEqual(self.analyzer._format_bytes_rate(2048), "2.00 KB/s")
        self.assertEqual(self.analyzer._format_bytes_rate(4 * 1024 * 1024), "4.00 MB/s")
        self.assertEqual(self.analyzer._format_bytes_rate(6 * 1024 * 1024 * 1024), "6.00 GB/s")
        
        # Test values less than 1024
        self.assertEqual(self.analyzer._format_bytes_rate(512), "512.00 B/s")
    
    @patch('platform.platform')
    @patch('platform.processor')
    @patch('psutil.cpu_count')
    @patch('psutil.virtual_memory')
    @patch('psutil.disk_partitions')
    @patch('psutil.disk_usage')
    @patch('psutil.net_if_addrs')
    def test_get_common_system_info(
        self, mock_net_if_addrs, mock_disk_usage, mock_disk_partitions, 
        mock_virtual_memory, mock_cpu_count, mock_processor, mock_platform
    ):
        """Test _get_common_system_info method."""
        # Setup mocks
        mock_platform.return_value = "Linux-4.19.0-x86_64"
        mock_processor.return_value = "x86_64"
        mock_cpu_count.side_effect = [4, 8]  # physical, logical
        
        mock_memory = MagicMock()
        mock_memory.total = 16 * 1024 * 1024 * 1024
        mock_memory.available = 8 * 1024 * 1024 * 1024
        mock_virtual_memory.return_value = mock_memory
        
        mock_partition = MagicMock()
        mock_partition.device = "/dev/sda1"
        mock_partition.mountpoint = "/"
        mock_partition.fstype = "ext4"
        mock_disk_partitions.return_value = [mock_partition]
        
        mock_disk_info = MagicMock()
        mock_disk_info.total = 100 * 1024 * 1024 * 1024
        mock_disk_info.free = 50 * 1024 * 1024 * 1024
        mock_disk_usage.return_value = mock_disk_info
        
        mock_net_if_addrs.return_value = {
            "eth0": ["addr1", "addr2"]
        }
        
        # Call _get_common_system_info
        result = self.analyzer._get_common_system_info()
        
        # Verify results
        self.assertEqual(result["platform"], "Linux-4.19.0-x86_64")
        self.assertEqual(result["processor"], "x86_64")
        self.assertEqual(result["physical_cores"], 4)
        self.assertEqual(result["logical_cores"], 8)
        self.assertEqual(result["memory_total"], 16 * 1024 * 1024 * 1024)
        self.assertEqual(result["memory_available"], 8 * 1024 * 1024 * 1024)
        
        self.assertEqual(len(result["disk_partitions"]), 1)
        self.assertEqual(result["disk_partitions"][0]["device"], "/dev/sda1")
        self.assertEqual(result["disk_partitions"][0]["mountpoint"], "/")
        self.assertEqual(result["disk_partitions"][0]["fstype"], "ext4")
        self.assertEqual(result["disk_partitions"][0]["total_size"], 100 * 1024 * 1024 * 1024)
        self.assertEqual(result["disk_partitions"][0]["free_space"], 50 * 1024 * 1024 * 1024)
        
        self.assertEqual(len(result["network_interfaces"]), 1)
        self.assertEqual(result["network_interfaces"][0]["name"], "eth0")
        self.assertEqual(result["network_interfaces"][0]["addresses"], str(["addr1", "addr2"]))
    
    @patch('time.time')
    @patch('time.sleep')
    @patch('psutil.cpu_percent')
    @patch('psutil.virtual_memory')
    @patch('psutil.disk_io_counters')
    @patch('psutil.net_io_counters')
    def test_measure_system_utilization(
        self, mock_net_io_counters, mock_disk_io_counters, 
        mock_virtual_memory, mock_cpu_percent, mock_sleep, mock_time
    ):
        """Test _measure_system_utilization method."""
        # Setup mocks
        mock_time.side_effect = [0, 5, 10]  # Initial, after first measurement, after second measurement
        
        mock_cpu_percent.return_value = [10, 20, 30, 40]  # 4 CPUs
        
        mock_memory = MagicMock()
        mock_memory.total = 16 * 1024 * 1024 * 1024
        mock_memory.available = 8 * 1024 * 1024 * 1024
        mock_memory.percent = 50
        mock_memory.used = 8 * 1024 * 1024 * 1024
        mock_memory.free = 8 * 1024 * 1024 * 1024
        mock_virtual_memory.return_value = mock_memory
        
        # First measurement
        mock_disk_io1 = MagicMock()
        mock_disk_io1.read_bytes = 1000
        mock_disk_io1.write_bytes = 2000
        mock_disk_io1.read_count = 100
        mock_disk_io1.write_count = 200
        
        # Second measurement
        mock_disk_io2 = MagicMock()
        mock_disk_io2.read_bytes = 3000
        mock_disk_io2.write_bytes = 6000
        mock_disk_io2.read_count = 300
        mock_disk_io2.write_count = 600
        
        mock_disk_io_counters.side_effect = [mock_disk_io1, mock_disk_io2]
        
        # First measurement
        mock_net_io1 = MagicMock()
        mock_net_io1.bytes_sent = 5000
        mock_net_io1.bytes_recv = 10000
        mock_net_io1.packets_sent = 50
        mock_net_io1.packets_recv = 100
        
        # Second measurement
        mock_net_io2 = MagicMock()
        mock_net_io2.bytes_sent = 15000
        mock_net_io2.bytes_recv = 30000
        mock_net_io2.packets_sent = 150
        mock_net_io2.packets_recv = 300
        
        mock_net_io_counters.side_effect = [mock_net_io1, mock_net_io2]
        
        # Call _measure_system_utilization with duration 10 and interval 5
        result = self.analyzer._measure_system_utilization(10, 5)
        
        # Verify results
        self.assertIn("cpu", result)
        self.assertIn("memory", result)
        self.assertIn("disk_io", result)
        self.assertIn("network_io", result)
        
        # Check CPU metrics
        self.assertEqual(len(result["cpu"]), 2)
        self.assertEqual(result["cpu"][0]["overall_percent"], 25)  # Average of [10, 20, 30, 40]
        
        # Check memory metrics
        self.assertEqual(len(result["memory"]), 2)
        self.assertEqual(result["memory"][0]["total"], 16 * 1024 * 1024 * 1024)
        self.assertEqual(result["memory"][0]["percent"], 50)
        
        # Check disk IO metrics
        self.assertEqual(len(result["disk_io"]), 2)
        # Second measurement - first measurement divided by time diff (5)
        self.assertEqual(result["disk_io"][0]["read_bytes_sec"], 400)  # (3000 - 1000) / 5
        self.assertEqual(result["disk_io"][0]["write_bytes_sec"], 800)  # (6000 - 2000) / 5
        
        # Check network IO metrics
        self.assertEqual(len(result["network_io"]), 2)
        # Second measurement - first measurement divided by time diff (5)
        self.assertEqual(result["network_io"][0]["sent_bytes_sec"], 2000)  # (15000 - 5000) / 5
        self.assertEqual(result["network_io"][0]["recv_bytes_sec"], 4000)  # (30000 - 10000) / 5
        
        # Verify sleep was called with the interval
        mock_sleep.assert_called_with(5)


if __name__ == '__main__':
    unittest.main()