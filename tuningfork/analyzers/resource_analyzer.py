"""
ResourceAnalyzer module for TuningFork database performance optimization tool.

This module provides functionality to analyze server resources, database configurations,
and resource utilization. It also generates recommendations for optimizing resource usage.
"""

import os
import logging
import platform
import json
from datetime import datetime
import time
import psutil
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple, Union

from tuningfork.connection.connection_manager import ConnectionManager, Connection
from tuningfork.core.config_manager import ConfigManager
from tuningfork.util.exceptions import ResourceAnalysisError, ConfigurationError
from tuningfork.analyzers.base_analyzer import BaseAnalyzer
from tuningfork.models.recommendation import Recommendation, RecommendationPriority, RecommendationType

logger = logging.getLogger(__name__)


class ResourceAnalyzer(BaseAnalyzer):
    """
    Analyzes server resources and database configurations.
    
    This class provides functionality to analyze server resources, database configurations,
    and resource utilization. It also generates recommendations for optimizing resource usage.
    """
    
    def __init__(self, connection_manager: ConnectionManager, config_manager: ConfigManager):
        """
        Initialize the ResourceAnalyzer.
        
        Args:
            connection_manager: The ConnectionManager instance
            config_manager: The ConfigManager instance
        """
        super().__init__(connection_manager, config_manager)
        self.resource_data = {}
        self.config_data = {}
        self.utilization_data = {}
        self.resource_recommendations = []
    
    def analyze_resources(self, connection_id: str) -> Dict[str, Any]:
        """
        Analyzes server resources for the specified connection.
        
        Args:
            connection_id: The ID of the database connection
            
        Returns:
            A dictionary containing resource information
            
        Raises:
            ResourceAnalysisError: If there is an error analyzing resources
        """
        try:
            connection = self.connection_manager.get_connection(connection_id)
            if not connection:
                raise ResourceAnalysisError(f"Connection {connection_id} not found")
            
            # Get the database type to instantiate the appropriate analyzer
            db_type = connection.db_type
            analyzer = self._get_resource_analyzer_for_db_type(db_type)
            
            # Analyze server resources
            logger.info(f"Analyzing resources for connection {connection_id} ({db_type})")
            self.resource_data[connection_id] = analyzer.analyze_server_resources(connection)
            
            return self.resource_data[connection_id]
        except Exception as e:
            logger.error(f"Error analyzing resources: {str(e)}")
            raise ResourceAnalysisError(f"Error analyzing resources: {str(e)}")
    
    def analyze_configuration(self, connection_id: str) -> Dict[str, Any]:
        """
        Analyzes database configuration for the specified connection.
        
        Args:
            connection_id: The ID of the database connection
            
        Returns:
            A dictionary containing configuration information
            
        Raises:
            ResourceAnalysisError: If there is an error analyzing configuration
        """
        try:
            connection = self.connection_manager.get_connection(connection_id)
            if not connection:
                raise ResourceAnalysisError(f"Connection {connection_id} not found")
            
            # Get the database type to instantiate the appropriate analyzer
            db_type = connection.db_type
            analyzer = self._get_resource_analyzer_for_db_type(db_type)
            
            # Analyze database configuration
            logger.info(f"Analyzing configuration for connection {connection_id} ({db_type})")
            self.config_data[connection_id] = analyzer.analyze_db_configuration(connection)
            
            return self.config_data[connection_id]
        except Exception as e:
            logger.error(f"Error analyzing configuration: {str(e)}")
            raise ResourceAnalysisError(f"Error analyzing configuration: {str(e)}")
    
    def monitor_resource_utilization(self, connection_id: str, duration: int = 300, interval: int = 5) -> Dict[str, Any]:
        """
        Monitors resource utilization over time for the specified connection.
        
        Args:
            connection_id: The ID of the database connection
            duration: The duration of monitoring in seconds (default: 300)
            interval: The interval between measurements in seconds (default: 5)
            
        Returns:
            A dictionary containing utilization information
            
        Raises:
            ResourceAnalysisError: If there is an error monitoring utilization
        """
        try:
            connection = self.connection_manager.get_connection(connection_id)
            if not connection:
                raise ResourceAnalysisError(f"Connection {connection_id} not found")
            
            # Get the database type to instantiate the appropriate analyzer
            db_type = connection.db_type
            analyzer = self._get_resource_analyzer_for_db_type(db_type)
            
            # Monitor resource utilization
            logger.info(f"Monitoring resource utilization for connection {connection_id} ({db_type})")
            logger.info(f"Monitoring for {duration} seconds with an interval of {interval} seconds")
            
            self.utilization_data[connection_id] = analyzer.monitor_resource_utilization(connection, duration, interval)
            
            return self.utilization_data[connection_id]
        except Exception as e:
            logger.error(f"Error monitoring resource utilization: {str(e)}")
            raise ResourceAnalysisError(f"Error monitoring resource utilization: {str(e)}")
    
    def generate_resource_recommendations(self, connection_id: str) -> List[Recommendation]:
        """
        Generates resource optimization recommendations for the specified connection.
        
        This method requires that analyze_resources(), analyze_configuration(), and
        monitor_resource_utilization() have been called for the connection first.
        
        Args:
            connection_id: The ID of the database connection
            
        Returns:
            A list of Recommendation objects
            
        Raises:
            ResourceAnalysisError: If there is an error generating recommendations
        """
        try:
            connection = self.connection_manager.get_connection(connection_id)
            if not connection:
                raise ResourceAnalysisError(f"Connection {connection_id} not found")
            
            # Ensure we have the necessary data
            if (connection_id not in self.resource_data or
                connection_id not in self.config_data or
                connection_id not in self.utilization_data):
                raise ResourceAnalysisError("Resource, configuration, and utilization data must be collected first")
            
            # Get the database type to instantiate the appropriate analyzer
            db_type = connection.db_type
            analyzer = self._get_resource_analyzer_for_db_type(db_type)
            
            # Generate recommendations based on the collected data
            logger.info(f"Generating resource recommendations for connection {connection_id} ({db_type})")
            
            resource_data = self.resource_data[connection_id]
            config_data = self.config_data[connection_id]
            utilization_data = self.utilization_data[connection_id]
            
            self.resource_recommendations = analyzer.generate_recommendations(
                connection, resource_data, config_data, utilization_data
            )
            
            # Log the number of recommendations generated
            logger.info(f"Generated {len(self.resource_recommendations)} resource recommendations")
            
            # Store the recommendations for persistence
            self._store_recommendations(connection_id, self.resource_recommendations)
            
            return self.resource_recommendations
        except Exception as e:
            logger.error(f"Error generating resource recommendations: {str(e)}")
            raise ResourceAnalysisError(f"Error generating resource recommendations: {str(e)}")
    
    def get_recommendations(self, connection_id: str) -> List[Recommendation]:
        """
        Gets the generated recommendations for the specified connection.
        
        Args:
            connection_id: The ID of the database connection
            
        Returns:
            A list of Recommendation objects
            
        Raises:
            ResourceAnalysisError: If there are no recommendations for the connection
        """
        if not self.resource_recommendations:
            # Try to load from storage first
            loaded_recs = self._load_recommendations(connection_id)
            if loaded_recs:
                self.resource_recommendations = loaded_recs
            else:
                raise ResourceAnalysisError(f"No recommendations found for connection {connection_id}")
        
        return self.resource_recommendations
    
    def save_analysis_data(self, connection_id: str, output_file: str) -> None:
        """
        Saves all analysis data to a file.
        
        Args:
            connection_id: The ID of the database connection
            output_file: The path to the output file
            
        Raises:
            ResourceAnalysisError: If there is an error saving data
        """
        try:
            if connection_id not in self.resource_data:
                raise ResourceAnalysisError(f"No resource data found for connection {connection_id}")
            
            # Prepare the data to save
            data = {
                "connection_id": connection_id,
                "timestamp": datetime.now().isoformat(),
                "resource_data": self.resource_data.get(connection_id, {}),
                "config_data": self.config_data.get(connection_id, {}),
                "utilization_data": self.utilization_data.get(connection_id, {})
            }
            
            # Save to file
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Saved analysis data to {output_file}")
        except Exception as e:
            logger.error(f"Error saving analysis data: {str(e)}")
            raise ResourceAnalysisError(f"Error saving analysis data: {str(e)}")
    
    def _store_recommendations(self, connection_id: str, recommendations: List[Recommendation]) -> None:
        """
        Stores recommendations for persistence.
        
        Args:
            connection_id: The ID of the database connection
            recommendations: The list of recommendations to store
        """
        # In a real implementation, this would store to a database or file
        # For now, we'll just log that we would store them
        logger.info(f"Storing {len(recommendations)} recommendations for connection {connection_id}")
        
        # Get the storage directory from config
        storage_dir = self.config_manager.get_value("storage_directory", "data")
        os.makedirs(storage_dir, exist_ok=True)
        
        # Create a file for this connection's recommendations
        file_path = os.path.join(storage_dir, f"recommendations_{connection_id}.json")
        
        # Convert recommendations to serializable format
        serializable_recs = [rec.to_dict() for rec in recommendations]
        
        # Save to file
        with open(file_path, 'w') as f:
            json.dump({
                "connection_id": connection_id,
                "timestamp": datetime.now().isoformat(),
                "recommendations": serializable_recs
            }, f, indent=2)
    
    def _load_recommendations(self, connection_id: str) -> List[Recommendation]:
        """
        Loads stored recommendations for a connection.
        
        Args:
            connection_id: The ID of the database connection
            
        Returns:
            A list of Recommendation objects
        """
        # Get the storage directory from config
        storage_dir = self.config_manager.get_value("storage_directory", "data")
        file_path = os.path.join(storage_dir, f"recommendations_{connection_id}.json")
        
        if not os.path.exists(file_path):
            logger.warning(f"No stored recommendations found for connection {connection_id}")
            return []
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Convert from serialized format back to Recommendation objects
            recommendations = []
            for rec_data in data.get("recommendations", []):
                rec = Recommendation.from_dict(rec_data)
                recommendations.append(rec)
            
            logger.info(f"Loaded {len(recommendations)} recommendations for connection {connection_id}")
            return recommendations
        except Exception as e:
            logger.error(f"Error loading recommendations: {str(e)}")
            return []
    
    def _get_resource_analyzer_for_db_type(self, db_type: str) -> 'DBResourceAnalyzer':
        """
        Factory method to get the appropriate resource analyzer for the database type.
        
        Args:
            db_type: The database type
            
        Returns:
            An instance of the appropriate DBResourceAnalyzer subclass
            
        Raises:
            ValueError: If the database type is not supported
        """
        db_type = db_type.lower()
        if db_type == "postgresql":
            return PostgreSQLResourceAnalyzer()
        elif db_type == "mysql":
            return MySQLResourceAnalyzer()
        elif db_type == "mssql":
            return MSSQLResourceAnalyzer()
        elif db_type == "sqlite":
            return SQLiteResourceAnalyzer()
        else:
            raise ValueError(f"Unsupported database type: {db_type}")


class DBResourceAnalyzer(ABC):
    """Abstract base class for database-specific resource analyzers."""
    
    @abstractmethod
    def analyze_server_resources(self, connection: Connection) -> Dict[str, Any]:
        """
        Analyzes server resources for the connected database.
        
        Args:
            connection: The database connection
            
        Returns:
            A dictionary containing resource information
        """
        pass
    
    @abstractmethod
    def analyze_db_configuration(self, connection: Connection) -> Dict[str, Any]:
        """
        Analyzes database configuration.
        
        Args:
            connection: The database connection
            
        Returns:
            A dictionary containing configuration information
        """
        pass
    
    @abstractmethod
    def monitor_resource_utilization(self, connection: Connection, duration: int, interval: int) -> Dict[str, Any]:
        """
        Monitors resource utilization over time.
        
        Args:
            connection: The database connection
            duration: The duration of monitoring in seconds
            interval: The interval between measurements in seconds
            
        Returns:
            A dictionary containing utilization information
        """
        pass
    
    @abstractmethod
    def generate_recommendations(
        self, 
        connection: Connection, 
        resource_data: Dict[str, Any], 
        config_data: Dict[str, Any], 
        utilization_data: Dict[str, Any]
    ) -> List[Recommendation]:
        """
        Generates resource optimization recommendations.
        
        Args:
            connection: The database connection
            resource_data: The resource data
            config_data: The configuration data
            utilization_data: The utilization data
            
        Returns:
            A list of Recommendation objects
        """
        pass
    
    def _get_common_system_info(self) -> Dict[str, Any]:
        """
        Gets common system information using psutil.
        
        Returns:
            A dictionary containing system information
        """
        return {
            "platform": platform.platform(),
            "processor": platform.processor(),
            "physical_cores": psutil.cpu_count(logical=False),
            "logical_cores": psutil.cpu_count(logical=True),
            "memory_total": psutil.virtual_memory().total,
            "memory_available": psutil.virtual_memory().available,
            "disk_partitions": [
                {
                    "device": p.device,
                    "mountpoint": p.mountpoint,
                    "fstype": p.fstype,
                    "total_size": psutil.disk_usage(p.mountpoint).total if p.mountpoint else None,
                    "free_space": psutil.disk_usage(p.mountpoint).free if p.mountpoint else None
                }
                for p in psutil.disk_partitions()
            ],
            "network_interfaces": [
                {
                    "name": iface,
                    "addresses": str(addrs)
                }
                for iface, addrs in psutil.net_if_addrs().items()
            ]
        }
    
    def _measure_system_utilization(self, duration: int = 10, interval: int = 1) -> Dict[str, List[Dict[str, Any]]]:
        """
        Measures system utilization over time.
        
        Args:
            duration: The duration of monitoring in seconds
            interval: The interval between measurements in seconds
            
        Returns:
            A dictionary containing utilization measurements
        """
        measurements = {
            "cpu": [],
            "memory": [],
            "disk_io": [],
            "network_io": []
        }
        
        # Get initial network and disk IO counters
        prev_net_io = psutil.net_io_counters()
        prev_disk_io = psutil.disk_io_counters()
        prev_time = time.time()
        
        # Measure for the specified duration
        end_time = prev_time + duration
        while time.time() < end_time:
            # CPU
            cpu_percent = psutil.cpu_percent(interval=None, percpu=True)
            measurements["cpu"].append({
                "timestamp": time.time(),
                "overall_percent": sum(cpu_percent) / len(cpu_percent),
                "per_cpu_percent": cpu_percent
            })
            
            # Memory
            memory = psutil.virtual_memory()
            measurements["memory"].append({
                "timestamp": time.time(),
                "total": memory.total,
                "available": memory.available,
                "percent": memory.percent,
                "used": memory.used,
                "free": memory.free
            })
            
            # Disk IO
            curr_disk_io = psutil.disk_io_counters()
            curr_time = time.time()
            time_diff = curr_time - prev_time
            
            read_bytes_sec = (curr_disk_io.read_bytes - prev_disk_io.read_bytes) / time_diff
            write_bytes_sec = (curr_disk_io.write_bytes - prev_disk_io.write_bytes) / time_diff
            
            measurements["disk_io"].append({
                "timestamp": curr_time,
                "read_bytes_sec": read_bytes_sec,
                "write_bytes_sec": write_bytes_sec,
                "read_count": curr_disk_io.read_count,
                "write_count": curr_disk_io.write_count
            })
            
            # Network IO
            curr_net_io = psutil.net_io_counters()
            
            sent_bytes_sec = (curr_net_io.bytes_sent - prev_net_io.bytes_sent) / time_diff
            recv_bytes_sec = (curr_net_io.bytes_recv - prev_net_io.bytes_recv) / time_diff
            
            measurements["network_io"].append({
                "timestamp": curr_time,
                "sent_bytes_sec": sent_bytes_sec,
                "recv_bytes_sec": recv_bytes_sec,
                "packets_sent": curr_net_io.packets_sent,
                "packets_recv": curr_net_io.packets_recv
            })
            
            # Update previous values
            prev_disk_io = curr_disk_io
            prev_net_io = curr_net_io
            prev_time = curr_time
            
            # Sleep for the specified interval
            time.sleep(interval)
        
        return measurements


class PostgreSQLResourceAnalyzer(DBResourceAnalyzer):
    """Resource analyzer for PostgreSQL databases."""

    def analyze_server_resources(self, connection: Connection) -> Dict[str, Any]:
        """
        Analyzes server resources for a PostgreSQL database.
        
        Args:
            connection: The database connection
            
        Returns:
            A dictionary containing resource information
        """
        # Get common system info
        system_info = self._get_common_system_info()
        
        # Get PostgreSQL-specific info
        pg_version_query = "SELECT version();"
        data_dir_query = "SHOW data_directory;"
        db_size_query = "SELECT pg_database_size(current_database()) AS size_bytes;"
        tablespace_query = """
            SELECT spcname, pg_tablespace_location(oid) AS location
            FROM pg_tablespace;
        """
        
        pg_info = {}
        
        try:
            # Execute queries
            cursor = connection.cursor()
            
            # Get PostgreSQL version
            cursor.execute(pg_version_query)
            pg_info["version"] = cursor.fetchone()[0]
            
            # Get data directory
            cursor.execute(data_dir_query)
            pg_info["data_directory"] = cursor.fetchone()[0]
            
            # Get database size
            cursor.execute(db_size_query)
            pg_info["database_size_bytes"] = cursor.fetchone()[0]
            
            # Get tablespaces
            cursor.execute(tablespace_query)
            pg_info["tablespaces"] = [
                {"name": row[0], "location": row[1]}
                for row in cursor.fetchall()
            ]
            
            cursor.close()
        except Exception as e:
            logger.error(f"Error getting PostgreSQL server info: {str(e)}")
            pg_info["error"] = str(e)
        
        # Combine system info and PostgreSQL info
        result = {
            "system": system_info,
            "postgresql": pg_info
        }
        
        return result
    
    def analyze_db_configuration(self, connection: Connection) -> Dict[str, Any]:
        """
        Analyzes PostgreSQL database configuration.
        
        Args:
            connection: The database connection
            
        Returns:
            A dictionary containing configuration information
        """
        # Query to get all PostgreSQL configuration settings
        config_query = """
            SELECT name, setting, unit, context, vartype, min_val, max_val, 
                   boot_val, reset_val, source, sourcefile, sourceline
            FROM pg_settings
            ORDER BY name;
        """
        
        memory_query = """
            SELECT name, setting, unit 
            FROM pg_settings 
            WHERE name IN (
                'shared_buffers', 'work_mem', 'maintenance_work_mem', 
                'effective_cache_size', 'max_connections'
            );
        """
        
        wal_query = """
            SELECT name, setting, unit 
            FROM pg_settings 
            WHERE name LIKE 'wal%' OR name IN (
                'archive_mode', 'archive_command', 'max_wal_senders'
            );
        """
        
        autovacuum_query = """
            SELECT name, setting, unit 
            FROM pg_settings 
            WHERE name LIKE 'autovacuum%';
        """
        
        config_data = {
            "all_settings": [],
            "memory_settings": [],
            "wal_settings": [],
            "autovacuum_settings": []
        }
        
        try:
            # Execute queries
            cursor = connection.cursor()
            
            # Get all settings
            cursor.execute(config_query)
            columns = [desc[0] for desc in cursor.description]
            for row in cursor.fetchall():
                setting = dict(zip(columns, row))
                config_data["all_settings"].append(setting)
            
            # Get memory settings
            cursor.execute(memory_query)
            columns = [desc[0] for desc in cursor.description]
            for row in cursor.fetchall():
                setting = dict(zip(columns, row))
                config_data["memory_settings"].append(setting)
            
            # Get WAL settings
            cursor.execute(wal_query)
            columns = [desc[0] for desc in cursor.description]
            for row in cursor.fetchall():
                setting = dict(zip(columns, row))
                config_data["wal_settings"].append(setting)
            
            # Get autovacuum settings
            cursor.execute(autovacuum_query)
            columns = [desc[0] for desc in cursor.description]
            for row in cursor.fetchall():
                setting = dict(zip(columns, row))
                config_data["autovacuum_settings"].append(setting)
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error analyzing PostgreSQL configuration: {str(e)}")
            config_data["error"] = str(e)
        
        return config_data
    
    def monitor_resource_utilization(self, connection: Connection, duration: int, interval: int) -> Dict[str, Any]:
        """
        Monitors PostgreSQL resource utilization over time.
        
        Args:
            connection: The database connection
            duration: The duration of monitoring in seconds
            interval: The interval between measurements in seconds
            
        Returns:
            A dictionary containing utilization information
        """
        # Measure system utilization
        system_utilization = self._measure_system_utilization(duration, interval)
        
        # Query to monitor PostgreSQL-specific metrics
        activity_query = """
            SELECT count(*) as connections
            FROM pg_stat_activity;
        """
        
        stat_database_query = """
            SELECT datname, xact_commit, xact_rollback, blks_read, blks_hit,
                   tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted
            FROM pg_stat_database
            WHERE datname = current_database();
        """
        
        pg_utilization = {
            "activity": [],
            "database_stats": []
        }
        
        try:
            # Measure for the specified duration
            end_time = time.time() + duration
            cursor = connection.cursor()
            
            while time.time() < end_time:
                # Get active connections
                cursor.execute(activity_query)
                connections = cursor.fetchone()[0]
                pg_utilization["activity"].append({
                    "timestamp": time.time(),
                    "connections": connections
                })
                
                # Get database statistics
                cursor.execute(stat_database_query)
                columns = [desc[0] for desc in cursor.description]
                row = cursor.fetchone()
                if row:
                    stats = dict(zip(columns, row))
                    stats["timestamp"] = time.time()
                    pg_utilization["database_stats"].append(stats)
                
                # Sleep for the specified interval
                time.sleep(interval)
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error monitoring PostgreSQL utilization: {str(e)}")
            pg_utilization["error"] = str(e)
        
        # Combine system and PostgreSQL utilization
        result = {
            "system": system_utilization,
            "postgresql": pg_utilization
        }
        
        return result
    
    def generate_recommendations(
        self, 
        connection: Connection, 
        resource_data: Dict[str, Any], 
        config_data: Dict[str, Any], 
        utilization_data: Dict[str, Any]
    ) -> List[Recommendation]:
        """
        Generates PostgreSQL resource optimization recommendations.
        
        Args:
            connection: The database connection
            resource_data: The resource data
            config_data: The configuration data
            utilization_data: The utilization data
            
        Returns:
            A list of Recommendation objects
        """
        recommendations = []
        
        # Extract relevant data
        system_info = resource_data.get("system", {})
        pg_info = resource_data.get("postgresql", {})
        
        memory_settings = {}
        for setting in config_data.get("memory_settings", []):
            memory_settings[setting["name"]] = {
                "value": setting["setting"],
                "unit": setting["unit"]
            }
        
        # Check shared_buffers
        try:
            if "shared_buffers" in memory_settings:
                shared_buffers_val = memory_settings["shared_buffers"]["value"]
                total_memory = system_info.get("memory_total", 0)
                
                # Convert shared_buffers to bytes
                shared_buffers_bytes = self._convert_pg_size_to_bytes(shared_buffers_val)
                
                # Check if shared_buffers is less than 25% of total memory
                if shared_buffers_bytes < total_memory * 0.25:
                    # Calculate recommended value (25% of RAM)
                    recommended_bytes = int(total_memory * 0.25)
                    recommended_val = self._format_bytes_to_pg_size(recommended_bytes)
                    
                    recommendation = Recommendation(
                        title="Increase shared_buffers",
                        description=f"The current shared_buffers setting ({shared_buffers_val}) is less than 25% of available memory. "
                                   f"Consider increasing it to {recommended_val} for better performance.",
                        priority=RecommendationPriority.HIGH,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script=f"ALTER SYSTEM SET shared_buffers = '{recommended_val}';",
                        expected_benefit="Improved query performance by allowing PostgreSQL to cache more data in memory.",
                        risk_level="Medium",
                        risk_details="Requires database restart. May increase memory pressure on the system.",
                        estimated_time="5 minutes plus restart time",
                        category="Memory Configuration"
                    )
                    recommendations.append(recommendation)
                
                # Check if shared_buffers is too high (>40% of RAM)
                elif shared_buffers_bytes > total_memory * 0.4:
                    # Calculate recommended value (30% of RAM)
                    recommended_bytes = int(total_memory * 0.3)
                    recommended_val = self._format_bytes_to_pg_size(recommended_bytes)
                    
                    recommendation = Recommendation(
                        title="Decrease shared_buffers",
                        description=f"The current shared_buffers setting ({shared_buffers_val}) is more than 40% of available memory. "
                                   f"Consider decreasing it to {recommended_val} to leave more memory for the OS and other processes.",
                        priority=RecommendationPriority.MEDIUM,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script=f"ALTER SYSTEM SET shared_buffers = '{recommended_val}';",
                        expected_benefit="Reduced memory pressure on the system while maintaining good database performance.",
                        risk_level="Low",
                        risk_details="Requires database restart. May slightly decrease performance for some queries.",
                        estimated_time="5 minutes plus restart time",
                        category="Memory Configuration"
                    )
                    recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing shared_buffers: {str(e)}")
        
        # Check work_mem
        try:
            if "work_mem" in memory_settings and "max_connections" in memory_settings:
                work_mem_val = memory_settings["work_mem"]["value"]
                max_connections_val = int(memory_settings["max_connections"]["value"])
                
                # Convert work_mem to bytes
                work_mem_bytes = self._convert_pg_size_to_bytes(work_mem_val)
                
                # Check if work_mem * max_connections > 0.3 * total_memory (potential memory pressure)
                total_memory = system_info.get("memory_total", 0)
                if work_mem_bytes * max_connections_val > total_memory * 0.3:
                    # Calculate recommended value
                    recommended_bytes = int((total_memory * 0.3) / max_connections_val)
                    recommended_val = self._format_bytes_to_pg_size(recommended_bytes)
                    
                    recommendation = Recommendation(
                        title="Adjust work_mem setting",
                        description=f"The current work_mem setting ({work_mem_val}) multiplied by max_connections ({max_connections_val}) "
                                   f"could potentially use more than 30% of system memory, risking memory pressure. "
                                   f"Consider decreasing it to {recommended_val}.",
                        priority=RecommendationPriority.MEDIUM,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script=f"ALTER SYSTEM SET work_mem = '{recommended_val}';",
                        expected_benefit="Reduced risk of memory pressure and improved system stability.",
                        risk_level="Low",
                        risk_details="May increase disk I/O for complex queries as they will use disk for sorts and hash operations.",
                        estimated_time="5 minutes plus configuration reload",
                        category="Memory Configuration"
                    )
                    recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing work_mem: {str(e)}")
        
        # Check for I/O configuration issues
        try:
            # Analyze system utilization for I/O bottlenecks
            disk_io_data = utilization_data.get("system", {}).get("disk_io", [])
            if disk_io_data:
                # Calculate average read and write rates
                read_rates = [entry.get("read_bytes_sec", 0) for entry in disk_io_data]
                write_rates = [entry.get("write_bytes_sec", 0) for entry in disk_io_data]
                
                avg_read_rate = sum(read_rates) / len(read_rates) if read_rates else 0
                avg_write_rate = sum(write_rates) / len(write_rates) if write_rates else 0
                
                # Check if I/O rates are high relative to disk capabilities
                # This is a simplified heuristic; real detection would consider disk specs
                high_io_threshold = 50 * 1024 * 1024  # 50 MB/s as an example threshold
                
                if avg_read_rate > high_io_threshold or avg_write_rate > high_io_threshold:
                    # Check if effective_cache_size is set appropriately
                    effective_cache_size_val = ""
                    effective_cache_size_bytes = 0
                    
                    for setting in config_data.get("memory_settings", []):
                        if setting["name"] == "effective_cache_size":
                            effective_cache_size_val = setting["setting"]
                            effective_cache_size_bytes = self._convert_pg_size_to_bytes(effective_cache_size_val)
                            break
                    
                    if effective_cache_size_bytes < total_memory * 0.5:
                        # Calculate recommended value (75% of RAM)
                        recommended_bytes = int(total_memory * 0.75)
                        recommended_val = self._format_bytes_to_pg_size(recommended_bytes)
                        
                        recommendation = Recommendation(
                            title="Increase effective_cache_size for I/O intensive workload",
                            description=f"High disk I/O detected (Read: {self._format_bytes_rate(avg_read_rate)}, "
                                      f"Write: {self._format_bytes_rate(avg_write_rate)}) with effective_cache_size "
                                      f"({effective_cache_size_val}) less than 50% of memory. "
                                      f"Consider increasing to {recommended_val}.",
                            priority=RecommendationPriority.HIGH,
                            type=RecommendationType.CONFIGURATION,
                            implementation_script=f"ALTER SYSTEM SET effective_cache_size = '{recommended_val}';",
                            expected_benefit="Improved query planning and reduced disk I/O.",
                            risk_level="Low",
                            risk_details="This setting only affects the planner's estimates and does not allocate actual memory.",
                            estimated_time="5 minutes plus configuration reload",
                            category="I/O Configuration"
                        )
                        recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing I/O configuration: {str(e)}")
        
        # Check autovacuum settings
        try:
            autovacuum_settings = {}
            for setting in config_data.get("autovacuum_settings", []):
                autovacuum_settings[setting["name"]] = setting["setting"]
            
            # Check if autovacuum is enabled
            if autovacuum_settings.get("autovacuum", "on") == "off":
                recommendation = Recommendation(
                    title="Enable Autovacuum",
                    description="Autovacuum is currently disabled. Enabling autovacuum is crucial for maintaining "
                               "database performance and preventing transaction ID wraparound.",
                    priority=RecommendationPriority.CRITICAL,
                    type=RecommendationType.CONFIGURATION,
                    implementation_script="ALTER SYSTEM SET autovacuum = on;",
                    expected_benefit="Improved long-term performance and database health by automatically reclaiming space and updating statistics.",
                    risk_level="Low",
                    risk_details="Autovacuum processes consume some system resources but prevent serious long-term issues.",
                    estimated_time="5 minutes plus configuration reload",
                    category="Maintenance Configuration"
                )
                recommendations.append(recommendation)
            
            # Check autovacuum_max_workers
            physical_cores = system_info.get("physical_cores", 1)
            autovacuum_max_workers = int(autovacuum_settings.get("autovacuum_max_workers", "3"))
            
            if physical_cores > 4 and autovacuum_max_workers < 4:
                recommended_workers = min(physical_cores // 2, 8)  # Half of cores, but not more than 8
                
                recommendation = Recommendation(
                    title="Increase autovacuum_max_workers",
                    description=f"Current autovacuum_max_workers ({autovacuum_max_workers}) is low for a system with "
                               f"{physical_cores} cores. Consider increasing to {recommended_workers} workers.",
                    priority=RecommendationPriority.MEDIUM,
                    type=RecommendationType.CONFIGURATION,
                    implementation_script=f"ALTER SYSTEM SET autovacuum_max_workers = {recommended_workers};",
                    expected_benefit="More parallel vacuum operations, helping to maintain database performance as it grows.",
                    risk_level="Low",
                    risk_details="Increased CPU usage during vacuum operations, but better long-term performance.",
                    estimated_time="5 minutes plus configuration reload",
                    category="Maintenance Configuration"
                )
                recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing autovacuum settings: {str(e)}")
        
        # Add more PostgreSQL-specific recommendations here
        
        return recommendations
    
    def _convert_pg_size_to_bytes(self, size_val: str) -> int:
        """
        Converts a PostgreSQL size value to bytes.
        
        Args:
            size_val: The size value as a string (e.g., '8MB', '4GB')
            
        Returns:
            The size in bytes
        """
        size_val = size_val.strip().upper()
        
        if size_val.endswith('B'):
            size_val = size_val[:-1]
        
        multipliers = {
            '': 1,
            'K': 1024,
            'M': 1024 * 1024,
            'G': 1024 * 1024 * 1024,
            'T': 1024 * 1024 * 1024 * 1024
        }
        
        if size_val[-1] in multipliers:
            unit = size_val[-1]
            value = float(size_val[:-1])
            return int(value * multipliers[unit])
        else:
            return int(size_val)
    
    def _format_bytes_to_pg_size(self, bytes_val: int) -> str:
        """
        Formats bytes as a PostgreSQL size value.
        
        Args:
            bytes_val: The size in bytes
            
        Returns:
            The formatted size (e.g., '8MB', '4GB')
        """
        if bytes_val >= 1024 * 1024 * 1024 * 1024:
            return f"{bytes_val / (1024 * 1024 * 1024 * 1024):.0f}TB"
        elif bytes_val >= 1024 * 1024 * 1024:
            return f"{bytes_val / (1024 * 1024 * 1024):.0f}GB"
        elif bytes_val >= 1024 * 1024:
            return f"{bytes_val / (1024 * 1024):.0f}MB"
        elif bytes_val >= 1024:
            return f"{bytes_val / 1024:.0f}kB"
        else:
            return f"{bytes_val}"
    
    def _format_bytes_rate(self, bytes_per_sec: float) -> str:
        """
        Formats a byte rate (bytes per second).
        
        Args:
            bytes_per_sec: The rate in bytes per second
            
        Returns:
            The formatted rate (e.g., '5 MB/s')
        """
        if bytes_per_sec >= 1024 * 1024 * 1024:
            return f"{bytes_per_sec / (1024 * 1024 * 1024):.2f} GB/s"
        elif bytes_per_sec >= 1024 * 1024:
            return f"{bytes_per_sec / (1024 * 1024):.2f} MB/s"
        elif bytes_per_sec >= 1024:
            return f"{bytes_per_sec / 1024:.2f} KB/s"
        else:
            return f"{bytes_per_sec:.2f} B/s"


class MySQLResourceAnalyzer(DBResourceAnalyzer):
    """Resource analyzer for MySQL databases."""
    
    def analyze_server_resources(self, connection: Connection) -> Dict[str, Any]:
        """
        Analyzes server resources for a MySQL database.
        
        Args:
            connection: The database connection
            
        Returns:
            A dictionary containing resource information
        """
        # Get common system info
        system_info = self._get_common_system_info()
        
        # Get MySQL-specific info
        version_query = "SELECT VERSION();"
        variables_query = "SHOW GLOBAL VARIABLES LIKE 'datadir';"
        engine_query = "SHOW ENGINES;"
        
        mysql_info = {}
        
        try:
            # Execute queries
            cursor = connection.cursor()
            
            # Get MySQL version
            cursor.execute(version_query)
            mysql_info["version"] = cursor.fetchone()[0]
            
            # Get data directory
            cursor.execute(variables_query)
            data_dir = None
            for row in cursor.fetchall():
                if row[0] == 'datadir':
                    data_dir = row[1]
                    break
            
            mysql_info["data_directory"] = data_dir
            
            # Get available engines
            cursor.execute(engine_query)
            mysql_info["engines"] = [
                {"name": row[0], "support": row[1], "comment": row[2]}
                for row in cursor.fetchall()
            ]
            
            cursor.close()
        except Exception as e:
            logger.error(f"Error getting MySQL server info: {str(e)}")
            mysql_info["error"] = str(e)
        
        # Combine system info and MySQL info
        result = {
            "system": system_info,
            "mysql": mysql_info
        }
        
        return result
    
    def analyze_db_configuration(self, connection: Connection) -> Dict[str, Any]:
        """
        Analyzes MySQL database configuration.
        
        Args:
            connection: The database connection
            
        Returns:
            A dictionary containing configuration information
        """
        # Query to get all MySQL configuration variables
        all_vars_query = "SHOW GLOBAL VARIABLES;"
        
        # Get memory-related variables
        memory_query = """
            SHOW GLOBAL VARIABLES WHERE Variable_name IN (
                'innodb_buffer_pool_size', 'key_buffer_size', 'query_cache_size',
                'tmp_table_size', 'max_connections', 'innodb_log_buffer_size'
            );
        """
        
        # Get InnoDB-related variables
        innodb_query = "SHOW GLOBAL VARIABLES LIKE 'innodb%';"
        
        # Get status variables
        status_query = "SHOW GLOBAL STATUS;"
        
        config_data = {
            "all_variables": [],
            "memory_variables": [],
            "innodb_variables": [],
            "status_variables": []
        }
        
        try:
            # Execute queries
            cursor = connection.cursor()
            
            # Get all variables
            cursor.execute(all_vars_query)
            for row in cursor.fetchall():
                config_data["all_variables"].append({
                    "name": row[0],
                    "value": row[1]
                })
            
            # Get memory variables
            cursor.execute(memory_query)
            for row in cursor.fetchall():
                config_data["memory_variables"].append({
                    "name": row[0],
                    "value": row[1]
                })
            
            # Get InnoDB variables
            cursor.execute(innodb_query)
            for row in cursor.fetchall():
                config_data["innodb_variables"].append({
                    "name": row[0],
                    "value": row[1]
                })
            
            # Get status variables
            cursor.execute(status_query)
            for row in cursor.fetchall():
                config_data["status_variables"].append({
                    "name": row[0],
                    "value": row[1]
                })
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error analyzing MySQL configuration: {str(e)}")
            config_data["error"] = str(e)
        
        return config_data
    
    def monitor_resource_utilization(self, connection: Connection, duration: int, interval: int) -> Dict[str, Any]:
        """
        Monitors MySQL resource utilization over time.
        
        Args:
            connection: The database connection
            duration: The duration of monitoring in seconds
            interval: The interval between measurements in seconds
            
        Returns:
            A dictionary containing utilization information
        """
        # Measure system utilization
        system_utilization = self._measure_system_utilization(duration, interval)
        
        # Query to monitor MySQL-specific metrics
        connections_query = "SHOW STATUS LIKE 'Threads_connected';"
        innodb_metrics_query = """
            SHOW STATUS WHERE Variable_name IN (
                'Innodb_buffer_pool_reads', 'Innodb_buffer_pool_read_requests',
                'Innodb_data_reads', 'Innodb_data_writes', 'Innodb_data_fsyncs'
            );
        """
        query_metrics_query = """
            SHOW STATUS WHERE Variable_name IN (
                'Queries', 'Slow_queries', 'Select_scan', 
                'Sort_scan', 'Sort_rows', 'Created_tmp_disk_tables'
            );
        """
        
        mysql_utilization = {
            "connections": [],
            "innodb_metrics": [],
            "query_metrics": []
        }
        
        try:
            # Measure for the specified duration
            end_time = time.time() + duration
            cursor = connection.cursor()
            
            # Get initial values for rate calculations
            cursor.execute(innodb_metrics_query)
            initial_innodb = {row[0]: int(row[1]) for row in cursor.fetchall()}
            
            cursor.execute(query_metrics_query)
            initial_query = {row[0]: int(row[1]) for row in cursor.fetchall()}
            
            initial_time = time.time()
            
            while time.time() < end_time:
                current_time = time.time()
                
                # Get active connections
                cursor.execute(connections_query)
                connections = int(cursor.fetchone()[1])
                
                mysql_utilization["connections"].append({
                    "timestamp": current_time,
                    "connections": connections
                })
                
                # Get InnoDB metrics
                cursor.execute(innodb_metrics_query)
                current_innodb = {row[0]: int(row[1]) for row in cursor.fetchall()}
                
                time_diff = current_time - initial_time
                
                innodb_rates = {
                    f"{key}_rate": (current_innodb[key] - initial_innodb[key]) / time_diff
                    for key in initial_innodb.keys()
                }
                
                innodb_metrics = {
                    "timestamp": current_time,
                    **current_innodb,
                    **innodb_rates
                }
                
                mysql_utilization["innodb_metrics"].append(innodb_metrics)
                
                # Get query metrics
                cursor.execute(query_metrics_query)
                current_query = {row[0]: int(row[1]) for row in cursor.fetchall()}
                
                query_rates = {
                    f"{key}_rate": (current_query[key] - initial_query[key]) / time_diff
                    for key in initial_query.keys()
                }
                
                query_metrics = {
                    "timestamp": current_time,
                    **current_query,
                    **query_rates
                }
                
                mysql_utilization["query_metrics"].append(query_metrics)
                
                # Update initial values for next iteration
                initial_innodb = current_innodb.copy()
                initial_query = current_query.copy()
                initial_time = current_time
                
                # Sleep for the specified interval
                time.sleep(interval)
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error monitoring MySQL utilization: {str(e)}")
            mysql_utilization["error"] = str(e)
        
        # Combine system and MySQL utilization
        result = {
            "system": system_utilization,
            "mysql": mysql_utilization
        }
        
        return result
    
    def generate_recommendations(
        self, 
        connection: Connection, 
        resource_data: Dict[str, Any], 
        config_data: Dict[str, Any], 
        utilization_data: Dict[str, Any]
    ) -> List[Recommendation]:
        """
        Generates MySQL resource optimization recommendations.
        
        Args:
            connection: The database connection
            resource_data: The resource data
            config_data: The configuration data
            utilization_data: The utilization data
            
        Returns:
            A list of Recommendation objects
        """
        recommendations = []
        
        # Extract relevant data
        system_info = resource_data.get("system", {})
        memory_variables = {}
        
        for var in config_data.get("memory_variables", []):
            memory_variables[var["name"]] = var["value"]
        
        innodb_variables = {}
        for var in config_data.get("innodb_variables", []):
            innodb_variables[var["name"]] = var["value"]
        
        # Check innodb_buffer_pool_size
        try:
            if "innodb_buffer_pool_size" in memory_variables:
                buffer_pool_size = self._parse_mysql_size(memory_variables["innodb_buffer_pool_size"])
                total_memory = system_info.get("memory_total", 0)
                
                # Check if buffer pool is less than 50% of memory
                if buffer_pool_size < total_memory * 0.5:
                    # Calculate recommended value (70% of RAM)
                    recommended_bytes = int(total_memory * 0.7)
                    recommended_val = self._format_mysql_size(recommended_bytes)
                    
                    recommendation = Recommendation(
                        title="Increase innodb_buffer_pool_size",
                        description=f"The current innodb_buffer_pool_size ({memory_variables['innodb_buffer_pool_size']}) "
                                   f"is less than 50% of available memory. Consider increasing it to {recommended_val} "
                                   f"for better performance.",
                        priority=RecommendationPriority.HIGH,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script=f"SET GLOBAL innodb_buffer_pool_size = {recommended_bytes};",
                        expected_benefit="Improved query performance by caching more data in memory.",
                        risk_level="Medium",
                        risk_details="May increase memory pressure on the system if set too high.",
                        estimated_time="5 minutes",
                        category="Memory Configuration"
                    )
                    recommendations.append(recommendation)
                
                # Check if innodb_buffer_pool is too high (>80% of RAM)
                elif buffer_pool_size > total_memory * 0.8:
                    # Calculate recommended value (70% of RAM)
                    recommended_bytes = int(total_memory * 0.7)
                    recommended_val = self._format_mysql_size(recommended_bytes)
                    
                    recommendation = Recommendation(
                        title="Decrease innodb_buffer_pool_size",
                        description=f"The current innodb_buffer_pool_size ({memory_variables['innodb_buffer_pool_size']}) "
                                   f"is more than 80% of available memory. Consider decreasing it to {recommended_val} "
                                   f"to leave more memory for the OS and other processes.",
                        priority=RecommendationPriority.MEDIUM,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script=f"SET GLOBAL innodb_buffer_pool_size = {recommended_bytes};",
                        expected_benefit="Reduced memory pressure on the system while maintaining good database performance.",
                        risk_level="Low",
                        risk_details="May slightly decrease performance for some queries.",
                        estimated_time="5 minutes",
                        category="Memory Configuration"
                    )
                    recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing innodb_buffer_pool_size: {str(e)}")
        
        # Check innodb_flush_method
        try:
            if "innodb_flush_method" in innodb_variables:
                flush_method = innodb_variables["innodb_flush_method"]
                
                # On Linux, O_DIRECT is often better for performance
                if platform.system() == "Linux" and flush_method != "O_DIRECT":
                    recommendation = Recommendation(
                        title="Optimize innodb_flush_method",
                        description="Setting innodb_flush_method to O_DIRECT on Linux can bypass the OS buffer cache "
                                   "and reduce double buffering, potentially improving I/O performance.",
                        priority=RecommendationPriority.MEDIUM,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script="SET GLOBAL innodb_flush_method = 'O_DIRECT';",
                        expected_benefit="Improved I/O performance by avoiding double buffering.",
                        risk_level="Low",
                        risk_details="May not be optimal for all workloads. Test before implementing in production.",
                        estimated_time="5 minutes plus restart",
                        category="I/O Configuration"
                    )
                    recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing innodb_flush_method: {str(e)}")
        
        # Check innodb_log_file_size
        try:
            if "innodb_log_file_size" in innodb_variables:
                log_file_size = self._parse_mysql_size(innodb_variables["innodb_log_file_size"])
                buffer_pool_size = self._parse_mysql_size(memory_variables.get("innodb_buffer_pool_size", "0"))
                
                # InnoDB log file size should be ~25% of buffer pool for busy systems
                recommended_log_size = buffer_pool_size * 0.25
                
                # But not smaller than 128MB or larger than 2GB typically
                recommended_log_size = max(128 * 1024 * 1024, min(recommended_log_size, 2 * 1024 * 1024 * 1024))
                
                if log_file_size < recommended_log_size * 0.5:
                    recommended_val = self._format_mysql_size(int(recommended_log_size))
                    
                    recommendation = Recommendation(
                        title="Increase innodb_log_file_size",
                        description=f"The current innodb_log_file_size ({innodb_variables['innodb_log_file_size']}) "
                                   f"may be too small relative to your buffer pool. Consider increasing it to {recommended_val}.",
                        priority=RecommendationPriority.MEDIUM,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script=f"SET GLOBAL innodb_log_file_size = {int(recommended_log_size)};",
                        expected_benefit="Improved performance for write-intensive workloads and crash recovery.",
                        risk_level="Medium",
                        risk_details="Requires a server restart. Larger log files can increase recovery time after a crash.",
                        estimated_time="10 minutes plus restart",
                        category="I/O Configuration"
                    )
                    recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing innodb_log_file_size: {str(e)}")
        
        # Check max_connections
        try:
            if "max_connections" in memory_variables:
                max_connections = int(memory_variables["max_connections"])
                
                # Check connection utilization
                connections_data = utilization_data.get("mysql", {}).get("connections", [])
                
                if connections_data:
                    # Calculate average, max connections
                    connection_values = [entry["connections"] for entry in connections_data]
                    avg_connections = sum(connection_values) / len(connection_values)
                    max_measured_connections = max(connection_values)
                    
                    # If consistently using >80% of max_connections, it might be too low
                    if max_measured_connections > max_connections * 0.8:
                        recommended_connections = int(max_connections * 1.5)  # 50% increase as a starting point
                        
                        recommendation = Recommendation(
                            title="Increase max_connections",
                            description=f"During monitoring, the database used up to {max_measured_connections} connections "
                                       f"out of the configured maximum of {max_connections}. Consider increasing max_connections "
                                       f"to {recommended_connections} to avoid connection failures.",
                            priority=RecommendationPriority.HIGH,
                            type=RecommendationType.CONFIGURATION,
                            implementation_script=f"SET GLOBAL max_connections = {recommended_connections};",
                            expected_benefit="Prevent connection failures during peak usage.",
                            risk_level="Medium",
                            risk_details="Each connection consumes memory. Ensure the server has enough RAM.",
                            estimated_time="5 minutes",
                            category="Connection Management"
                        )
                        recommendations.append(recommendation)
                    
                    # If consistently using <20% of max_connections, it might be too high
                    elif max_measured_connections < max_connections * 0.2 and max_connections > 100:
                        recommended_connections = max(100, int(max_measured_connections * 2))  # 2x margin
                        
                        recommendation = Recommendation(
                            title="Decrease max_connections",
                            description=f"During monitoring, the database used a maximum of {max_measured_connections} connections "
                                       f"out of the configured maximum of {max_connections}. Consider decreasing max_connections "
                                       f"to {recommended_connections} to conserve server resources.",
                            priority=RecommendationPriority.LOW,
                            type=RecommendationType.CONFIGURATION,
                            implementation_script=f"SET GLOBAL max_connections = {recommended_connections};",
                            expected_benefit="Reduce memory usage by limiting the maximum number of connections.",
                            risk_level="Medium",
                            risk_details="May cause connection failures if traffic suddenly increases.",
                            estimated_time="5 minutes",
                            category="Connection Management"
                        )
                        recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing max_connections: {str(e)}")
        
        # Check for I/O issues
        try:
            innodb_metrics = utilization_data.get("mysql", {}).get("innodb_metrics", [])
            
            if innodb_metrics:
                # Calculate hit ratio
                last_metrics = innodb_metrics[-1]
                
                if ("Innodb_buffer_pool_reads" in last_metrics and 
                    "Innodb_buffer_pool_read_requests" in last_metrics):
                    
                    reads = float(last_metrics["Innodb_buffer_pool_reads"])
                    read_requests = float(last_metrics["Innodb_buffer_pool_read_requests"])
                    
                    if read_requests > 0:
                        hit_ratio = 1 - (reads / read_requests)
                        
                        # If hit ratio is low, suggest increasing buffer pool
                        if hit_ratio < 0.95 and "innodb_buffer_pool_size" in memory_variables:
                            current_size = self._parse_mysql_size(memory_variables["innodb_buffer_pool_size"])
                            total_memory = system_info.get("memory_total", 0)
                            
                            # Calculate recommended value (increase by 50% if possible, up to 75% of RAM)
                            recommended_bytes = min(int(current_size * 1.5), int(total_memory * 0.75))
                            recommended_val = self._format_mysql_size(recommended_bytes)
                            
                            if recommended_bytes > current_size:
                                recommendation = Recommendation(
                                    title="Increase buffer pool size for low hit ratio",
                                    description=f"The current buffer pool hit ratio is {hit_ratio:.2%}, which is below "
                                               f"the recommended 95%. Consider increasing innodb_buffer_pool_size from "
                                               f"{memory_variables['innodb_buffer_pool_size']} to {recommended_val}.",
                                    priority=RecommendationPriority.HIGH,
                                    type=RecommendationType.CONFIGURATION,
                                    implementation_script=f"SET GLOBAL innodb_buffer_pool_size = {recommended_bytes};",
                                    expected_benefit="Improved query performance by increasing buffer pool hit ratio.",
                                    risk_level="Medium",
                                    risk_details="May increase memory pressure on the system.",
                                    estimated_time="5 minutes",
                                    category="Memory Configuration"
                                )
                                recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing buffer pool hit ratio: {str(e)}")
        
        # Check for slow query rate
        try:
            query_metrics = utilization_data.get("mysql", {}).get("query_metrics", [])
            
            if query_metrics and len(query_metrics) > 1:
                # Calculate slow query rate from the last data point
                last_metrics = query_metrics[-1]
                
                if "Slow_queries" in last_metrics and "Queries" in last_metrics and "Slow_queries_rate" in last_metrics:
                    slow_queries = int(last_metrics["Slow_queries"])
                    queries = int(last_metrics["Queries"])
                    slow_query_rate = float(last_metrics["Slow_queries_rate"])
                    
                    if queries > 0:
                        slow_query_percentage = (slow_queries / queries) * 100
                        
                        # If more than 1% of queries are slow, recommend enabling slow query log
                        if slow_query_percentage > 1 or slow_query_rate > 5:
                            recommendation = Recommendation(
                                title="Enable and configure slow query log",
                                description=f"Detected a high rate of slow queries ({slow_query_percentage:.2f}% of all queries). "
                                           f"Consider enabling and properly configuring the slow query log to identify "
                                           f"and optimize problematic queries.",
                                priority=RecommendationPriority.HIGH,
                                type=RecommendationType.CONFIGURATION,
                                implementation_script="""
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL slow_query_log_file = '/var/log/mysql/slow-query.log';
SET GLOBAL long_query_time = 1;
SET GLOBAL log_queries_not_using_indexes = 'ON';
                                """,
                                expected_benefit="Identify slow queries for optimization, improving overall system performance.",
                                risk_level="Low",
                                risk_details="Minimal performance impact, but will generate log files that need to be managed.",
                                estimated_time="5 minutes",
                                category="Performance Monitoring"
                            )
                            recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing slow query rate: {str(e)}")
        
        # Check IO capacity
        try:
            # Analyze system utilization for IO bottlenecks
            disk_io_data = utilization_data.get("system", {}).get("disk_io", [])
            
            if disk_io_data and "innodb_io_capacity" in innodb_variables:
                # Calculate average read and write rates
                read_rates = [entry.get("read_bytes_sec", 0) for entry in disk_io_data]
                write_rates = [entry.get("write_bytes_sec", 0) for entry in disk_io_data]
                
                avg_read_rate = sum(read_rates) / len(read_rates) if read_rates else 0
                avg_write_rate = sum(write_rates) / len(write_rates) if write_rates else 0
                total_io_rate = avg_read_rate + avg_write_rate
                
                # Convert to IOPS (very rough estimate)
                # Assuming average IO size of 16KB
                estimated_iops = total_io_rate / (16 * 1024)
                
                current_io_capacity = int(innodb_variables["innodb_io_capacity"])
                
                # If estimated IOPS is significantly higher than innodb_io_capacity, recommend increasing
                if estimated_iops > current_io_capacity * 1.5:
                    recommended_capacity = max(2000, int(estimated_iops * 1.2))  # At least 2000, with 20% buffer
                    
                    recommendation = Recommendation(
                        title="Increase innodb_io_capacity",
                        description=f"The current innodb_io_capacity ({current_io_capacity}) appears to be lower than "
                                   f"the estimated IOPS capability of your storage ({estimated_iops:.0f}). Consider "
                                   f"increasing it to {recommended_capacity}.",
                        priority=RecommendationPriority.MEDIUM,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script=f"SET GLOBAL innodb_io_capacity = {recommended_capacity};",
                        expected_benefit="Better utilization of storage IO capabilities, potentially improving throughput for write-heavy workloads.",
                        risk_level="Low",
                        risk_details="May increase IO pressure. Set this based on your storage capabilities.",
                        estimated_time="5 minutes",
                        category="IO Configuration"
                    )
                    recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing IO capacity: {str(e)}")
        
        # Add more MySQL-specific recommendations here
        
        return recommendations
    
    def _parse_mysql_size(self, size_str: str) -> int:
        """
        Parse MySQL size string to bytes.
        
        Args:
            size_str: Size string (e.g., '128M', '1G')
            
        Returns:
            Size in bytes
        """
        if not size_str:
            return 0
        
        # Handle if already a number (bytes)
        if isinstance(size_str, (int, float)) or size_str.isdigit():
            return int(size_str)
        
        size_str = size_str.upper().strip()
        
        # Extract numeric part and unit
        if size_str[-1] in 'KMGTP':
            num = float(size_str[:-1])
            unit = size_str[-1]
            
            multipliers = {
                'K': 1024,
                'M': 1024 * 1024,
                'G': 1024 * 1024 * 1024,
                'T': 1024 * 1024 * 1024 * 1024,
                'P': 1024 * 1024 * 1024 * 1024 * 1024
            }
            
            return int(num * multipliers[unit])
        
        # No unit, assume bytes
        return int(size_str)
    
    def _format_mysql_size(self, bytes_val: int) -> str:
        """
        Format bytes to MySQL size string.
        
        Args:
            bytes_val: Size in bytes
            
        Returns:
            Formatted size (e.g., '128M', '1G')
        """
        if bytes_val >= 1024 * 1024 * 1024 * 1024:
            return f"{bytes_val / (1024 * 1024 * 1024 * 1024):.0f}T"
        elif bytes_val >= 1024 * 1024 * 1024:
            return f"{bytes_val / (1024 * 1024 * 1024):.0f}G"
        elif bytes_val >= 1024 * 1024:
            return f"{bytes_val / (1024 * 1024):.0f}M"
        elif bytes_val >= 1024:
            return f"{bytes_val / 1024:.0f}K"
        else:
            return f"{bytes_val}"


class MSSQLResourceAnalyzer(DBResourceAnalyzer):
    """Resource analyzer for Microsoft SQL Server databases."""
    
    def analyze_server_resources(self, connection: Connection) -> Dict[str, Any]:
        """
        Analyzes server resources for a Microsoft SQL Server database.
        
        Args:
            connection: The database connection
            
        Returns:
            A dictionary containing resource information
        """
        # Get common system info
        system_info = self._get_common_system_info()
        
        # Get MSSQL-specific info
        version_query = "SELECT @@VERSION AS version;"
        server_props_query = """
            SELECT 
                SERVERPROPERTY('ProductVersion') AS product_version,
                SERVERPROPERTY('ProductLevel') AS product_level,
                SERVERPROPERTY('Edition') AS edition,
                SERVERPROPERTY('EngineEdition') AS engine_edition,
                SERVERPROPERTY('ProcessID') AS process_id,
                SERVERPROPERTY('ServerName') AS server_name,
                SERVERPROPERTY('MachineName') AS machine_name;
        """
        db_files_query = """
            SELECT 
                DB_NAME(database_id) AS database_name,
                name AS file_name,
                physical_name,
                type_desc,
                state_desc,
                size * 8 * 1024 AS size_bytes,
                max_size * 8 * 1024 AS max_size_bytes,
                growth * 8 * 1024 AS growth_bytes
            FROM sys.master_files
            ORDER BY database_id, type;
        """
        
        mssql_info = {}
        
        try:
            # Execute queries
            cursor = connection.cursor()
            
            # Get SQL Server version
            cursor.execute(version_query)
            mssql_info["version"] = cursor.fetchone()[0]
            
            # Get server properties
            cursor.execute(server_props_query)
            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            if row:
                server_props = dict(zip(columns, row))
                mssql_info["server_properties"] = server_props
            
            # Get database file info
            cursor.execute(db_files_query)
            columns = [desc[0] for desc in cursor.description]
            mssql_info["database_files"] = []
            for row in cursor.fetchall():
                file_info = dict(zip(columns, row))
                mssql_info["database_files"].append(file_info)
            
            cursor.close()
        except Exception as e:
            logger.error(f"Error getting MSSQL server info: {str(e)}")
            mssql_info["error"] = str(e)
        
        # Combine system info and MSSQL info
        result = {
            "system": system_info,
            "mssql": mssql_info
        }
        
        return result
    
    def analyze_db_configuration(self, connection: Connection) -> Dict[str, Any]:
        """
        Analyzes Microsoft SQL Server database configuration.
        
        Args:
            connection: The database connection
            
        Returns:
            A dictionary containing configuration information
        """
        # Get configuration settings
        config_query = """
            SELECT configuration_id, name, value, value_in_use, minimum, maximum, is_dynamic, is_advanced, description
            FROM sys.configurations
            ORDER BY name;
        """
        
        # Get memory configuration
        memory_query = """
            SELECT 
                (SELECT value_in_use FROM sys.configurations WHERE name = 'max server memory (MB)') AS max_server_memory_mb,
                (SELECT value_in_use FROM sys.configurations WHERE name = 'min server memory (MB)') AS min_server_memory_mb,
                (SELECT value_in_use FROM sys.configurations WHERE name = 'cost threshold for parallelism') AS cost_threshold_for_parallelism,
                (SELECT value_in_use FROM sys.configurations WHERE name = 'max degree of parallelism') AS max_dop,
                (SELECT value_in_use FROM sys.configurations WHERE name = 'max worker threads') AS max_worker_threads;
        """
        
        # Get IO configuration
        io_query = """
            SELECT 
                (SELECT value_in_use FROM sys.configurations WHERE name = 'backup compression default') AS backup_compression_default,
                (SELECT value_in_use FROM sys.configurations WHERE name = 'max degree of parallelism') AS max_dop,
                (SELECT value_in_use FROM sys.configurations WHERE name = 'optimize for ad hoc workloads') AS optimize_for_ad_hoc_workloads;
        """
        
        config_data = {
            "all_configurations": [],
            "memory_configuration": {},
            "io_configuration": {}
        }
        
        try:
            # Execute queries
            cursor = connection.cursor()
            
            # Get all configurations
            cursor.execute(config_query)
            columns = [desc[0] for desc in cursor.description]
            for row in cursor.fetchall():
                config = dict(zip(columns, row))
                config_data["all_configurations"].append(config)
            
            # Get memory configuration
            cursor.execute(memory_query)
            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            if row:
                memory_config = dict(zip(columns, row))
                config_data["memory_configuration"] = memory_config
            
            # Get IO configuration
            cursor.execute(io_query)
            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            if row:
                io_config = dict(zip(columns, row))
                config_data["io_configuration"] = io_config
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error analyzing MSSQL configuration: {str(e)}")
            config_data["error"] = str(e)
        
        return config_data
    
    def monitor_resource_utilization(self, connection: Connection, duration: int, interval: int) -> Dict[str, Any]:
        """
        Monitors Microsoft SQL Server resource utilization over time.
        
        Args:
            connection: The database connection
            duration: The duration of monitoring in seconds
            interval: The interval between measurements in seconds
            
        Returns:
            A dictionary containing utilization information
        """
        # Measure system utilization
        system_utilization = self._measure_system_utilization(duration, interval)
        
        # Query to monitor MSSQL-specific metrics
        connections_query = """
            SELECT 
                COUNT(*) AS connection_count
            FROM sys.dm_exec_connections;
        """
        
        memory_query = """
            SELECT 
                (total_physical_memory_kb * 1024) AS total_physical_memory_bytes,
                (available_physical_memory_kb * 1024) AS available_physical_memory_bytes,
                (total_page_file_kb * 1024) AS total_page_file_bytes,
                (available_page_file_kb * 1024) AS available_page_file_bytes,
                (system_memory_state_desc) AS system_memory_state
            FROM sys.dm_os_sys_memory;
        """
        
        buffer_query = """
            SELECT 
                (COUNT(*) * 8 * 1024) AS buffer_pool_size_bytes,
                (COUNT(*) * 8 / 1024) AS buffer_pool_size_mb,
                (SUM(CAST(free_pages_kb AS BIGINT)) * 1024) AS free_buffer_bytes
            FROM sys.dm_os_buffer_descriptors;
        """
        
        waits_query = """
            SELECT 
                wait_type, 
                waiting_tasks_count, 
                wait_time_ms,
                max_wait_time_ms,
                signal_wait_time_ms
            FROM sys.dm_os_wait_stats
            WHERE wait_type NOT LIKE 'SLEEP%'
            AND wait_type NOT LIKE 'LAZY%'
            ORDER BY wait_time_ms DESC;
        """
        
        mssql_utilization = {
            "connections": [],
            "memory": [],
            "buffer_pool": [],
            "waits": []
        }
        
        try:
            # Measure for the specified duration
            end_time = time.time() + duration
            cursor = connection.cursor()
            
            # Initial wait stats for differential calculation
            cursor.execute(waits_query)
            columns = [desc[0] for desc in cursor.description]
            initial_waits = {}
            for row in cursor.fetchall():
                wait_info = dict(zip(columns, row))
                initial_waits[wait_info["wait_type"]] = wait_info
            
            while time.time() < end_time:
                current_time = time.time()
                
                # Get active connections
                cursor.execute(connections_query)
                row = cursor.fetchone()
                mssql_utilization["connections"].append({
                    "timestamp": current_time,
                    "connection_count": row[0] if row else 0
                })
                
                # Get memory stats
                cursor.execute(memory_query)
                columns = [desc[0] for desc in cursor.description]
                row = cursor.fetchone()
                if row:
                    memory_stats = dict(zip(columns, row))
                    memory_stats["timestamp"] = current_time
                    mssql_utilization["memory"].append(memory_stats)
                
                # Get buffer pool stats
                cursor.execute(buffer_query)
                columns = [desc[0] for desc in cursor.description]
                row = cursor.fetchone()
                if row:
                    buffer_stats = dict(zip(columns, row))
                    buffer_stats["timestamp"] = current_time
                    mssql_utilization["buffer_pool"].append(buffer_stats)
                
                # Get wait stats
                cursor.execute(waits_query)
                columns = [desc[0] for desc in cursor.description]
                current_waits = {}
                for row in cursor.fetchall():
                    wait_info = dict(zip(columns, row))
                    current_waits[wait_info["wait_type"]] = wait_info
                
                # Calculate differential wait stats
                wait_stats = {
                    "timestamp": current_time,
                    "wait_stats": []
                }
                
                for wait_type, current in current_waits.items():
                    if wait_type in initial_waits:
                        initial = initial_waits[wait_type]
                        diff_wait = {
                            "wait_type": wait_type,
                            "waiting_tasks_count_diff": current["waiting_tasks_count"] - initial["waiting_tasks_count"],
                            "wait_time_ms_diff": current["wait_time_ms"] - initial["wait_time_ms"],
                            "signal_wait_time_ms_diff": current["signal_wait_time_ms"] - initial["signal_wait_time_ms"]
                        }
                        wait_stats["wait_stats"].append(diff_wait)
                
                mssql_utilization["waits"].append(wait_stats)
                
                # Update initial waits for next iteration
                initial_waits = current_waits
                
                # Sleep for the specified interval
                time.sleep(interval)
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error monitoring MSSQL utilization: {str(e)}")
            mssql_utilization["error"] = str(e)
        
        # Combine system and MSSQL utilization
        result = {
            "system": system_utilization,
            "mssql": mssql_utilization
        }
        
        return result
    
    def generate_recommendations(
        self, 
        connection: Connection, 
        resource_data: Dict[str, Any], 
        config_data: Dict[str, Any], 
        utilization_data: Dict[str, Any]
    ) -> List[Recommendation]:
        """
        Generates Microsoft SQL Server resource optimization recommendations.
        
        Args:
            connection: The database connection
            resource_data: The resource data
            config_data: The configuration data
            utilization_data: The utilization data
            
        Returns:
            A list of Recommendation objects
        """
        recommendations = []
        
        # Extract relevant data
        system_info = resource_data.get("system", {})
        memory_config = config_data.get("memory_configuration", {})
        
        # Check max server memory
        try:
            if "max_server_memory_mb" in memory_config:
                max_memory_mb = int(memory_config["max_server_memory_mb"])
                total_memory_bytes = system_info.get("memory_total", 0)
                total_memory_mb = total_memory_bytes / (1024 * 1024)
                
                # Default max server memory is often too high (2147483647 MB)
                if max_memory_mb > total_memory_mb * 0.9 or max_memory_mb > 2000000:
                    # Calculate recommended value (75% of RAM)
                    recommended_mb = int(total_memory_mb * 0.75)
                    
                    recommendation = Recommendation(
                        title="Configure max server memory",
                        description=f"The current max server memory setting ({max_memory_mb} MB) is not properly configured. "
                                   f"Consider setting it to {recommended_mb} MB (75% of total memory) to leave enough "
                                   f"memory for the OS and other processes.",
                        priority=RecommendationPriority.HIGH,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script=f"EXEC sp_configure 'show advanced options', 1; RECONFIGURE; "
                                            f"EXEC sp_configure 'max server memory', {recommended_mb}; RECONFIGURE;",
                        expected_benefit="Prevent excessive memory pressure on the system while maintaining good database performance.",
                        risk_level="Low",
                        risk_details="May slightly affect performance if currently using more memory.",
                        estimated_time="5 minutes",
                        category="Memory Configuration"
                    )
                    recommendations.append(recommendation)
                
                # If max memory is too low (<40% of total)
                elif max_memory_mb < total_memory_mb * 0.4 and max_memory_mb < 10000:
                    # Calculate recommended value (75% of RAM)
                    recommended_mb = int(total_memory_mb * 0.75)
                    
                    recommendation = Recommendation(
                        title="Increase max server memory",
                        description=f"The current max server memory setting ({max_memory_mb} MB) is less than 40% of total memory. "
                                   f"Consider increasing it to {recommended_mb} MB (75% of total memory) for better performance.",
                        priority=RecommendationPriority.MEDIUM,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script=f"EXEC sp_configure 'show advanced options', 1; RECONFIGURE; "
                                            f"EXEC sp_configure 'max server memory', {recommended_mb}; RECONFIGURE;",
                        expected_benefit="Improved query performance by using more memory for buffer pool and caching.",
                        risk_level="Low",
                        risk_details="Monitor system for memory pressure after increasing.",
                        estimated_time="5 minutes",
                        category="Memory Configuration"
                    )
                    recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing max server memory: {str(e)}")
        
        # Check cost threshold for parallelism
        try:
            if "cost_threshold_for_parallelism" in memory_config:
                cost_threshold = int(memory_config["cost_threshold_for_parallelism"])
                
                # Default cost threshold (5) is often too low
                if cost_threshold < 25:
                    recommendation = Recommendation(
                        title="Increase cost threshold for parallelism",
                        description=f"The current cost threshold for parallelism ({cost_threshold}) is likely too low. "
                                   f"This can cause excessive parallelism for smaller queries. Consider increasing to 50.",
                        priority=RecommendationPriority.MEDIUM,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script="EXEC sp_configure 'show advanced options', 1; RECONFIGURE; "
                                           "EXEC sp_configure 'cost threshold for parallelism', 50; RECONFIGURE;",
                        expected_benefit="Improved CPU utilization by reserving parallelism for complex queries.",
                        risk_level="Low",
                        risk_details="May decrease performance for some queries that benefit from parallelism.",
                        estimated_time="5 minutes",
                        category="Query Processor Configuration"
                    )
                    recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing cost threshold for parallelism: {str(e)}")
        
        # Check max degree of parallelism (MAXDOP)
        try:
            if "max_dop" in memory_config:
                max_dop = int(memory_config["max_dop"])
                logical_cores = system_info.get("logical_cores", 1)
                
                # Default MAXDOP (0 = unlimited) is often not optimal
                if max_dop == 0 and logical_cores > 8:
                    # Calculate recommended MAXDOP based on number of cores
                    recommended_dop = min(8, logical_cores // 2)
                    
                    recommendation = Recommendation(
                        title="Configure max degree of parallelism (MAXDOP)",
                        description=f"The current MAXDOP setting (0 = unlimited) on a server with {logical_cores} "
                                   f"logical cores can cause excessive parallelism. Consider setting MAXDOP to {recommended_dop}.",
                        priority=RecommendationPriority.MEDIUM,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script=f"EXEC sp_configure 'show advanced options', 1; RECONFIGURE; "
                                            f"EXEC sp_configure 'max degree of parallelism', {recommended_dop}; RECONFIGURE;",
                        expected_benefit="Improved CPU utilization and query performance by limiting excessive parallelism.",
                        risk_level="Low",
                        risk_details="May affect performance of some queries. Monitor after implementation.",
                        estimated_time="5 minutes",
                        category="Query Processor Configuration"
                    )
                    recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing max degree of parallelism: {str(e)}")
        
        # Check for IO issues based on wait stats
        try:
            waits_data = utilization_data.get("mssql", {}).get("waits", [])
            
            if waits_data:
                # Get the most recent wait stats
                last_waits = waits_data[-1]["wait_stats"]
                
                # Check for IO-related waits
                io_wait_types = ["PAGEIOLATCH", "IO_COMPLETION", "WRITE_COMPLETION", "ASYNC_IO_COMPLETION"]
                
                io_waits = [
                    wait for wait in last_waits 
                    if any(io_type in wait["wait_type"] for io_type in io_wait_types)
                    and wait["wait_time_ms_diff"] > 1000  # Significant wait time
                ]
                
                if io_waits:
                    # Check if backup compression is enabled
                    io_config = config_data.get("io_configuration", {})
                    backup_compression = io_config.get("backup_compression_default", 0)
                    
                    if backup_compression == 0:
                        recommendation = Recommendation(
                            title="Enable backup compression",
                            description="Significant IO-related waits detected. Enabling backup compression can reduce IO "
                                       "pressure during backup operations at the cost of increased CPU usage.",
                            priority=RecommendationPriority.MEDIUM,
                            type=RecommendationType.CONFIGURATION,
                            implementation_script="EXEC sp_configure 'show advanced options', 1; RECONFIGURE; "
                                               "EXEC sp_configure 'backup compression default', 1; RECONFIGURE;",
                            expected_benefit="Reduced IO pressure and potentially faster backups.",
                            risk_level="Low",
                            risk_details="Will increase CPU usage during backups.",
                            estimated_time="5 minutes",
                            category="IO Configuration"
                        )
                        recommendations.append(recommendation)
                    
                    # Check if optimize for ad hoc workloads is enabled
                    optimize_ad_hoc = io_config.get("optimize_for_ad_hoc_workloads", 0)
                    
                    if optimize_ad_hoc == 0:
                        recommendation = Recommendation(
                            title="Enable 'optimize for ad hoc workloads'",
                            description="Significant IO-related waits detected. Enabling 'optimize for ad hoc workloads' "
                                       "can reduce memory pressure and improve plan cache efficiency for systems with many "
                                       "ad hoc queries.",
                            priority=RecommendationPriority.MEDIUM,
                            type=RecommendationType.CONFIGURATION,
                            implementation_script="EXEC sp_configure 'show advanced options', 1; RECONFIGURE; "
                                               "EXEC sp_configure 'optimize for ad hoc workloads', 1; RECONFIGURE;",
                            expected_benefit="Improved memory utilization and potentially reduced IO.",
                            risk_level="Low",
                            risk_details="Minimal risk for most workloads.",
                            estimated_time="5 minutes",
                            category="Memory Configuration"
                        )
                        recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing wait stats: {str(e)}")
        
        # Add more MSSQL-specific recommendations here
        
        return recommendations


class SQLiteResourceAnalyzer(DBResourceAnalyzer):
    """Resource analyzer for SQLite databases."""
    
    def analyze_server_resources(self, connection: Connection) -> Dict[str, Any]:
        """
        Analyzes server resources for a SQLite database.
        
        Args:
            connection: The database connection
            
        Returns:
            A dictionary containing resource information
        """
        # Get common system info
        system_info = self._get_common_system_info()
        
        # For SQLite, there's no server, but we can analyze the database file
        version_query = "SELECT sqlite_version();"
        pragma_query = "PRAGMA page_count; PRAGMA page_size; PRAGMA freelist_count;"
        
        sqlite_info = {}
        
        try:
            # Execute queries
            cursor = connection.cursor()
            
            # Get SQLite version
            cursor.execute(version_query)
            sqlite_info["version"] = cursor.fetchone()[0]
            
            # Get database file info
            # Get page count
            cursor.execute("PRAGMA page_count;")
            page_count = cursor.fetchone()[0]
            
            # Get page size
            cursor.execute("PRAGMA page_size;")
            page_size = cursor.fetchone()[0]
            
            # Get free list count
            cursor.execute("PRAGMA freelist_count;")
            freelist_count = cursor.fetchone()[0]
            
            # Calculate database size
            total_pages = page_count
            free_pages = freelist_count
            used_pages = total_pages - free_pages
            
            sqlite_info["database_info"] = {
                "page_count": page_count,
                "page_size": page_size,
                "freelist_count": freelist_count,
                "total_size_bytes": page_count * page_size,
                "used_size_bytes": used_pages * page_size,
                "free_size_bytes": free_pages * page_size
            }
            
            # Get table info
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            table_info = []
            for table in tables:
                table_name = table[0]
                
                # Skip internal SQLite tables
                if table_name.startswith('sqlite_'):
                    continue
                
                # Get row count (approximate)
                cursor.execute(f"SELECT COUNT(*) FROM '{table_name}';")
                row_count = cursor.fetchone()[0]
                
                # Get table schema
                cursor.execute(f"PRAGMA table_info('{table_name}');")
                columns = cursor.fetchall()
                
                # Get index info
                cursor.execute(f"PRAGMA index_list('{table_name}');")
                indexes = cursor.fetchall()
                
                table_info.append({
                    "name": table_name,
                    "row_count": row_count,
                    "column_count": len(columns),
                    "index_count": len(indexes)
                })
            
            sqlite_info["tables"] = table_info
            
            cursor.close()
        except Exception as e:
            logger.error(f"Error getting SQLite info: {str(e)}")
            sqlite_info["error"] = str(e)
        
        # Combine system info and SQLite info
        result = {
            "system": system_info,
            "sqlite": sqlite_info
        }
        
        return result
    
    def analyze_db_configuration(self, connection: Connection) -> Dict[str, Any]:
        """
        Analyzes SQLite database configuration.
        
        Args:
            connection: The database connection
            
        Returns:
            A dictionary containing configuration information
        """
        # For SQLite, most configuration is done through PRAGMA statements
        pragma_list = [
            "auto_vacuum", "automatic_index", "busy_timeout", "cache_size", "case_sensitive_like",
            "cell_size_check", "checkpoint_fullfsync", "foreign_keys", "fullfsync", "ignore_check_constraints",
            "journal_mode", "journal_size_limit", "legacy_file_format", "locking_mode", "max_page_count",
            "mmap_size", "page_size", "recursive_triggers", "reverse_unordered_selects", "secure_delete",
            "synchronous", "temp_store", "wal_autocheckpoint"
        ]
        
        config_data = {
            "pragmas": {}
        }
        
        try:
            cursor = connection.cursor()
            
            # Get values for each PRAGMA
            for pragma in pragma_list:
                try:
                    cursor.execute(f"PRAGMA {pragma};")
                    result = cursor.fetchone()
                    if result is not None:
                        config_data["pragmas"][pragma] = result[0]
                except Exception as e:
                    logger.warning(f"Error getting PRAGMA {pragma}: {str(e)}")
                    config_data["pragmas"][pragma] = None
            
            # Get the database file path (only works for databases opened with a file path)
            try:
                cursor.execute("PRAGMA database_list;")
                database_list = cursor.fetchall()
                config_data["database_list"] = [
                    {"seq": row[0], "name": row[1], "file": row[2]}
                    for row in database_list
                ]
            except Exception as e:
                logger.warning(f"Error getting database list: {str(e)}")
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error analyzing SQLite configuration: {str(e)}")
            config_data["error"] = str(e)
        
        return config_data
    
    def monitor_resource_utilization(self, connection: Connection, duration: int, interval: int) -> Dict[str, Any]:
        """
        Monitors SQLite resource utilization over time.
        
        Args:
            connection: The database connection
            duration: The duration of monitoring in seconds
            interval: The interval between measurements in seconds
            
        Returns:
            A dictionary containing utilization information
        """
        # Measure system utilization
        system_utilization = self._measure_system_utilization(duration, interval)
        
        # For SQLite, we can monitor stats counters
        stats_query = "PRAGMA stats;"
        
        sqlite_utilization = {
            "stats": []
        }
        
        try:
            # Reset stats
            cursor = connection.cursor()
            
            # SQLite 3.8.0+ has a stats PRAGMA
            try:
                cursor.execute("PRAGMA stats_reset;")
            except Exception:
                # Stats not supported or can't be reset
                pass
            
            # Measure for the specified duration
            end_time = time.time() + duration
            
            while time.time() < end_time:
                current_time = time.time()
                
                # Get stats
                try:
                    cursor.execute(stats_query)
                    stats = cursor.fetchall()
                    
                    if stats:
                        stats_data = {
                            "timestamp": current_time,
                            "entries": [
                                {"key": row[0], "value": row[1]}
                                for row in stats
                            ]
                        }
                        sqlite_utilization["stats"].append(stats_data)
                except Exception:
                    # Stats not supported
                    pass
                
                # Sleep for the specified interval
                time.sleep(interval)
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error monitoring SQLite utilization: {str(e)}")
            sqlite_utilization["error"] = str(e)
        
        # Combine system and SQLite utilization
        result = {
            "system": system_utilization,
            "sqlite": sqlite_utilization
        }
        
        return result
    
    def generate_recommendations(
        self, 
        connection: Connection, 
        resource_data: Dict[str, Any], 
        config_data: Dict[str, Any], 
        utilization_data: Dict[str, Any]
    ) -> List[Recommendation]:
        """
        Generates SQLite resource optimization recommendations.
        
        Args:
            connection: The database connection
            resource_data: The resource data
            config_data: The configuration data
            utilization_data: The utilization data
            
        Returns:
            A list of Recommendation objects
        """
        recommendations = []
        
        # Extract relevant data
        system_info = resource_data.get("system", {})
        sqlite_info = resource_data.get("sqlite", {})
        pragmas = config_data.get("pragmas", {})
        
        # Check journal mode
        try:
            journal_mode = pragmas.get("journal_mode", "").upper()
            
            # WAL mode is generally better for concurrent access
            if journal_mode != "WAL":
                recommendation = Recommendation(
                    title="Enable WAL journal mode",
                    description="SQLite is currently using {journal_mode} journal mode. Consider switching to WAL "
                               "(Write-Ahead Logging) mode for better concurrency and performance.",
                    priority=RecommendationPriority.HIGH,
                    type=RecommendationType.CONFIGURATION,
                    implementation_script="PRAGMA journal_mode = WAL;",
                    expected_benefit="Improved concurrency and performance by allowing reads to occur concurrently with writes.",
                    risk_level="Low",
                    risk_details="May require additional setup for some applications. WAL mode requires proper file permissions.",
                    estimated_time="1 minute",
                    category="Storage Configuration"
                )
                recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing journal mode: {str(e)}")
        
        # Check synchronous setting
        try:
            synchronous = pragmas.get("synchronous", 2)
            
            # NORMAL (1) is a good balance for most applications
            if synchronous > 1:
                recommendation = Recommendation(
                    title="Optimize synchronous setting",
                    description=f"SQLite is using synchronous level {synchronous} (FULL). Consider changing to NORMAL (1) "
                              f"for better performance while maintaining reasonable safety.",
                    priority=RecommendationPriority.MEDIUM,
                    type=RecommendationType.CONFIGURATION,
                    implementation_script="PRAGMA synchronous = NORMAL;",
                    expected_benefit="Improved write performance while maintaining protection against corruption in most cases.",
                    risk_level="Medium",
                    risk_details="Slightly increases risk of database corruption in case of power failure or OS crash.",
                    estimated_time="1 minute",
                    category="I/O Configuration"
                )
                recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing synchronous setting: {str(e)}")
        
        # Check cache size
        try:
            cache_size = pragmas.get("cache_size", 0)
            
            # Calculate a reasonable cache size based on available memory
            total_memory = system_info.get("memory_total", 0)
            available_memory = system_info.get("memory_available", 0)
            
            # Convert cache_size to KB if negative (SQLite stores negative values as KB)
            cache_size_kb = abs(cache_size) if cache_size < 0 else cache_size // 1024
            
            # Recommend a larger cache if it's small
            if cache_size_kb < 10000 and available_memory > 100 * 1024 * 1024:  # 100 MB available
                # Recommend 5% of available memory, but not more than 100 MB
                recommended_kb = min(int(available_memory / 1024 * 0.05), 100 * 1024)
                
                if recommended_kb > cache_size_kb * 2:  # Only recommend if significantly larger
                    recommendation = Recommendation(
                        title="Increase cache size",
                        description=f"SQLite cache size is currently {cache_size_kb} KB. Consider increasing to "
                                  f"{recommended_kb} KB for better performance.",
                        priority=RecommendationPriority.MEDIUM,
                        type=RecommendationType.CONFIGURATION,
                        implementation_script=f"PRAGMA cache_size = -{recommended_kb};",
                        expected_benefit="Improved query performance by caching more data in memory.",
                        risk_level="Low",
                        risk_details="May increase memory usage.",
                        estimated_time="1 minute",
                        category="Memory Configuration"
                    )
                    recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing cache size: {str(e)}")
        
        # Check for missing indexes on large tables
        try:
            database_info = sqlite_info.get("database_info", {})
            tables = sqlite_info.get("tables", [])
            
            # Look for large tables with few indexes
            for table in tables:
                row_count = table.get("row_count", 0)
                index_count = table.get("index_count", 0)
                
                if row_count > 10000 and index_count == 0:
                    recommendation = Recommendation(
                        title=f"Missing indexes on large table '{table['name']}'",
                        description=f"Table '{table['name']}' has {row_count} rows but no indexes. "
                                  f"Consider adding indexes to improve query performance.",
                        priority=RecommendationPriority.HIGH,
                        type=RecommendationType.SCHEMA,
                        implementation_script="-- Example: CREATE INDEX idx_{0}_id ON {0}(id);".format(table['name']),
                        expected_benefit="Greatly improved query performance for searches on indexed columns.",
                        risk_level="Low",
                        risk_details="Will increase storage space slightly and slow down writes.",
                        estimated_time="5 minutes",
                        category="Schema Optimization"
                    )
                    recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing indexes: {str(e)}")
        
        # Check for fragmentation
        try:
            database_info = sqlite_info.get("database_info", {})
            page_count = database_info.get("page_count", 0)
            freelist_count = database_info.get("freelist_count", 0)
            
            if page_count > 1000 and freelist_count > page_count * 0.2:
                # Database has significant fragmentation
                recommendation = Recommendation(
                    title="Vacuum fragmented database",
                    description=f"The database has {freelist_count} free pages out of {page_count} total pages "
                              f"({(freelist_count / page_count) * 100:.1f}% fragmentation). Consider running VACUUM "
                              f"to defragment the database.",
                    priority=RecommendationPriority.MEDIUM,
                    type=RecommendationType.MAINTENANCE,
                    implementation_script="VACUUM;",
                    expected_benefit="Reduced file size and potentially improved performance.",
                    risk_level="Low",
                    risk_details="Takes time for large databases and temporarily doubles storage space during execution.",
                    estimated_time=f"{page_count // 100000 + 1} minutes",
                    category="Maintenance"
                )
                recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing fragmentation: {str(e)}")
        
        # Check auto_vacuum setting
        try:
            auto_vacuum = pragmas.get("auto_vacuum", 0)
            
            if auto_vacuum == 0 and database_info.get("total_size_bytes", 0) > 100 * 1024 * 1024:
                # For databases > 100 MB, incremental vacuum might be better
                recommendation = Recommendation(
                    title="Enable incremental vacuum",
                    description="The database has auto_vacuum disabled. For large databases, enabling incremental "
                              "vacuum can help manage file size without the performance impact of full vacuum.",
                    priority=RecommendationPriority.LOW,
                    type=RecommendationType.CONFIGURATION,
                    implementation_script="""
PRAGMA auto_vacuum = INCREMENTAL;
-- Note: This requires a VACUUM to take effect
VACUUM;
                    """,
                    expected_benefit="Ability to reclaim space incrementally without full vacuum operations.",
                    risk_level="Medium",
                    risk_details="Requires a one-time VACUUM and slightly increases storage overhead.",
                    estimated_time="Varies with database size",
                    category="Storage Configuration"
                )
                recommendations.append(recommendation)
        except Exception as e:
            logger.error(f"Error analyzing auto_vacuum: {str(e)}")
        
        # Add more SQLite-specific recommendations here
        
        return recommendations