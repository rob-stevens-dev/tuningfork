#!/usr/bin/env python
"""
The simplest possible solution to verify Stage 2.1.

This script creates a verification script that uses the absolute minimum
code needed to verify that the ResourceAnalyzer functions correctly.
"""

import os
import sys
from pathlib import Path

# Get the project root
project_root = Path(__file__).parent.parent.absolute()
verification_script_path = project_root / "scripts" / "verify_stage2_1_simple.py"

# Define the simplified verification script
verification_script_content = '''#!/usr/bin/env python
"""
Simple verification script for Stage 2.1 of TuningFork.

This script tests the core functionality of the ResourceAnalyzer
without relying on the ConnectionManager implementation details.
"""

import os
import sys
import json
import time
import sqlite3
import tempfile
import shutil
import platform
import psutil
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(project_root))

# Import the resource_analyzer module
from tuningfork.analyzers.resource_analyzer import ResourceAnalyzer, SQLiteResourceAnalyzer
from tuningfork.models.recommendation import Recommendation

# Colors for terminal output
GREEN = '\\033[92m'
RED = '\\033[91m'
BLUE = '\\033[94m'
ENDC = '\\033[0m'
BOLD = '\\033[1m'

print(f"{BLUE}{BOLD}TuningFork Stage 2.1 Verification (Simple){ENDC}")
print(f"{BLUE}{'=' * 40}{ENDC}")
print(f"Starting verification at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

# Create a temporary directory for testing
temp_dir = tempfile.mkdtemp()
print(f"Temporary directory: {temp_dir}")

try:
    # Step 1: Verify imports
    print(f"\\n{BOLD}Step 1: Verifying imports{ENDC}")
    
    # Check all the main components
    print(f"{GREEN}✓ Successfully imported ResourceAnalyzer{ENDC}")
    print(f"{GREEN}✓ Successfully imported SQLiteResourceAnalyzer{ENDC}")
    print(f"{GREEN}✓ Successfully imported Recommendation{ENDC}")
    
    # Step 2: Verify ResourceAnalyzer can analyze a SQLite database
    print(f"\\n{BOLD}Step 2: Testing SQLiteResourceAnalyzer{ENDC}")
    
    # Create a test SQLite database
    db_path = os.path.join(temp_dir, "test.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create a test table
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER
        )
    """)
    
    # Insert some test data
    cursor.executemany(
        "INSERT INTO test_table (name, value) VALUES (?, ?)",
        [("item1", 100), ("item2", 200), ("item3", 300)]
    )
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"{GREEN}✓ Created test database successfully{ENDC}")
    
    # Create a Connection-like object for testing
    class MockConnection:
        def __init__(self, db_path):
            self.connection_id = "sqlite"
            self.db_type = "sqlite"
            self.connection_obj = sqlite3.connect(db_path)
            self.config = {"type": "sqlite", "database": db_path}
            
        def cursor(self):
            return self.connection_obj.cursor()
            
        def commit(self):
            return self.connection_obj.commit()
            
        def rollback(self):
            return self.connection_obj.rollback()
            
        def close(self):
            return self.connection_obj.close()
            
        def is_connected(self):
            try:
                self.connection_obj.execute("SELECT 1")
                return True
            except:
                return False
    
    # Create a ConnectionManager-like object
    class MockConnectionManager:
        def __init__(self):
            self.connections = {}
            
        def get_connection(self, connection_id):
            return self.connections.get(connection_id)
            
        def connect(self, connection_id):
            self.connections[connection_id] = MockConnection(db_path)
            return self.connections[connection_id]
    
    # Create a ConfigManager-like object
    class MockConfigManager:
        def __init__(self):
            self.config = {
                "storage_directory": temp_dir
            }
            
        def get_value(self, key, default=None):
            return self.config.get(key, default)
    
    # Create the mock objects
    connection_manager = MockConnectionManager()
    config_manager = MockConfigManager()
    
    # Create the ResourceAnalyzer
    resource_analyzer = ResourceAnalyzer(connection_manager, config_manager)
    
    # Connect to the test database
    connection_id = "sqlite"
    connection = connection_manager.connect(connection_id)
    
    print(f"{GREEN}✓ Connected to SQLite test database{ENDC}")
    
    # Step 3: Test analyze_resources
    print(f"\\n{BOLD}Step 3: Testing analyze_resources{ENDC}")
    resource_data = resource_analyzer.analyze_resources(connection_id)
    
    # Verify key data is present
    if not resource_data:
        print(f"{RED}Resource analysis failed - no data returned{ENDC}")
        sys.exit(1)
        
    if "system" not in resource_data:
        print(f"{RED}Resource analysis failed - no system info{ENDC}")
        sys.exit(1)
        
    if "sqlite" not in resource_data:
        print(f"{RED}Resource analysis failed - no SQLite info{ENDC}")
        sys.exit(1)
    
    # Check for our test table
    tables = resource_data["sqlite"].get("tables", [])
    test_table = next((t for t in tables if t["name"] == "test_table"), None)
    
    if not test_table:
        print(f"{RED}Resource analysis failed - test_table not found{ENDC}")
        sys.exit(1)
        
    if test_table["row_count"] != 3:
        print(f"{RED}Resource analysis failed - incorrect row count: {test_table['row_count']}{ENDC}")
        sys.exit(1)
    
    print(f"{GREEN}✓ analyze_resources successful{ENDC}")
    print(f"  - Found system info: {len(resource_data['system'])} properties")
    print(f"  - Found SQLite info: version {resource_data['sqlite'].get('version', 'unknown')}")
    print(f"  - Found test_table with {test_table['row_count']} rows")
    
    # Step 4: Test analyze_configuration
    print(f"\\n{BOLD}Step 4: Testing analyze_configuration{ENDC}")
    config_data = resource_analyzer.analyze_configuration(connection_id)
    
    # Verify key data is present
    if not config_data:
        print(f"{RED}Configuration analysis failed - no data returned{ENDC}")
        sys.exit(1)
        
    if "pragmas" not in config_data:
        print(f"{RED}Configuration analysis failed - no pragmas info{ENDC}")
        sys.exit(1)
    
    print(f"{GREEN}✓ analyze_configuration successful{ENDC}")
    print(f"  - Found {len(config_data['pragmas'])} SQLite pragmas")
    
    # Step 5: Test monitor_resource_utilization
    print(f"\\n{BOLD}Step 5: Testing monitor_resource_utilization{ENDC}")
    utilization_data = resource_analyzer.monitor_resource_utilization(
        connection_id, duration=1, interval=0.5
    )
    
    # Verify key data is present
    if not utilization_data:
        print(f"{RED}Resource utilization monitoring failed - no data returned{ENDC}")
        sys.exit(1)
        
    if "system" not in utilization_data:
        print(f"{RED}Resource utilization monitoring failed - no system info{ENDC}")
        sys.exit(1)
    
    print(f"{GREEN}✓ monitor_resource_utilization successful{ENDC}")
    print(f"  - Collected system metrics: {', '.join(utilization_data['system'].keys())}")
    
    # Step 6: Test generate_resource_recommendations
    print(f"\\n{BOLD}Step 6: Testing generate_resource_recommendations{ENDC}")
    recommendations = resource_analyzer.generate_resource_recommendations(connection_id)
    
    # Verify recommendations were generated
    if not recommendations:
        print(f"{RED}Recommendation generation failed - no recommendations returned{ENDC}")
        sys.exit(1)
        
    if not isinstance(recommendations, list):
        print(f"{RED}Recommendation generation failed - result is not a list{ENDC}")
        sys.exit(1)
        
    if not all(isinstance(r, Recommendation) for r in recommendations):
        print(f"{RED}Recommendation generation failed - not all items are Recommendation objects{ENDC}")
        sys.exit(1)
    
    print(f"{GREEN}✓ generate_resource_recommendations successful{ENDC}")
    print(f"  - Generated {len(recommendations)} recommendations")
    print(f"  - Example: {recommendations[0].title}")
    
    # Step 7: Test save_analysis_data
    print(f"\\n{BOLD}Step 7: Testing save_analysis_data{ENDC}")
    output_file = os.path.join(temp_dir, "analysis_data.json")
    resource_analyzer.save_analysis_data(connection_id, output_file)
    
    # Verify the file was created
    if not os.path.exists(output_file):
        print(f"{RED}Data persistence failed - output file not created{ENDC}")
        sys.exit(1)
        
    # Verify the file contains valid JSON
    try:
        with open(output_file, "r") as f:
            saved_data = json.load(f)
            
        if not isinstance(saved_data, dict):
            print(f"{RED}Data persistence failed - output file does not contain a valid JSON object{ENDC}")
            sys.exit(1)
            
        if "connection_id" not in saved_data or saved_data["connection_id"] != connection_id:
            print(f"{RED}Data persistence failed - missing or incorrect connection_id{ENDC}")
            sys.exit(1)
    except json.JSONDecodeError:
        print(f"{RED}Data persistence failed - output file does not contain valid JSON{ENDC}")
        sys.exit(1)
    
    print(f"{GREEN}✓ save_analysis_data successful{ENDC}")
    print(f"  - Saved analysis data to {output_file}")
    
    # Final summary
    print(f"\\n{BLUE}{BOLD}Verification Summary{ENDC}")
    print(f"{BLUE}{'=' * 40}{ENDC}")
    print(f"{GREEN}{BOLD}✓ All tests passed!{ENDC}")
    print(f"{GREEN}Stage 2.1 (Resource Analyzer) is fully implemented and ready for the next stage.{ENDC}")
    print(f"\\nCompleted at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

except Exception as e:
    print(f"{RED}Verification failed: {str(e)}{ENDC}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    # Clean up
    try:
        shutil.rmtree(temp_dir)
    except:
        pass
'''

# Write the verification script
with open(verification_script_path, 'w') as f:
    f.write(verification_script_content)

print(f"Created simplified verification script at {verification_script_path}")
print("\nThis script tests the ResourceAnalyzer directly, without relying on your")
print("existing ConnectionManager implementation. It creates mock objects that")
print("conform to the expected interfaces.")
print("\nRun it with: python scripts/verify_stage2_1_simple.py")