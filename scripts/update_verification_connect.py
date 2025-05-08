#!/usr/bin/env python
"""
Script to update the verification script with a customized connect call.

This script replaces the problematic debug section with a simplified version
that correctly calls your ConnectionManager.connect() method.
"""

import os
import sys
import re
from pathlib import Path

# Get the project root
project_root = Path(__file__).parent.parent.absolute()
verification_script_path = project_root / "scripts" / "verify_stage2_1.py"

if not verification_script_path.exists():
    print(f"Error: {verification_script_path} does not exist!")
    sys.exit(1)

# Read the verification script
print(f"Reading {verification_script_path}...")
with open(verification_script_path, 'r') as f:
    content = f.read()

# Create a backup
backup_path = verification_script_path.with_suffix(".py.final_backup")
with open(backup_path, 'w') as f:
    f.write(content)
print(f"Created backup at {backup_path}")

# Replace the entire basic functionality check section
basic_check_section_pattern = r'# Step 2: Basic functionality check.*?(?=# Step 3:|$)'
basic_check_section_match = re.search(basic_check_section_pattern, content, re.DOTALL)

if not basic_check_section_match:
    print("Could not find the basic functionality check section in verification script!")
    sys.exit(1)

# Define a simplified version that correctly connects to SQLite
new_basic_check_section = '''# Step 2: Basic functionality check
print(f"\\n{BOLD}Step 2: Basic functionality check{ENDC}")

# Setup configuration
config_file = os.path.join(temp_dir, "config.json")
with open(config_file, "w") as f:
    json.dump({
        "storage_directory": os.path.join(temp_dir, "data"),
        "connections": {
            "sqlite": {
                "type": "sqlite",
                "database": ":memory:",
                "timeout": 30
            }
        }
    }, f, indent=2)

try:
    # Initialize components
    config_manager = ConfigManager(config_file)
    connection_manager = ConnectionManager(config_manager)
    resource_analyzer = ResourceAnalyzer(connection_manager, config_manager)
    
    # Connect to SQLite using the correct parameters
    connection = connection_manager.connect(
        host=':memory:', 
        port=0, 
        username='', 
        password='', 
        database=':memory:'
    )
    
    # For compatibility with the rest of the script, store this connection with a connection_id
    connection_id = "sqlite"
    connection_manager.connections[connection_id] = connection
    
    print(f"{GREEN}✓ Connected to SQLite successfully{ENDC}")
    
    # Setup test database
    cursor = connection.cursor()
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
    
    print(f"{GREEN}✓ Created test database successfully{ENDC}")
    
    # Test resource analysis
    resource_data = resource_analyzer.analyze_resources(connection_id)
    if not resource_data or "system" not in resource_data or "sqlite" not in resource_data:
        print(f"{RED}Resource analysis failed.{ENDC}")
        sys.exit(1)
    print(f"{GREEN}✓ Resource analysis successful{ENDC}")
    
    # Test configuration analysis
    config_data = resource_analyzer.analyze_configuration(connection_id)
    if not config_data or "pragmas" not in config_data:
        print(f"{RED}Configuration analysis failed.{ENDC}")
        sys.exit(1)
    print(f"{GREEN}✓ Configuration analysis successful{ENDC}")
    
    # Test resource utilization monitoring (brief)
    utilization_data = resource_analyzer.monitor_resource_utilization(
        connection_id, duration=1, interval=0.5
    )
    if not utilization_data or "system" not in utilization_data:
        print(f"{RED}Resource utilization monitoring failed.{ENDC}")
        sys.exit(1)
    print(f"{GREEN}✓ Resource utilization monitoring successful{ENDC}")
    
    # Test recommendation generation
    recommendations = resource_analyzer.generate_resource_recommendations(connection_id)
    if not recommendations or not isinstance(recommendations, list):
        print(f"{RED}Recommendation generation failed.{ENDC}")
        sys.exit(1)
    print(f"{GREEN}✓ Generated {len(recommendations)} recommendations successfully{ENDC}")
    
    # Test data persistence
    output_file = os.path.join(temp_dir, "data", "analysis_data.json")
    resource_analyzer.save_analysis_data(connection_id, output_file)
    if not os.path.exists(output_file):
        print(f"{RED}Data persistence failed.{ENDC}")
        sys.exit(1)
    print(f"{GREEN}✓ Data persistence successful{ENDC}")
    
    # Clean up
    connection_manager.close_all_connections()
    print(f"{GREEN}✓ Basic functionality check completed successfully{ENDC}")
    
except Exception as e:
    print(f"{RED}Basic functionality check failed: {str(e)}{ENDC}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
'''

# Update the content
updated_content = content[:basic_check_section_match.start()] + new_basic_check_section + content[basic_check_section_match.end():]

# Write the updated content
with open(verification_script_path, 'w') as f:
    f.write(updated_content)

print(f"\nUpdated basic functionality check in {verification_script_path}")
print("Now properly calling connect() with all required parameters")
print("\nDone. Run the script with: python scripts/verify_stage2_1.py")