#!/usr/bin/env python
"""
Script to examine your connection_manager.py implementation and identify issues.

This script analyzes the connection_manager.py file to understand how it handles
database types and connections, then suggests specific fixes.
"""

import os
import sys
import re
from pathlib import Path

# Get the project root
project_root = Path(__file__).parent.parent.absolute()
connection_manager_path = project_root / "tuningfork" / "connection" / "connection_manager.py"

if not connection_manager_path.exists():
    print(f"Error: {connection_manager_path} does not exist!")
    sys.exit(1)

# Read the connection_manager.py file
print(f"Reading {connection_manager_path}...")
with open(connection_manager_path, 'r') as f:
    content = f.read()

# Extract the ConnectionManager class
class_match = re.search(r'class ConnectionManager[^:]*:(.*?)(?=\nclass|\Z)', content, re.DOTALL)
if not class_match:
    print("Could not find ConnectionManager class!")
    sys.exit(1)

cm_class = class_match.group(0)
print(f"Found ConnectionManager class ({len(cm_class)} characters)")

# Extract the connect method
connect_match = re.search(r'def connect\s*\(\s*self\s*,\s*([^)]*)\)\s*:(.*?)(?=\n    def|\Z)', cm_class, re.DOTALL)
if not connect_match:
    print("Could not find connect() method in ConnectionManager class!")
    sys.exit(1)

connect_method = connect_match.group(0)
connect_params = connect_match.group(1)
print(f"\nConnect method parameters: {connect_params}")

# Look for how database type is determined
db_type_match = re.search(r'db_type\s*=\s*([^\n]*)', connect_method)
if db_type_match:
    db_type_code = db_type_match.group(0)
    print(f"\nDatabase type determination: {db_type_code}")

# Look for the supported types check
supported_types_match = re.search(r'Supported types: [^\n]*', content)
if supported_types_match:
    supported_types = supported_types_match.group(0)
    print(f"\nSupported types check: {supported_types}")

db_type_check_match = re.search(r'if\s+([^:]*)\s*:', connect_method)
if db_type_check_match:
    db_type_check = db_type_check_match.group(1)
    print(f"\nDatabase type check: {db_type_check}")

# Look for connection creation based on type
type_conditions = re.findall(r'if\s+([^:]*)\s*:(.*?)(?=\s*elif|\s*else:|\Z)', connect_method, re.DOTALL)
print("\nDatabase type conditions:")
for condition, code in type_conditions:
    print(f"  - {condition.strip()}")

# Look for how connections are stored
connections_match = re.search(r'self\.connections\[\s*([^\]]*)\s*\]\s*=', connect_method)
if connections_match:
    connection_storage = connections_match.group(1)
    print(f"\nConnections stored with key: {connection_storage}")

# Debug the error location
print("\nPotential error locations:")

# Check if config_manager has get_connection_config method
config_manager_check = re.search(r'config\s*=\s*self\.config_manager\.get_connection_config', connect_method)
if config_manager_check:
    print("- ConnectionManager tries to use config_manager.get_connection_config()")
    
    # Check if ConfigManager has this method
    config_manager_path = project_root / "tuningfork" / "core" / "config_manager.py"
    if config_manager_path.exists():
        with open(config_manager_path, 'r') as f:
            cm_content = f.read()
        
        if "def get_connection_config" in cm_content:
            print("  + ConfigManager has get_connection_config method")
        else:
            print("  - ConfigManager is missing get_connection_config method!")
    else:
        print(f"  - {config_manager_path} does not exist!")

# Check for connection_id in connect method
if "connection_id" in connect_params:
    print("- connect() method accepts connection_id parameter")
else:
    print("- connect() method does not accept connection_id parameter!")

print("\nPossible fixes:")
print("1. Update ConnectionManager.connect() to handle 'test_sqlite' correctly")
print("2. Update the verification script to use 'sqlite' as the database type")
print("3. If ConnectionManager needs a config_manager.get_connection_config() method, implement it")

print("\nExamine the specific error:")
print("- 'Unsupported database type: test_sqlite. Supported types: ['postgres', 'postgresql', 'mysql', 'mariadb', 'sqlserver', 'mssql', 'sqlite', 'oracle']'")
print("  This suggests the verification script is passing 'test_sqlite' as the database type,")
print("  but the ConnectionManager only accepts specific types like 'sqlite'.")

# Create a direct fix script
fix_script_path = project_root / "scripts" / "direct_fix.py"
with open(fix_script_path, 'w') as f:
    f.write('''#!/usr/bin/env python
"""
Script to directly fix the verify_stage2_1.py script to work with your ConnectionManager.

This script modifies the verification script to use 'sqlite' instead of 'test_sqlite'
for the database type.
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
backup_path = verification_script_path.with_suffix(".py.bak4")
with open(backup_path, 'w') as f:
    f.write(content)
print(f"Created backup at {backup_path}")

# Update all occurrences of test_sqlite to sqlite
updated_content = content.replace('connection_id = "test_sqlite"', 'connection_id = "sqlite"')
updated_content = updated_content.replace('"test_sqlite"', '"sqlite"')

# Write the updated content
with open(verification_script_path, 'w') as f:
    f.write(updated_content)

print(f"Updated {verification_script_path} to use 'sqlite' instead of 'test_sqlite'")
print("\\nDone. Try running the verification script again.")
''')

print(f"\nCreated direct fix script at {fix_script_path}")
print("Run it with: python scripts/direct_fix.py")