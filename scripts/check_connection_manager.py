#!/usr/bin/env python
"""
Script to check and update connection_manager.py to include the Connection class.

This script examines the existing connection_manager.py file and updates it
if the Connection class is missing.
"""

import os
import sys
from pathlib import Path

# Get the project root
project_root = Path(__file__).parent.parent.absolute()
connection_manager_path = project_root / "tuningfork" / "connection" / "connection_manager.py"

if not connection_manager_path.exists():
    print(f"Error: {connection_manager_path} does not exist!")
    sys.exit(1)

# Read the current content
print(f"Checking {connection_manager_path}...")
with open(connection_manager_path, 'r') as f:
    content = f.read()

# Check if Connection class exists
if "class Connection:" in content:
    print("Connection class already exists in connection_manager.py.")
    
    # Let's check the imports in resource_analyzer.py
    resource_analyzer_path = project_root / "tuningfork" / "analyzers" / "resource_analyzer.py"
    
    if resource_analyzer_path.exists():
        with open(resource_analyzer_path, 'r') as f:
            ra_content = f.read()
        
        print("\nChecking imports in resource_analyzer.py...")
        import_line = "from tuningfork.connection.connection_manager import ConnectionManager, Connection"
        
        if import_line in ra_content:
            print(f"Import statement exists: {import_line}")
            print("\nThere might be an issue with the Connection class definition:")
            
            # Let's print the Connection class definition from connection_manager.py
            connection_class_start = content.find("class Connection:")
            
            if connection_class_start != -1:
                connection_class_end = content.find("\nclass ", connection_class_start + 1)
                if connection_class_end == -1:
                    connection_class_end = len(content)
                
                connection_class = content[connection_class_start:connection_class_end]
                print("\nConnection class definition:")
                print("-" * 50)
                print(connection_class)
                print("-" * 50)
                
                # Print the key methods the ResourceAnalyzer might be using
                print("\nMethods that ResourceAnalyzer might be trying to use:")
                required_methods = ["is_connected", "cursor", "commit", "rollback", "close"]
                
                for method in required_methods:
                    if f"def {method}" in connection_class:
                        print(f"✓ {method} method exists")
                    else:
                        print(f"✗ {method} method missing")
        else:
            print(f"Import statement different: {import_line}")
            
            # Let's print the actual import line
            import_index = ra_content.find("from tuningfork.connection")
            if import_index != -1:
                end_line = ra_content.find("\n", import_index)
                actual_import = ra_content[import_index:end_line]
                print(f"Actual import statement: {actual_import}")
    else:
        print(f"Error: {resource_analyzer_path} does not exist!")
else:
    print("Connection class is missing in connection_manager.py.")
    
    # Let's create a backup of the file
    backup_path = connection_manager_path.with_suffix(".py.bak")
    with open(backup_path, 'w') as f:
        f.write(content)
    print(f"Created backup at {backup_path}")
    
    # Add Connection class - using direct string writing to avoid docstring issues
    connection_class_code = '''
class Connection:
    """
    Represents a database connection.
    
    This class is a wrapper around different database connection objects.
    """
    
    def __init__(self, connection_id: str, db_type: str, connection_obj, config):
        """
        Initialize a Connection object.
        
        Args:
            connection_id: The connection ID
            db_type: The database type (postgresql, mysql, mssql, sqlite)
            connection_obj: The actual database connection object
            config: The connection configuration
        """
        self.connection_id = connection_id
        self.db_type = db_type.lower()
        self.connection_obj = connection_obj
        self.config = config
        self.is_ssh_tunnel = False
        self.ssh_tunnel = None
    
    def cursor(self):
        """
        Get a cursor for the connection.
        
        Returns:
            A database cursor
        """
        return self.connection_obj.cursor()
    
    def commit(self):
        """Commit the current transaction."""
        return self.connection_obj.commit()
    
    def rollback(self):
        """Rollback the current transaction."""
        return self.connection_obj.rollback()
    
    def close(self):
        """Close the connection."""
        if self.is_connected():
            self.connection_obj.close()
            
            # Close SSH tunnel if present
            if self.is_ssh_tunnel and self.ssh_tunnel:
                try:
                    self.ssh_tunnel.close()
                except Exception as e:
                    pass
    
    def is_connected(self) -> bool:
        """
        Check if the connection is still active.
        
        Returns:
            True if the connection is active, False otherwise
        """
        if not self.connection_obj:
            return False
            
        try:
            # Different ways to check connection based on database type
            if self.db_type == "postgresql":
                return not self.connection_obj.closed
            elif self.db_type == "mysql":
                self.connection_obj.ping(reconnect=False)
                return True
            elif self.db_type == "mssql":
                cursor = self.connection_obj.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return True
            elif self.db_type == "sqlite":
                cursor = self.connection_obj.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return True
            else:
                # Default for unknown types - just return True and hope for the best
                return True
        except Exception:
            return False
'''
    
    # Find where to insert the Connection class
    import_section_end = 0
    for i, line in enumerate(content.split('\n')):
        if line.startswith("import ") or line.startswith("from "):
            import_section_end = i
        elif not line.startswith("#") and line.strip() and i > 5:
            break
    
    # Add the Connection class after the imports
    content_lines = content.split('\n')
    updated_content = '\n'.join(content_lines[:import_section_end+1]) + connection_class_code + '\n' + '\n'.join(content_lines[import_section_end+1:])
    
    # Write the updated content
    with open(connection_manager_path, 'w') as f:
        f.write(updated_content)
    
    print(f"Added Connection class to {connection_manager_path}")

print("\nDone.")