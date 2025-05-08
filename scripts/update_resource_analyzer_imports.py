#!/usr/bin/env python
"""
Script to update resource_analyzer.py to not require Connection class import.

This script modifies the resource_analyzer.py file to type hint ConnectionManager only
and use string annotations for Connection to avoid direct import.
"""

import os
import sys
import re
from pathlib import Path

# Get the project root
project_root = Path(__file__).parent.parent.absolute()
resource_analyzer_path = project_root / "tuningfork" / "analyzers" / "resource_analyzer.py"

if not resource_analyzer_path.exists():
    print(f"Error: {resource_analyzer_path} does not exist!")
    sys.exit(1)

# Read the current content
print(f"Checking {resource_analyzer_path}...")
with open(resource_analyzer_path, 'r') as f:
    content = f.read()

# Update import statement
original_import = "from tuningfork.connection.connection_manager import ConnectionManager, Connection"
new_import = "from tuningfork.connection.connection_manager import ConnectionManager"

if original_import in content:
    updated_content = content.replace(original_import, new_import)
    print(f"Updated import from:\n  {original_import}\nto:\n  {new_import}")
    
    # Update type hints that use Connection
    # Pattern: <param_name>: Connection -> <param_name>: 'Connection'
    # and def func(...) -> Connection: -> def func(...) -> 'Connection':
    updated_content = re.sub(r'(\w+): Connection', r"\1: 'Connection'", updated_content)
    updated_content = re.sub(r'def (\w+\([^)]*\)) -> Connection:', r"def \1 -> 'Connection':", updated_content)
    
    # Add typing imports if not already there
    if "from typing import " in updated_content:
        if "TYPE_CHECKING" not in updated_content:
            updated_content = updated_content.replace(
                "from typing import", 
                "from typing import TYPE_CHECKING, "
            )
    else:
        updated_content = "from typing import TYPE_CHECKING, Dict, List, Any, Optional, Tuple, Union\n" + updated_content
    
    # Add conditional Connection import
    if "if TYPE_CHECKING:" not in updated_content:
        import_section_end = 0
        for i, line in enumerate(updated_content.split("\n")):
            if line.startswith("import ") or line.startswith("from "):
                import_section_end = i
            elif line.strip() and not line.startswith("#") and i > 5:
                break
        
        type_checking_import = "\n# For type checking only\nif TYPE_CHECKING:\n    from tuningfork.connection.connection_manager import Connection\n"
        
        lines = updated_content.split("\n")
        updated_content = "\n".join(lines[:import_section_end+1]) + type_checking_import + "\n".join(lines[import_section_end+1:])
    
    # Create a backup
    backup_path = resource_analyzer_path.with_suffix(".py.bak")
    with open(backup_path, 'w') as f:
        f.write(content)
    print(f"Created backup at {backup_path}")
    
    # Write the updated content
    with open(resource_analyzer_path, 'w') as f:
        f.write(updated_content)
    
    print(f"Updated {resource_analyzer_path}")
else:
    print(f"Import statement '{original_import}' not found in {resource_analyzer_path}.")
    
    # Let's print the actual import line
    import_index = content.find("from tuningfork.connection")
    if import_index != -1:
        end_line = content.find("\n", import_index)
        actual_import = content[import_index:end_line]
        print(f"Actual import statement: {actual_import}")

print("\nDone.")