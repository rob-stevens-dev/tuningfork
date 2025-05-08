#!/usr/bin/env python
"""
Script to run the simple verification script.

This script just runs the simplified verification script and handles any errors.
"""

import os
import sys
import subprocess
from pathlib import Path

# Get the project root
project_root = Path(__file__).parent.parent.absolute()
simple_verification_path = project_root / "scripts" / "verify_stage2_1_simple.py"

if not simple_verification_path.exists():
    print(f"Error: {simple_verification_path} does not exist!")
    print("Run 'python scripts/simplest_solution.py' first to create it.")
    sys.exit(1)

# Make the script executable
os.chmod(simple_verification_path, 0o755)

# Run the script
print(f"Running {simple_verification_path}...")
print("-" * 60)
subprocess.run([sys.executable, str(simple_verification_path)], check=False)
print("-" * 60)
print("Done.")