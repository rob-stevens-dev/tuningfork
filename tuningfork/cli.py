#!/usr/bin/env python3
"""
Tuning Fork - Database Performance Optimization Tool
Command-line interface entry point
"""

import sys
import os
import logging
from tuningfork.core.config_manager import ConfigManager
from tuningfork.core.connection_manager import ConnectionManager
from tuningfork.core.cli_manager import CLIManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the CLI"""
    try:
        # Create config manager
        config_file = os.environ.get("TUNINGFORK_CONFIG", "config.json")
        config_manager = ConfigManager(config_file if os.path.exists(config_file) else None)
        
        # Create connection manager
        connection_manager = ConnectionManager(config_manager)
        
        # Create CLI manager
        cli_manager = CLIManager(config_manager, connection_manager)
        
        # Run CLI with sys.argv
        exit_code = cli_manager.run()
        
        # Clean up
        connection_manager.disconnect_all()
        
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()