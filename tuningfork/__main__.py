import logging

from tuningfork.core.cli_manager import CLIManager
from tuningfork.core.config_manager import ConfigManager
from tuningfork.core.connection_manager import ConnectionManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dboptimize.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """
    Main entry point for the application.
    """
    # Create config manager
    config_manager = ConfigManager()
    
    # Create connection manager
    connection_manager = ConnectionManager(config_manager)
    
    # Create CLI manager
    cli_manager = CLIManager(config_manager, connection_manager)
    
    try:
        # Run CLI
        exit_code = cli_manager.run()
        
        # Clean up
        connection_manager.disconnect_all()
        
        return exit_code
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        connection_manager.disconnect_all()
        return 1
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        connection_manager.disconnect_all()
        return 1


if __name__ == "__main__":
    exit(main())