"""
Exceptions module for TuningFork database performance optimization tool.

This module defines custom exceptions used throughout the application.
"""


class TuningForkError(Exception):
    """Base exception for all TuningFork errors."""
    pass


class ConfigurationError(TuningForkError):
    """Exception raised for configuration errors."""
    pass


class ConnectionError(TuningForkError):
    """Exception raised for connection errors."""
    pass


class ResourceAnalysisError(TuningForkError):
    """Exception raised for resource analysis errors."""
    pass


class SchemaAnalysisError(TuningForkError):
    """Exception raised for schema analysis errors."""
    pass


class QueryAnalysisError(TuningForkError):
    """Exception raised for query analysis errors."""
    pass


class BackupError(TuningForkError):
    """Exception raised for backup errors."""
    pass


class RecommendationError(TuningForkError):
    """Exception raised for recommendation errors."""
    pass