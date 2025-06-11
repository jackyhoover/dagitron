"""Custom exceptions for Dagitron."""

from typing import Optional


class DagitronError(Exception):
    """Base exception for all Dagitron errors."""
    
    def __init__(self, message: str, details: Optional[str] = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details
    
    def __str__(self) -> str:
        if self.details:
            return f"{self.message}: {self.details}"
        return self.message


class YamlParsingError(DagitronError):
    """Raised when YAML parsing fails."""
    pass


class YamlValidationError(DagitronError):
    """Raised when YAML validation against schema fails."""
    pass


class TaskFactoryError(DagitronError):
    """Raised when task creation fails."""
    pass


class DependencyError(DagitronError):
    """Raised when dependency resolution fails."""
    pass


class DagGenerationError(DagitronError):
    """Raised when DAG generation fails."""
    pass


class UnsupportedOperatorError(TaskFactoryError):
    """Raised when an unsupported operator type is requested."""
    pass


class CircularDependencyError(DependencyError):
    """Raised when circular dependencies are detected."""
    pass


class MissingTaskError(DependencyError):
    """Raised when a dependency references a non-existent task."""
    pass


class ConfigurationError(DagitronError):
    """Raised when configuration is invalid."""
    pass