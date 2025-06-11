"""
Dagitron: Apache Airflow DAG generator from YAML specifications.

This package provides a clean, declarative way to define complex Airflow workflows
using YAML specification files instead of writing Python code directly.

Main API:
    generate_dag_from_yaml: Generate Airflow DAG from YAML file
    generate_dag_from_string: Generate Airflow DAG from YAML string

Classes:
    YamlDagParser: Parse and validate YAML DAG specifications
    DagGenerator: Convert YAML specifications to Airflow DAGs
    TaskFactory: Create Airflow operators from task specifications
    DependencyResolver: Resolve and validate task dependencies

Example:
    >>> from dagitron import generate_dag_from_yaml
    >>> dag = generate_dag_from_yaml("my_workflow.yaml")
"""

from typing import Dict, Any
from .parser import YamlDagParser
from .generator import DagGenerator
from .factory import TaskFactory
from .dependencies import DependencyResolver
from .exceptions import (
    DagitronError,
    YamlParsingError,
    YamlValidationError,
    TaskFactoryError,
    DependencyError,
    DagGenerationError,
    UnsupportedOperatorError,
    CircularDependencyError,
    MissingTaskError,
    ConfigurationError,
)
from .schema import get_dag_schema, get_supported_operators

# Version information
__version__ = "0.1.0"
__author__ = "Dagitron Contributors"
__email__ = "contributors@dagitron.dev"
__description__ = "Apache Airflow DAG generator from YAML specifications"

# Public API
__all__ = [
    # Main API functions
    "generate_dag_from_yaml",
    "generate_dag_from_string",
    "validate_yaml_spec",
    
    # Core classes
    "YamlDagParser",
    "DagGenerator", 
    "TaskFactory",
    "DependencyResolver",
    
    # Exceptions
    "DagitronError",
    "YamlParsingError",
    "YamlValidationError",
    "TaskFactoryError",
    "DependencyError",
    "DagGenerationError",
    "UnsupportedOperatorError",
    "CircularDependencyError",
    "MissingTaskError",
    "ConfigurationError",
    
    # Utility functions
    "get_dag_schema",
    "get_supported_operators",
    
    # Version info
    "__version__",
]


def generate_dag_from_yaml(yaml_file: str):
    """Generate an Airflow DAG from a YAML specification file.
    
    This is the main API function for generating DAGs from YAML files.
    
    Args:
        yaml_file: Path to the YAML specification file.
        
    Returns:
        Generated Airflow DAG instance.
        
    Raises:
        DagitronError: If DAG generation fails.
        
    Example:
        >>> dag = generate_dag_from_yaml("workflows/data_pipeline.yaml")
        >>> print(dag.dag_id)
        'data_pipeline'
    """
    generator = DagGenerator()
    return generator.generate_dag_from_file(yaml_file)


def generate_dag_from_string(yaml_content: str):
    """Generate an Airflow DAG from YAML content string.
    
    Args:
        yaml_content: YAML specification as string.
        
    Returns:
        Generated Airflow DAG instance.
        
    Raises:
        DagitronError: If DAG generation fails.
        
    Example:
        >>> yaml_spec = '''
        ... dag:
        ...   dag_id: "simple_dag"
        ...   description: "A simple example"
        ...   schedule_interval: "@daily"
        ...   start_date: "2024-01-01"
        ... tasks:
        ...   - name: "hello_world"
        ...     operator: "BashOperator"
        ...     bash_command: "echo 'Hello, World!'"
        ... '''
        >>> dag = generate_dag_from_string(yaml_spec)
    """
    generator = DagGenerator()
    return generator.generate_dag_from_string(yaml_content)


def validate_yaml_spec(yaml_file: str) -> bool:
    """Validate a YAML specification file without generating the DAG.
    
    Args:
        yaml_file: Path to the YAML specification file.
        
    Returns:
        True if valid, False otherwise.
        
    Example:
        >>> is_valid = validate_yaml_spec("workflows/data_pipeline.yaml")
        >>> print(f"Specification is {'valid' if is_valid else 'invalid'}")
    """
    try:
        parser = YamlDagParser()
        generator = DagGenerator()
        dag_spec = parser.parse_file(yaml_file)
        return generator.validate_dag_spec(dag_spec)
    except Exception:
        return False


def get_dag_summary(yaml_file: str) -> Dict[str, Any]:
    """Get a summary of a DAG specification without generating the DAG.
    
    Args:
        yaml_file: Path to the YAML specification file.
        
    Returns:
        Summary dictionary with DAG information.
        
    Raises:
        DagitronError: If parsing or validation fails.
        
    Example:
        >>> summary = get_dag_summary("workflows/data_pipeline.yaml")
        >>> print(f"DAG has {summary['task_count']} tasks")
    """
    parser = YamlDagParser()
    generator = DagGenerator()
    dag_spec = parser.parse_file(yaml_file)
    return generator.get_dag_summary(dag_spec)