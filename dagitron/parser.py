"""YAML parser for DAG specifications."""

import os
from typing import Dict, Any, List, Optional
import yaml
import jsonschema
from .exceptions import YamlParsingError, YamlValidationError
from .schema import get_dag_schema


class YamlDagParser:
    """Parser for YAML DAG specification files."""
    
    def __init__(self, schema: Optional[Dict[str, Any]] = None) -> None:
        """Initialize the parser.
        
        Args:
            schema: Optional custom schema for validation. If None, uses default schema.
        """
        self.schema = schema if schema is not None else get_dag_schema()
    
    def parse_file(self, file_path: str) -> Dict[str, Any]:
        """Parse a YAML file and return the DAG specification.
        
        Args:
            file_path: Path to the YAML file.
            
        Returns:
            Parsed DAG specification dictionary.
            
        Raises:
            YamlParsingError: If file cannot be read or parsed.
            YamlValidationError: If YAML doesn't match schema.
        """
        if not os.path.exists(file_path):
            raise YamlParsingError(f"File not found: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except IOError as e:
            raise YamlParsingError(f"Failed to read file {file_path}", str(e))
        
        return self.parse_string(content)
    
    def parse_string(self, yaml_content: str) -> Dict[str, Any]:
        """Parse YAML content from string and return the DAG specification.
        
        Args:
            yaml_content: YAML content as string.
            
        Returns:
            Parsed DAG specification dictionary.
            
        Raises:
            YamlParsingError: If YAML cannot be parsed.
            YamlValidationError: If YAML doesn't match schema.
        """
        try:
            # Parse YAML content
            dag_spec = yaml.safe_load(yaml_content)
        except yaml.YAMLError as e:
            raise YamlParsingError("Failed to parse YAML content", str(e))
        
        if dag_spec is None:
            raise YamlParsingError("Empty YAML content")
        
        # Validate against schema
        self.validate(dag_spec)
        
        return dag_spec
    
    def validate(self, dag_spec: Dict[str, Any]) -> None:
        """Validate DAG specification against schema.
        
        Args:
            dag_spec: DAG specification dictionary.
            
        Raises:
            YamlValidationError: If validation fails.
        """
        try:
            jsonschema.validate(dag_spec, self.schema)
        except jsonschema.ValidationError as e:
            error_path = " -> ".join(str(p) for p in e.path) if e.path else "root"
            raise YamlValidationError(
                f"Schema validation failed at {error_path}: {e.message}",
                str(e)
            )
        except jsonschema.SchemaError as e:
            raise YamlValidationError("Invalid schema definition", str(e))
        
        # Additional custom validations
        self._validate_task_dependencies(dag_spec)
        self._validate_unique_task_names(dag_spec)
    
    def _validate_task_dependencies(self, dag_spec: Dict[str, Any]) -> None:
        """Validate that all task dependencies reference existing tasks.
        
        Args:
            dag_spec: DAG specification dictionary.
            
        Raises:
            YamlValidationError: If dependencies reference non-existent tasks.
        """
        tasks = dag_spec.get("tasks", [])
        task_names = {task["name"] for task in tasks}
        
        for task in tasks:
            depends_on = task.get("depends_on", [])
            if isinstance(depends_on, str):
                depends_on = [depends_on]
            
            for dependency in depends_on:
                if dependency not in task_names:
                    raise YamlValidationError(
                        f"Task '{task['name']}' depends on non-existent task '{dependency}'"
                    )
    
    def _validate_unique_task_names(self, dag_spec: Dict[str, Any]) -> None:
        """Validate that all task names are unique.
        
        Args:
            dag_spec: DAG specification dictionary.
            
        Raises:
            YamlValidationError: If duplicate task names are found.
        """
        tasks = dag_spec.get("tasks", [])
        task_names = []
        
        for task in tasks:
            name = task["name"]
            if name in task_names:
                raise YamlValidationError(f"Duplicate task name: '{name}'")
            task_names.append(name)
    
    def get_dag_config(self, dag_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Extract DAG configuration from specification.
        
        Args:
            dag_spec: DAG specification dictionary.
            
        Returns:
            DAG configuration dictionary.
        """
        return dag_spec.get("dag", {})
    
    def get_tasks_config(self, dag_spec: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract tasks configuration from specification.
        
        Args:
            dag_spec: DAG specification dictionary.
            
        Returns:
            List of task configuration dictionaries.
        """
        return dag_spec.get("tasks", [])