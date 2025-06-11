"""Tests for YAML parser functionality."""

import pytest
import os
from dagitron.parser import YamlDagParser
from dagitron.exceptions import YamlParsingError, YamlValidationError


class TestYamlDagParser:
    """Test cases for YamlDagParser."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.parser = YamlDagParser()
        self.test_dir = os.path.dirname(os.path.abspath(__file__))
        self.fixtures_dir = os.path.join(self.test_dir, "fixtures")
    
    def test_parse_valid_yaml_file(self):
        """Test parsing a valid YAML file."""
        valid_file = os.path.join(self.fixtures_dir, "valid_dag.yaml")
        result = self.parser.parse_file(valid_file)
        
        assert isinstance(result, dict)
        assert "dag" in result
        assert "tasks" in result
        assert result["dag"]["dag_id"] == "test_dag"
        assert len(result["tasks"]) == 2
    
    def test_parse_invalid_yaml_file(self):
        """Test parsing an invalid YAML file."""
        invalid_file = os.path.join(self.fixtures_dir, "invalid_dag.yaml")
        
        with pytest.raises(YamlValidationError):
            self.parser.parse_file(invalid_file)
    
    def test_parse_nonexistent_file(self):
        """Test parsing a non-existent file."""
        with pytest.raises(YamlParsingError) as exc_info:
            self.parser.parse_file("nonexistent.yaml")
        
        assert "File not found" in str(exc_info.value)
    
    def test_parse_valid_yaml_string(self):
        """Test parsing valid YAML content from string."""
        yaml_content = """
dag:
  dag_id: "string_test_dag"
  description: "Test DAG from string"
  schedule_interval: "@daily"
  start_date: "2024-01-01"

tasks:
  - name: "test_task"
    operator: "DummyOperator"
"""
        result = self.parser.parse_string(yaml_content)
        
        assert result["dag"]["dag_id"] == "string_test_dag"
        assert len(result["tasks"]) == 1
    
    def test_parse_invalid_yaml_string(self):
        """Test parsing invalid YAML content."""
        invalid_yaml = """
dag:
  dag_id: "invalid_dag"
  # Missing required fields
tasks: []  # Empty tasks list
"""
        with pytest.raises(YamlValidationError):
            self.parser.parse_string(invalid_yaml)
    
    def test_parse_malformed_yaml_string(self):
        """Test parsing malformed YAML syntax."""
        malformed_yaml = """
dag:
  dag_id: "test"
  invalid_yaml: [unclosed list
"""
        with pytest.raises(YamlParsingError):
            self.parser.parse_string(malformed_yaml)
    
    def test_get_dag_config(self):
        """Test extracting DAG configuration."""
        valid_file = os.path.join(self.fixtures_dir, "valid_dag.yaml")
        dag_spec = self.parser.parse_file(valid_file)
        dag_config = self.parser.get_dag_config(dag_spec)
        
        assert dag_config["dag_id"] == "test_dag"
        assert dag_config["description"] == "A test DAG for validation"
    
    def test_get_tasks_config(self):
        """Test extracting tasks configuration."""
        valid_file = os.path.join(self.fixtures_dir, "valid_dag.yaml")
        dag_spec = self.parser.parse_file(valid_file)
        tasks_config = self.parser.get_tasks_config(dag_spec)
        
        assert len(tasks_config) == 2
        assert tasks_config[0]["name"] == "task_1"
        assert tasks_config[1]["name"] == "task_2"
    
    def test_validate_unique_task_names(self):
        """Test validation of unique task names."""
        yaml_with_duplicate_names = """
dag:
  dag_id: "duplicate_test"
  schedule_interval: "@daily"
  start_date: "2024-01-01"

tasks:
  - name: "duplicate_task"
    operator: "DummyOperator"
  - name: "duplicate_task"
    operator: "BashOperator"
    bash_command: "echo 'test'"
"""
        with pytest.raises(YamlValidationError) as exc_info:
            self.parser.parse_string(yaml_with_duplicate_names)
        
        assert "Duplicate task name" in str(exc_info.value)
    
    def test_validate_task_dependencies(self):
        """Test validation of task dependencies."""
        yaml_with_invalid_deps = """
dag:
  dag_id: "invalid_deps_test"
  schedule_interval: "@daily"
  start_date: "2024-01-01"

tasks:
  - name: "task_1"
    operator: "DummyOperator"
    depends_on: ["nonexistent_task"]
"""
        with pytest.raises(YamlValidationError) as exc_info:
            self.parser.parse_string(yaml_with_invalid_deps)
        
        assert "depends on non-existent task" in str(exc_info.value)
    
    def test_validate_required_fields(self):
        """Test validation of required fields."""
        # Test missing dag_id
        yaml_missing_dag_id = """
dag:
  description: "Missing dag_id"
  schedule_interval: "@daily"
  start_date: "2024-01-01"

tasks:
  - name: "task_1"
    operator: "DummyOperator"
"""
        with pytest.raises(YamlValidationError):
            self.parser.parse_string(yaml_missing_dag_id)
        
        # Test missing tasks
        yaml_missing_tasks = """
dag:
  dag_id: "no_tasks"
  schedule_interval: "@daily"
  start_date: "2024-01-01"
"""
        with pytest.raises(YamlValidationError):
            self.parser.parse_string(yaml_missing_tasks)
    
    def test_validate_operator_types(self):
        """Test that any operator type is now accepted at parsing level."""
        yaml_custom_operator = """
dag:
  dag_id: "custom_operator_test"
  schedule_interval: "@daily"
  start_date: "2024-01-01"

tasks:
  - name: "task_1"
    operator: "mycompany.operators.CustomOperator"
  - name: "task_2"  
    operator: "BashOperator"
    bash_command: "echo hello"
"""
        # Should no longer raise YamlValidationError since custom operators are allowed
        try:
            result = self.parser.parse_string(yaml_custom_operator)
            # Verify parsing succeeds and contains expected content
            assert result["dag"]["dag_id"] == "custom_operator_test"
            assert len(result["tasks"]) == 2
            assert result["tasks"][0]["operator"] == "mycompany.operators.CustomOperator"
            assert result["tasks"][1]["operator"] == "BashOperator"
        except YamlValidationError:
            pytest.fail("Custom operators should be allowed at parsing level")