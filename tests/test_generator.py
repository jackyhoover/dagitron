"""Tests for DAG generator functionality."""

import pytest
from unittest.mock import Mock, patch
from dagitron.generator import DagGenerator
from dagitron.parser import YamlDagParser
from dagitron.exceptions import DagGenerationError


class TestDagGenerator:
    """Test cases for DagGenerator."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = DagGenerator()
        self.parser = YamlDagParser()
    
    def test_generate_dag_summary(self):
        """Test generating DAG summary."""
        yaml_content = """
dag:
  dag_id: "test_summary_dag"
  description: "Test DAG for summary"
  schedule_interval: "@daily"
  start_date: "2024-01-01"

tasks:
  - name: "task_1"
    operator: "DummyOperator"
  - name: "task_2"
    operator: "BashOperator"
    bash_command: "echo 'test'"
    depends_on: ["task_1"]
"""
        dag_spec = self.parser.parse_string(yaml_content)
        summary = self.generator.get_dag_summary(dag_spec)
        
        assert summary["dag_id"] == "test_summary_dag"
        assert summary["task_count"] == 2
        assert summary["max_depth"] == 1
        assert summary["has_dependencies"] is True
        assert "DummyOperator" in summary["operators"]
        assert "BashOperator" in summary["operators"]
    
    def test_validate_dag_spec_valid(self):
        """Test validating a valid DAG specification."""
        yaml_content = """
dag:
  dag_id: "valid_dag"
  schedule_interval: "@daily"
  start_date: "2024-01-01"

tasks:
  - name: "task_1"
    operator: "DummyOperator"
"""
        dag_spec = self.parser.parse_string(yaml_content)
        is_valid = self.generator.validate_dag_spec(dag_spec)
        
        assert is_valid is True
    
    def test_validate_dag_spec_invalid(self):
        """Test validating an invalid DAG specification."""
        # This would normally be caught by parser, but test generator validation
        invalid_spec = {
            "dag": {"dag_id": "test"},
            "tasks": [
                {"name": "task_1", "operator": "DummyOperator", "depends_on": ["nonexistent"]}
            ]
        }
        
        is_valid = self.generator.validate_dag_spec(invalid_spec)
        assert is_valid is False
    
    def test_build_default_args(self):
        """Test building default args from configuration."""
        default_args_config = {
            "owner": "test_owner",
            "retries": 2,
            "retry_delay_minutes": 10,
            "email": ["test@example.com"],
            "email_on_failure": True
        }
        
        default_args = self.generator._build_default_args(default_args_config)
        
        assert default_args["owner"] == "test_owner"
        assert default_args["retries"] == 2
        assert default_args["email"] == ["test@example.com"]
        assert default_args["email_on_failure"] is True
    
    def test_parse_date_valid(self):
        """Test parsing valid date strings."""
        date_str = "2024-01-01"
        parsed_date = self.generator._parse_date(date_str)
        
        assert parsed_date.year == 2024
        assert parsed_date.month == 1
        assert parsed_date.day == 1
    
    def test_parse_date_invalid(self):
        """Test parsing invalid date strings."""
        with pytest.raises(DagGenerationError) as exc_info:
            self.generator._parse_date("invalid-date")
        
        assert "Invalid date format" in str(exc_info.value)
    
    def test_generate_dag_without_airflow(self):
        """Test DAG generation fails gracefully without Airflow."""
        yaml_content = """
dag:
  dag_id: "test_dag"
  schedule_interval: "@daily"
  start_date: "2024-01-01"

tasks:
  - name: "task_1"
    operator: "DummyOperator"
"""
        # This should fail because we don't have airflow installed
        with pytest.raises(DagGenerationError) as exc_info:
            self.generator.generate_dag_from_string(yaml_content)
        
        assert "Airflow is not installed" in str(exc_info.value)