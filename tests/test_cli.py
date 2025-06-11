"""Tests for CLI functionality."""

import pytest
import os
from click.testing import CliRunner
from dagitron.cli import cli, generate, validate, operators


class TestCLI:
    """Test cases for CLI commands."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
        self.test_dir = os.path.dirname(os.path.abspath(__file__))
        self.fixtures_dir = os.path.join(self.test_dir, "fixtures")
    
    def test_cli_help(self):
        """Test CLI help command."""
        result = self.runner.invoke(cli, ["--help"])
        
        assert result.exit_code == 0
        assert "Dagitron" in result.output
        assert "generate" in result.output
        assert "validate" in result.output
        assert "operators" in result.output
    
    def test_cli_version(self):
        """Test CLI version command."""
        result = self.runner.invoke(cli, ["--version"])
        
        assert result.exit_code == 0
        assert "0.1.0" in result.output
    
    def test_operators_command(self):
        """Test operators listing command."""
        result = self.runner.invoke(operators)
        
        assert result.exit_code == 0
        assert "Supported Airflow Operators" in result.output
        assert "BashOperator" in result.output
        assert "PythonOperator" in result.output
        assert "Total:" in result.output
    
    def test_validate_command_valid_file(self):
        """Test validate command with valid file."""
        valid_file = os.path.join(self.fixtures_dir, "valid_dag.yaml")
        result = self.runner.invoke(validate, [valid_file])
        
        assert result.exit_code == 0
        assert "✓ YAML specification is valid" in result.output
    
    def test_validate_command_invalid_file(self):
        """Test validate command with invalid file."""
        invalid_file = os.path.join(self.fixtures_dir, "invalid_dag.yaml")
        result = self.runner.invoke(validate, [invalid_file])
        
        assert result.exit_code == 1
        assert "Validation Error" in result.output
    
    def test_validate_command_nonexistent_file(self):
        """Test validate command with non-existent file."""
        result = self.runner.invoke(validate, ["nonexistent.yaml"])
        
        assert result.exit_code == 1
        assert "Validation Error" in result.output
    
    def test_validate_command_verbose(self):
        """Test validate command with verbose output."""
        valid_file = os.path.join(self.fixtures_dir, "valid_dag.yaml")
        result = self.runner.invoke(validate, [valid_file, "--verbose"])
        
        assert result.exit_code == 0
        assert "Validating YAML file" in result.output
        assert "Validation Summary" in result.output
        assert "DAG ID:" in result.output
    
    def test_generate_command_dry_run(self):
        """Test generate command with dry run."""
        valid_file = os.path.join(self.fixtures_dir, "valid_dag.yaml")
        result = self.runner.invoke(generate, [valid_file, "--dry-run"])
        
        assert result.exit_code == 0
        assert "DAG Summary:" in result.output
        assert "✓ YAML specification is valid" in result.output
    
    def test_generate_command_invalid_file(self):
        """Test generate command with invalid file."""
        invalid_file = os.path.join(self.fixtures_dir, "invalid_dag.yaml")
        result = self.runner.invoke(generate, [invalid_file, "--dry-run"])
        
        assert result.exit_code == 1
        assert "Error:" in result.output
    
    def test_generate_command_verbose_dry_run(self):
        """Test generate command with verbose and dry run."""
        valid_file = os.path.join(self.fixtures_dir, "valid_dag.yaml")
        result = self.runner.invoke(generate, [valid_file, "--dry-run", "--verbose"])
        
        assert result.exit_code == 0
        assert "Parsing YAML file" in result.output
        assert "DAG Summary:" in result.output
        assert "Task Count:" in result.output
        assert "Operators:" in result.output
    
    def test_generate_command_without_airflow(self):
        """Test generate command fails without Airflow (not dry run)."""
        valid_file = os.path.join(self.fixtures_dir, "valid_dag.yaml")
        
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(generate, [valid_file])
            
            # Should fail because Airflow is not installed
            assert result.exit_code == 1
            assert "Error:" in result.output
    
    def test_generate_help(self):
        """Test generate command help."""
        result = self.runner.invoke(generate, ["--help"])
        
        assert result.exit_code == 0
        assert "Generate Airflow DAG from YAML" in result.output
        assert "--output" in result.output
        assert "--dry-run" in result.output
        assert "--verbose" in result.output
    
    def test_validate_help(self):
        """Test validate command help."""
        result = self.runner.invoke(validate, ["--help"])
        
        assert result.exit_code == 0
        assert "Validate YAML specification" in result.output
        assert "--verbose" in result.output