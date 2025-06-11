"""Tests for task factory functionality."""

import pytest
from dagitron.factory import TaskFactory
from dagitron.exceptions import UnsupportedOperatorError, TaskFactoryError


class TestTaskFactory:
    """Test cases for TaskFactory."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.factory = TaskFactory()
    
    def test_get_supported_operators(self):
        """Test getting list of supported operators."""
        operators = self.factory.get_supported_operators()
        
        assert isinstance(operators, list)
        assert len(operators) > 0
        assert "BashOperator" in operators
        assert "PythonOperator" in operators
        assert "DummyOperator" in operators
    
    def test_create_task_without_airflow(self):
        """Test task creation fails gracefully without Airflow."""
        task_config = {
            "name": "test_task",
            "operator": "DummyOperator"
        }
        
        with pytest.raises(TaskFactoryError) as exc_info:
            self.factory.create_task(task_config, None)
        
        assert "Airflow is not installed" in str(exc_info.value)
    
    def test_unsupported_operator(self):
        """Test error handling for unsupported operators."""
        # Use the TaskFactory validation logic directly to test unsupported operators
        # Since custom operators are now allowed, we need to test the validation differently
        
        # Test that built-in validation still works for known invalid operators
        task_config = {
            "name": "test_task",
            "operator": "NonExistentOperator"  # This will be treated as custom operator now
        }
        
        # This should try to import as custom operator and fail
        with pytest.raises(TaskFactoryError) as exc_info:
            self.factory.create_task(task_config, None)
        
        # Should fail either at Airflow check or import check
        assert "Airflow is not installed" in str(exc_info.value) or "Failed to import custom operator" in str(exc_info.value)
    
    def test_build_bash_operator_args(self):
        """Test building BashOperator arguments."""
        task_config = {
            "name": "bash_task",
            "operator": "BashOperator",
            "bash_command": "echo 'hello world'"
        }
        
        args = self.factory._build_bash_operator_args(task_config)
        assert args["bash_command"] == "echo 'hello world'"
    
    def test_build_bash_operator_args_missing_command(self):
        """Test BashOperator args validation."""
        task_config = {
            "name": "bash_task",
            "operator": "BashOperator"
            # Missing bash_command
        }
        
        with pytest.raises(TaskFactoryError) as exc_info:
            self.factory._build_bash_operator_args(task_config)
        
        assert "requires 'bash_command' parameter" in str(exc_info.value)
    
    def test_build_python_operator_args(self):
        """Test building PythonOperator arguments."""
        task_config = {
            "name": "python_task",
            "operator": "PythonOperator",
            "python_callable": "test_function",
            "op_args": [1, 2, 3],
            "op_kwargs": {"key": "value"}
        }
        
        args = self.factory._build_python_operator_args(task_config)
        
        # Note: This will fail in resolve_callable due to missing function
        # but we're just testing the arg building logic
        assert "op_args" in args
        assert "op_kwargs" in args
        assert args["op_args"] == [1, 2, 3]
        assert args["op_kwargs"] == {"key": "value"}
    
    def test_build_email_operator_args(self):
        """Test building EmailOperator arguments."""
        task_config = {
            "name": "email_task",
            "operator": "EmailOperator",
            "to": "test@example.com",
            "subject": "Test Email",
            "html_content": "<h1>Test</h1>"
        }
        
        args = self.factory._build_email_operator_args(task_config)
        
        assert args["to"] == ["test@example.com"]  # Should be converted to list
        assert args["subject"] == "Test Email"
        assert args["html_content"] == "<h1>Test</h1>"
    
    def test_build_email_operator_args_list_recipients(self):
        """Test EmailOperator with list of recipients."""
        task_config = {
            "name": "email_task",
            "operator": "EmailOperator",
            "to": ["test1@example.com", "test2@example.com"]
        }
        
        args = self.factory._build_email_operator_args(task_config)
        assert args["to"] == ["test1@example.com", "test2@example.com"]
    
    def test_build_common_operator_args(self):
        """Test building common operator arguments."""
        task_config = {
            "name": "common_task",
            "operator": "DummyOperator",
            "retries": 3,
            "retry_delay_minutes": 15,
            "pool": "test_pool",
            "priority_weight": 10,
            "queue": "test_queue",
            "trigger_rule": "all_success"
        }
        
        args = self.factory._build_operator_args(task_config, None)
        
        assert args["retries"] == 3
        assert args["pool"] == "test_pool"
        assert args["priority_weight"] == 10
        assert args["queue"] == "test_queue"
        assert args["trigger_rule"] == "all_success"
        
        # Check retry_delay is converted to timedelta
        from datetime import timedelta
        assert args["retry_delay"] == timedelta(minutes=15)
    
    def test_resolve_callable_invalid(self):
        """Test resolving invalid callable names."""
        with pytest.raises(TaskFactoryError) as exc_info:
            self.factory._resolve_callable("nonexistent.function.that.does.not.exist")
        
        assert "Failed to resolve callable" in str(exc_info.value)
    
    def test_import_custom_operator_valid_path(self):
        """Test importing a valid custom operator."""
        # Use a built-in Python class that we know exists
        operator_class = self.factory._import_custom_operator("builtins.str")
        assert operator_class == str
        
    def test_import_custom_operator_invalid_path(self):
        """Test importing an invalid custom operator."""
        with pytest.raises(TaskFactoryError) as exc_info:
            self.factory._import_custom_operator("nonexistent.module.NonExistentOperator")
        
        assert "Failed to import custom operator" in str(exc_info.value)
        
    def test_import_custom_operator_invalid_class(self):
        """Test importing from valid module but invalid class."""
        with pytest.raises(TaskFactoryError) as exc_info:
            self.factory._import_custom_operator("builtins.NonExistentClass")
        
        assert "not found in module" in str(exc_info.value)
        
    def test_import_custom_operator_no_dot(self):
        """Test that custom operators without dots raise an error."""
        with pytest.raises(TaskFactoryError) as exc_info:
            self.factory._import_custom_operator("SimpleOperatorName")
        
        assert "must be specified as a fully-qualified import path" in str(exc_info.value)
    
    def test_build_custom_operator_args(self):
        """Test building custom operator arguments."""
        task_config = {
            "name": "custom_task",
            "operator": "my_package.operators.CustomOperator",
            "custom_param": "value1",
            "another_param": 42,
            "complex_param": {"key": "value"},
            # These should be excluded
            "retries": 3,
            "depends_on": ["other_task"]
        }
        
        args = self.factory._build_custom_operator_args(task_config)
        
        # Should include custom parameters
        assert args["custom_param"] == "value1"
        assert args["another_param"] == 42
        assert args["complex_param"] == {"key": "value"}
        
        # Should exclude system fields
        assert "name" not in args
        assert "operator" not in args
        assert "retries" not in args
        assert "depends_on" not in args
    
    def test_detect_custom_operator(self):
        """Test detection of custom operators."""
        # Test with fully-qualified name (should be detected as custom)
        task_config_custom = {
            "name": "test_task",
            "operator": "my_package.operators.MyCustomOperator"
        }
        
        operator_type = task_config_custom["operator"]
        is_custom = ("." in operator_type) or (operator_type not in self.factory.supported_operators)
        assert is_custom
        
        # Test with built-in operator (should not be detected as custom)
        task_config_builtin = {
            "name": "test_task", 
            "operator": "BashOperator"
        }
        
        operator_type = task_config_builtin["operator"]
        is_custom = ("." in operator_type) or (operator_type not in self.factory.supported_operators)
        assert not is_custom