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
        task_config = {
            "name": "test_task",
            "operator": "NonExistentOperator"
        }
        
        # Should fail at operator validation level before Airflow check
        with pytest.raises(UnsupportedOperatorError) as exc_info:
            self.factory.create_task(task_config, None)
        
        assert "Unsupported operator type" in str(exc_info.value)
    
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