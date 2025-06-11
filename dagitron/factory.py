"""Task factory for creating Airflow operators from specifications."""

from typing import Dict, Any, Optional, List, Union
from datetime import timedelta

# Import Airflow components with graceful fallback
try:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
    from airflow.operators.email import EmailOperator
    from airflow.operators.dummy import DummyOperator
    from airflow.sensors.filesystem import FileSensor
    from airflow.sensors.http_sensor import HttpSensor
    from airflow.sensors.s3_key_sensor import S3KeySensor
    from airflow.sensors.sql_sensor import SqlSensor
    from airflow.sensors.external_task import ExternalTaskSensor
    from airflow.models import BaseOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    # Mock classes for when Airflow is not available
    class BaseOperator:
        pass
    
    BashOperator = BaseOperator
    PythonOperator = BaseOperator
    BranchPythonOperator = BaseOperator
    ShortCircuitOperator = BaseOperator
    EmailOperator = BaseOperator
    DummyOperator = BaseOperator
    FileSensor = BaseOperator
    HttpSensor = BaseOperator
    S3KeySensor = BaseOperator
    SqlSensor = BaseOperator
    ExternalTaskSensor = BaseOperator
    AIRFLOW_AVAILABLE = False
from .exceptions import TaskFactoryError, UnsupportedOperatorError
from .schema import get_supported_operators


class TaskFactory:
    """Factory for creating Airflow operators from task specifications."""
    
    def __init__(self) -> None:
        """Initialize the task factory."""
        self.supported_operators = get_supported_operators()
        self._operator_mapping = {
            "BashOperator": BashOperator,
            "PythonOperator": PythonOperator,
            "BranchPythonOperator": BranchPythonOperator,
            "ShortCircuitOperator": ShortCircuitOperator,
            "EmailOperator": EmailOperator,
            "DummyOperator": DummyOperator,
            "FileSensor": FileSensor,
            "HttpSensor": HttpSensor,
            "S3KeySensor": S3KeySensor,
            "SqlSensor": SqlSensor,
            "ExternalTaskSensor": ExternalTaskSensor,
        }
    
    def create_task(self, task_config: Dict[str, Any], dag: Any) -> BaseOperator:
        """Create an Airflow operator from task configuration.
        
        Args:
            task_config: Task configuration dictionary.
            dag: The DAG instance to attach the task to.
            
        Returns:
            Configured Airflow operator instance.
            
        Raises:
            UnsupportedOperatorError: If operator type is not supported.
            TaskFactoryError: If task creation fails.
        """
        if not AIRFLOW_AVAILABLE:
            raise TaskFactoryError(
                "Airflow is not installed. Please install apache-airflow to create tasks.",
                "Run: pip install apache-airflow>=2.0.0"
            )
        
        operator_type = task_config["operator"]
        task_id = task_config["name"]
        
        if operator_type not in self.supported_operators:
            raise UnsupportedOperatorError(
                f"Unsupported operator type: {operator_type}",
                f"Supported operators: {', '.join(self.supported_operators)}"
            )
        
        try:
            operator_class = self._operator_mapping[operator_type]
            
            # Build operator arguments
            operator_args = self._build_operator_args(task_config, dag)
            
            # Create and return the operator
            return operator_class(task_id=task_id, dag=dag, **operator_args)
        
        except Exception as e:
            raise TaskFactoryError(
                f"Failed to create task '{task_id}' with operator '{operator_type}'",
                str(e)
            )
    
    def _build_operator_args(self, task_config: Dict[str, Any], dag: Any) -> Dict[str, Any]:
        """Build operator-specific arguments from task configuration.
        
        Args:
            task_config: Task configuration dictionary.
            dag: The DAG instance.
            
        Returns:
            Dictionary of operator arguments.
        """
        operator_type = task_config["operator"]
        args = {}
        
        # Common arguments for all operators
        if "retries" in task_config:
            args["retries"] = task_config["retries"]
        
        if "retry_delay_minutes" in task_config:
            args["retry_delay"] = timedelta(minutes=task_config["retry_delay_minutes"])
        
        if "pool" in task_config:
            args["pool"] = task_config["pool"]
        
        if "priority_weight" in task_config:
            args["priority_weight"] = task_config["priority_weight"]
        
        if "queue" in task_config:
            args["queue"] = task_config["queue"]
        
        if "trigger_rule" in task_config:
            args["trigger_rule"] = task_config["trigger_rule"]
        
        # Operator-specific arguments
        if operator_type == "BashOperator":
            args.update(self._build_bash_operator_args(task_config))
        elif operator_type in ["PythonOperator", "BranchPythonOperator", "ShortCircuitOperator"]:
            args.update(self._build_python_operator_args(task_config))
        elif operator_type == "EmailOperator":
            args.update(self._build_email_operator_args(task_config))
        elif operator_type == "FileSensor":
            args.update(self._build_file_sensor_args(task_config))
        elif operator_type == "HttpSensor":
            args.update(self._build_http_sensor_args(task_config))
        elif operator_type == "S3KeySensor":
            args.update(self._build_s3_sensor_args(task_config))
        elif operator_type == "SqlSensor":
            args.update(self._build_sql_sensor_args(task_config))
        elif operator_type == "ExternalTaskSensor":
            args.update(self._build_external_task_sensor_args(task_config))
        # DummyOperator needs no additional arguments
        
        return args
    
    def _build_bash_operator_args(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build arguments for BashOperator."""
        args = {}
        
        if "bash_command" in task_config:
            args["bash_command"] = task_config["bash_command"]
        else:
            raise TaskFactoryError("BashOperator requires 'bash_command' parameter")
        
        return args
    
    def _build_python_operator_args(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build arguments for Python operators."""
        args = {}
        
        if "python_callable" in task_config:
            # For simplicity, we expect the callable to be importable by name
            # In a real implementation, you'd want more sophisticated function resolution
            callable_name = task_config["python_callable"]
            args["python_callable"] = self._resolve_callable(callable_name)
        else:
            raise TaskFactoryError("PythonOperator requires 'python_callable' parameter")
        
        if "op_args" in task_config:
            args["op_args"] = task_config["op_args"]
        
        if "op_kwargs" in task_config:
            args["op_kwargs"] = task_config["op_kwargs"]
        
        return args
    
    def _build_email_operator_args(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build arguments for EmailOperator."""
        args = {}
        
        if "to" in task_config:
            to = task_config["to"]
            if isinstance(to, str):
                args["to"] = [to]
            else:
                args["to"] = to
        else:
            raise TaskFactoryError("EmailOperator requires 'to' parameter")
        
        if "subject" in task_config:
            args["subject"] = task_config["subject"]
        else:
            args["subject"] = "Airflow Email"
        
        if "html_content" in task_config:
            args["html_content"] = task_config["html_content"]
        else:
            args["html_content"] = "Airflow task email notification"
        
        return args
    
    def _build_file_sensor_args(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build arguments for FileSensor."""
        args = {}
        
        if "filepath" in task_config:
            args["filepath"] = task_config["filepath"]
        else:
            raise TaskFactoryError("FileSensor requires 'filepath' parameter")
        
        if "conn_id" in task_config:
            args["fs_conn_id"] = task_config["conn_id"]
        
        return args
    
    def _build_http_sensor_args(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build arguments for HttpSensor."""
        args = {}
        
        if "endpoint" in task_config:
            args["endpoint"] = task_config["endpoint"]
        else:
            raise TaskFactoryError("HttpSensor requires 'endpoint' parameter")
        
        if "conn_id" in task_config:
            args["http_conn_id"] = task_config["conn_id"]
        
        return args
    
    def _build_s3_sensor_args(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build arguments for S3KeySensor."""
        args = {}
        
        if "bucket_name" in task_config:
            args["bucket_name"] = task_config["bucket_name"]
        else:
            raise TaskFactoryError("S3KeySensor requires 'bucket_name' parameter")
        
        if "bucket_key" in task_config:
            args["bucket_key"] = task_config["bucket_key"]
        else:
            raise TaskFactoryError("S3KeySensor requires 'bucket_key' parameter")
        
        if "conn_id" in task_config:
            args["aws_conn_id"] = task_config["conn_id"]
        
        return args
    
    def _build_sql_sensor_args(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build arguments for SqlSensor."""
        args = {}
        
        if "sql" in task_config:
            args["sql"] = task_config["sql"]
        else:
            raise TaskFactoryError("SqlSensor requires 'sql' parameter")
        
        if "conn_id" in task_config:
            args["conn_id"] = task_config["conn_id"]
        else:
            raise TaskFactoryError("SqlSensor requires 'conn_id' parameter")
        
        return args
    
    def _build_external_task_sensor_args(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build arguments for ExternalTaskSensor."""
        args = {}
        
        if "external_dag_id" in task_config:
            args["external_dag_id"] = task_config["external_dag_id"]
        else:
            raise TaskFactoryError("ExternalTaskSensor requires 'external_dag_id' parameter")
        
        if "external_task_id" in task_config:
            args["external_task_id"] = task_config["external_task_id"]
        
        return args
    
    def _resolve_callable(self, callable_name: str) -> Any:
        """Resolve a callable from its string name.
        
        Args:
            callable_name: Name of the callable to resolve.
            
        Returns:
            The resolved callable.
            
        Raises:
            TaskFactoryError: If callable cannot be resolved.
        """
        # This is a simplified implementation
        # In practice, you'd want more sophisticated module/function resolution
        try:
            if "." in callable_name:
                module_name, func_name = callable_name.rsplit(".", 1)
                import importlib
                module = importlib.import_module(module_name)
                return getattr(module, func_name)
            else:
                # Assume it's in the current namespace or builtins
                return eval(callable_name)
        except Exception as e:
            raise TaskFactoryError(
                f"Failed to resolve callable '{callable_name}'",
                str(e)
            )
    
    def get_supported_operators(self) -> List[str]:
        """Get list of supported operator types.
        
        Returns:
            List of supported operator type names.
        """
        return self.supported_operators.copy()