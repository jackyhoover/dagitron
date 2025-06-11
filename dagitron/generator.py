"""DAG generator for converting YAML specifications to Airflow DAGs."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

# Import Airflow components with graceful fallback
try:
    from airflow import DAG
    from airflow.models import BaseOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    # Mock classes for when Airflow is not available
    class DAG:
        pass
    class BaseOperator:
        pass
    AIRFLOW_AVAILABLE = False
from .parser import YamlDagParser
from .factory import TaskFactory
from .dependencies import DependencyResolver
from .exceptions import DagGenerationError


class DagGenerator:
    """Generates Airflow DAGs from YAML specifications."""
    
    def __init__(
        self, 
        parser: Optional[YamlDagParser] = None,
        factory: Optional[TaskFactory] = None,
        resolver: Optional[DependencyResolver] = None
    ) -> None:
        """Initialize the DAG generator.
        
        Args:
            parser: YAML parser instance. If None, creates a default one.
            factory: Task factory instance. If None, creates a default one.
            resolver: Dependency resolver instance. If None, creates a default one.
        """
        self.parser = parser if parser is not None else YamlDagParser()
        self.factory = factory if factory is not None else TaskFactory()
        self.resolver = resolver if resolver is not None else DependencyResolver()
    
    def generate_dag_from_file(self, yaml_file: str) -> DAG:
        """Generate an Airflow DAG from a YAML file.
        
        Args:
            yaml_file: Path to the YAML specification file.
            
        Returns:
            Generated Airflow DAG instance.
            
        Raises:
            DagGenerationError: If DAG generation fails.
        """
        try:
            dag_spec = self.parser.parse_file(yaml_file)
            return self.generate_dag_from_spec(dag_spec)
        except Exception as e:
            raise DagGenerationError(f"Failed to generate DAG from file '{yaml_file}'", str(e))
    
    def generate_dag_from_string(self, yaml_content: str) -> DAG:
        """Generate an Airflow DAG from YAML content string.
        
        Args:
            yaml_content: YAML specification as string.
            
        Returns:
            Generated Airflow DAG instance.
            
        Raises:
            DagGenerationError: If DAG generation fails.
        """
        try:
            dag_spec = self.parser.parse_string(yaml_content)
            return self.generate_dag_from_spec(dag_spec)
        except Exception as e:
            raise DagGenerationError("Failed to generate DAG from YAML content", str(e))
    
    def generate_dag_from_spec(self, dag_spec: Dict[str, Any]) -> DAG:
        """Generate an Airflow DAG from parsed specification.
        
        Args:
            dag_spec: Parsed DAG specification dictionary.
            
        Returns:
            Generated Airflow DAG instance.
            
        Raises:
            DagGenerationError: If DAG generation fails.
        """
        if not AIRFLOW_AVAILABLE:
            raise DagGenerationError(
                "Airflow is not installed. Please install apache-airflow to generate DAGs.",
                "Run: pip install apache-airflow>=2.0.0"
            )
        
        try:
            # Extract DAG and tasks configuration
            dag_config = self.parser.get_dag_config(dag_spec)
            tasks_config = self.parser.get_tasks_config(dag_spec)
            
            # Create the DAG
            dag = self._create_dag(dag_config)
            
            # Create tasks
            tasks = self._create_tasks(tasks_config, dag)
            
            # Set up dependencies
            self._setup_dependencies(tasks_config, tasks)
            
            return dag
        
        except Exception as e:
            raise DagGenerationError("Failed to generate DAG from specification", str(e))
    
    def _create_dag(self, dag_config: Dict[str, Any]) -> DAG:
        """Create an Airflow DAG from configuration.
        
        Args:
            dag_config: DAG configuration dictionary.
            
        Returns:
            Airflow DAG instance.
        """
        # Required parameters
        dag_id = dag_config["dag_id"]
        
        # Optional parameters with defaults
        description = dag_config.get("description", f"DAG generated from YAML: {dag_id}")
        schedule_interval = dag_config.get("schedule_interval", None)
        start_date = self._parse_date(dag_config.get("start_date", "2024-01-01"))
        end_date = dag_config.get("end_date")
        if end_date:
            end_date = self._parse_date(end_date)
        
        catchup = dag_config.get("catchup", False)
        max_active_runs = dag_config.get("max_active_runs", 1)
        max_active_tasks = dag_config.get("max_active_tasks", 16)
        tags = dag_config.get("tags", [])
        
        # Build default_args
        default_args = self._build_default_args(dag_config.get("default_args", {}))
        
        return DAG(
            dag_id=dag_id,
            description=description,
            schedule_interval=schedule_interval,
            start_date=start_date,
            end_date=end_date,
            catchup=catchup,
            max_active_runs=max_active_runs,
            max_active_tasks=max_active_tasks,
            default_args=default_args,
            tags=tags,
        )
    
    def _build_default_args(self, default_args_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build default_args dictionary for DAG.
        
        Args:
            default_args_config: Default args configuration.
            
        Returns:
            Default args dictionary.
        """
        default_args = {}
        
        if "owner" in default_args_config:
            default_args["owner"] = default_args_config["owner"]
        else:
            default_args["owner"] = "dagitron"
        
        if "depends_on_past" in default_args_config:
            default_args["depends_on_past"] = default_args_config["depends_on_past"]
        
        if "start_date" in default_args_config:
            default_args["start_date"] = self._parse_date(default_args_config["start_date"])
        
        if "email_on_failure" in default_args_config:
            default_args["email_on_failure"] = default_args_config["email_on_failure"]
        
        if "email_on_retry" in default_args_config:
            default_args["email_on_retry"] = default_args_config["email_on_retry"]
        
        if "email" in default_args_config:
            email = default_args_config["email"]
            if isinstance(email, str):
                default_args["email"] = [email]
            else:
                default_args["email"] = email
        
        if "retries" in default_args_config:
            default_args["retries"] = default_args_config["retries"]
        
        if "retry_delay_minutes" in default_args_config:
            default_args["retry_delay"] = timedelta(
                minutes=default_args_config["retry_delay_minutes"]
            )
        
        return default_args
    
    def _create_tasks(self, tasks_config: List[Dict[str, Any]], dag: DAG) -> Dict[str, BaseOperator]:
        """Create tasks from configuration.
        
        Args:
            tasks_config: List of task configurations.
            dag: DAG instance to attach tasks to.
            
        Returns:
            Dictionary mapping task names to operator instances.
        """
        tasks = {}
        
        for task_config in tasks_config:
            task_name = task_config["name"]
            operator = self.factory.create_task(task_config, dag)
            tasks[task_name] = operator
        
        return tasks
    
    def _setup_dependencies(
        self, 
        tasks_config: List[Dict[str, Any]], 
        tasks: Dict[str, BaseOperator]
    ) -> None:
        """Set up task dependencies.
        
        Args:
            tasks_config: List of task configurations.
            tasks: Dictionary of created task operators.
        """
        # Resolve dependencies
        dependencies = self.resolver.resolve_dependencies(tasks_config)
        
        # Set up Airflow task dependencies
        for task_name, task_deps in dependencies.items():
            if task_deps:
                task = tasks[task_name]
                upstream_tasks = [tasks[dep] for dep in task_deps]
                task.set_upstream(upstream_tasks)
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string to datetime object.
        
        Args:
            date_str: Date string in YYYY-MM-DD format.
            
        Returns:
            Datetime object.
            
        Raises:
            DagGenerationError: If date parsing fails.
        """
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError as e:
            raise DagGenerationError(f"Invalid date format '{date_str}'. Expected YYYY-MM-DD", str(e))
    
    def validate_dag_spec(self, dag_spec: Dict[str, Any]) -> bool:
        """Validate a DAG specification without generating the DAG.
        
        Args:
            dag_spec: DAG specification dictionary.
            
        Returns:
            True if valid, False otherwise.
        """
        try:
            # Parse and validate the specification
            dag_config = self.parser.get_dag_config(dag_spec)
            tasks_config = self.parser.get_tasks_config(dag_spec)
            
            # Validate dependencies
            self.resolver.resolve_dependencies(tasks_config)
            
            return True
        except Exception:
            return False
    
    def get_dag_summary(self, dag_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Get a summary of the DAG specification.
        
        Args:
            dag_spec: DAG specification dictionary.
            
        Returns:
            Summary dictionary with DAG information.
        """
        dag_config = self.parser.get_dag_config(dag_spec)
        tasks_config = self.parser.get_tasks_config(dag_spec)
        
        # Get dependency information
        dependencies = self.resolver.resolve_dependencies(tasks_config)
        levels = self.resolver.get_task_levels(dependencies)
        parallel_groups = self.resolver.get_parallel_groups(dependencies)
        
        return {
            "dag_id": dag_config["dag_id"],
            "description": dag_config.get("description", ""),
            "schedule_interval": dag_config.get("schedule_interval"),
            "start_date": dag_config.get("start_date"),
            "task_count": len(tasks_config),
            "operators": list(set(task["operator"] for task in tasks_config)),
            "max_depth": max(levels.values()) if levels else 0,
            "parallel_groups": len(parallel_groups),
            "has_dependencies": any(deps for deps in dependencies.values()),
        }